import argparse
import concurrent.futures
import hashlib
import json
import re
import signal
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from requests import HTTPError

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None


BASE_URL = "https://shuiyuan.sjtu.edu.cn"
CATEGORY_JSON_URL = f"{BASE_URL}/c/leisure-entertainment/food/64.json"
COOKIE_PATH = Path("cookies.txt")
DATA_DIR = Path("food_review_data")
THREADS_DIR = DATA_DIR / "threads"
EXTRACTIONS_DIR = DATA_DIR / "extractions"
DEBUG_DIR = DATA_DIR / "debug"
REPORTS_DIR = DATA_DIR / "reports"
MERCHANT_REPORTS_DIR = REPORTS_DIR / "merchants"
INDEX_PATH = DATA_DIR / "topic_index.json"
MERCHANT_BOOK_PATH = DATA_DIR / "merchant_book.json"

THREAD_SCHEMA_VERSION = "thread-v1"
EXTRACTION_VERSION = "extract-v3"

DEFAULT_LIMIT = 50
DEFAULT_LLM_ENDPOINT = "http://localhost:8088/api/v1/chat/completions"
DEFAULT_LLM_MODEL = "stepfun/step-3.5-flash:free"
DEFAULT_TIMEOUT = 120
MAX_CHARS_PER_CHUNK = 18000
REQUEST_SLEEP_SECONDS = 0.3
LLM_RETRY_COUNT = 6
LLM_RETRY_BASE_SECONDS = 2.0
LLM_REQUEST_SLEEP_SECONDS = 1.0
DEFAULT_WORKERS = 2
NORMALIZATION_LLM_LIMIT = 8

LOW_INFO_PATTERNS = {
    "dd",
    "ddd",
    "d",
    "蹲",
    "蹲蹲",
    "mark",
    "m",
    "cy",
    "插眼",
    "顶",
    "顶顶",
    "up",
}

GENERIC_MERCHANT_REFERENCES = {
    "这家店",
    "这家馆子",
    "这家馆儿",
    "这家苍蝇馆儿",
    "这家苍蝇馆",
    "这家小店",
    "这家",
    "这店",
    "店家",
    "他家",
    "它家",
    "这家饭店",
    "这家餐厅",
    "这家食堂",
    "这家窗口",
    "这家铺子",
}


INTERRUPTED = False


def handle_interrupt(signum, frame) -> None:
    global INTERRUPTED
    if INTERRUPTED:
        raise KeyboardInterrupt
    INTERRUPTED = True
    print("\nInterrupt received. Stopping after current in-flight work. Press Ctrl+C again to force exit.")


def check_interrupted() -> None:
    if INTERRUPTED:
        raise KeyboardInterrupt


def interruptible_sleep(seconds: float) -> None:
    remaining = max(0.0, seconds)
    step = 0.2
    while remaining > 0:
        check_interrupted()
        chunk = min(step, remaining)
        time.sleep(chunk)
        remaining -= chunk


def read_cookie(path: Path = COOKIE_PATH) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Cookie file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def build_session(cookie: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Cookie": cookie,
        }
    )
    return session


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    THREADS_DIR.mkdir(exist_ok=True)
    EXTRACTIONS_DIR.mkdir(exist_ok=True)
    DEBUG_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)
    MERCHANT_REPORTS_DIR.mkdir(exist_ok=True)


def get_json(session: requests.Session, url: str) -> dict[str, Any]:
    check_interrupted()
    response = session.get(url, timeout=30)
    if response.status_code == 403:
        raise RuntimeError(
            f"Request rejected for {url}. Your Shuiyuan cookie may be expired or missing required permissions."
        )
    response.raise_for_status()
    content_type = (response.headers.get("Content-Type") or "").lower()
    text = response.text.lstrip()

    if "application/json" in content_type:
        return response.json()

    if text.startswith("{") or text.startswith("["):
        return response.json()

    preview = text[:300].replace("\n", " ")
    raise RuntimeError(
        "Expected JSON response but got something else.\n"
        f"url: {url}\n"
        f"status: {response.status_code}\n"
        f"content-type: {response.headers.get('Content-Type')}\n"
        f"final-url: {response.url}\n"
        f"body-preview: {preview}"
    )


def normalize_category_json_url(url: str) -> str:
    absolute = urljoin(BASE_URL, url)
    parts = urlsplit(absolute)
    path = parts.path
    if not path.endswith(".json"):
        path = f"{path}.json"
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def fetch_latest_topics(session: requests.Session, limit: int) -> list[dict[str, Any]]:
    topics: list[dict[str, Any]] = []
    next_url = normalize_category_json_url(CATEGORY_JSON_URL)

    while next_url and len(topics) < limit:
        check_interrupted()
        data = get_json(session, next_url)
        topic_list = data.get("topic_list", {})
        topics.extend(topic_list.get("topics", []))

        more_topics_url = topic_list.get("more_topics_url")
        next_url = normalize_category_json_url(more_topics_url) if more_topics_url else None
        time.sleep(REQUEST_SLEEP_SECONDS)

    return topics[:limit]


def topic_signature(topic: dict[str, Any]) -> str:
    identity = {
        "id": topic.get("id"),
        "title": topic.get("title"),
        "posts_count": topic.get("posts_count"),
        "last_posted_at": topic.get("last_posted_at"),
    }
    payload = json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def build_topic_url(topic: dict[str, Any]) -> str:
    slug = topic.get("slug") or "topic"
    return f"{BASE_URL}/t/{slug}/{topic['id']}"


def fetch_topic_detail(session: requests.Session, topic: dict[str, Any]) -> dict[str, Any]:
    slug = topic.get("slug") or "topic"
    candidates = [
        f"{BASE_URL}/t/{slug}/{topic['id']}.json?include_raw=1",
        f"{BASE_URL}/t/{topic['id']}.json?include_raw=1",
    ]

    last_error: Exception | None = None
    for url in candidates:
        try:
            return get_json(session, url)
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Failed to fetch topic detail for {topic['id']}: {last_error}")


def html_to_text(html: str) -> str:
    if not html:
        return ""
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text("\n")
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text


def clean_markdown_text(text: str) -> str:
    text = text.replace("\r\n", "\n")
    text = re.sub(r"!\[.*?]\(.*?\)", "[IMAGE]", text)
    text = re.sub(r"\[(.*?)\|attachment]\(.*?\)", r"[ATTACHMENT: \1]", text)
    text = re.sub(r"\[(.*?)\|audio]\(.*?\)", r"[AUDIO: \1]", text)
    text = re.sub(r"\[(.*?)\|video]\(.*?\)", r"[VIDEO: \1]", text)
    text = re.sub(r"https?://\S+", "[LINK]", text)
    text = re.sub(r"^>.*$", "[QUOTE]", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_low_information(text: str) -> bool:
    compact = re.sub(r"\s+", "", text).lower()
    if not compact:
        return True
    if compact in LOW_INFO_PATTERNS:
        return True
    if len(compact) < 5:
        return True
    if all(ch in "[]()!?.,~+-=_" for ch in compact):
        return True
    return False


def normalize_post(post: dict[str, Any]) -> dict[str, Any]:
    original_text = post.get("raw") or html_to_text(post.get("cooked", ""))
    cleaned_text = clean_markdown_text(original_text)
    return {
        "post_id": post.get("id"),
        "post_number": post.get("post_number"),
        "author": post.get("username") or post.get("name") or "unknown",
        "created_at": post.get("created_at"),
        "updated_at": post.get("updated_at"),
        "reply_to_post_number": post.get("reply_to_post_number"),
        "original_text": original_text.strip(),
        "cleaned_text": cleaned_text,
        "is_low_information": is_low_information(cleaned_text),
    }


def clean_candidate_name(name: str) -> str:
    value = name.strip(" \t\n\r-:：!！?？,，。[]()（）【】\"'“”")
    value = re.sub(r"\s+", "", value)
    return value


def extract_title_candidates(title: str) -> list[str]:
    raw_candidates: list[str] = []

    for part in re.findall(r"[（(]([^()（）]{1,30})[）)]", title):
        raw_candidates.append(part)

    title_wo_prefix = re.sub(r"^(安利|推荐|避雷|踩雷|求助|测评|repo|REPO|食评|探店)[!！:：\s]*", "", title)
    title_wo_suffix = re.sub(r"(不要去|别去|推荐|避雷|测评|repo|REPO).*$", "", title_wo_prefix)
    split_parts = re.split(r"[【】\[\]<>《》/|·]", title_wo_suffix)
    raw_candidates.extend(split_parts)

    phrase_matches = re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+(?:店|馆|饭店|餐厅|猪脚饭|汤面|麻辣烫|烧烤|火锅|面馆|食堂|窗口)", title)
    raw_candidates.extend(phrase_matches)

    candidates: list[str] = []
    seen: set[str] = set()
    for item in raw_candidates:
        candidate = clean_candidate_name(item)
        if not candidate:
            continue
        if len(candidate) < 2:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)

    candidates.sort(key=len, reverse=True)
    return candidates


def is_generic_merchant_reference(name: str | None) -> bool:
    if not name:
        return True
    cleaned = clean_candidate_name(name)
    if not cleaned:
        return True
    if cleaned in GENERIC_MERCHANT_REFERENCES:
        return True
    if re.fullmatch(r"(这|那|他|她|它)(家|店|馆|馆子|餐厅|饭店|窗口)", cleaned):
        return True
    return False


def rule_normalize_name(
    opinion: dict[str, Any],
    title_candidates: list[str],
    known_names: list[str],
) -> tuple[str | None, float, str]:
    raw_name = clean_candidate_name(opinion.get("raw_name") or "")
    normalized_name = clean_candidate_name(opinion.get("normalized_name") or "")

    if normalized_name:
        return normalized_name, 0.95, "model_provided"

    if raw_name and not is_generic_merchant_reference(raw_name):
        for candidate in title_candidates:
            if raw_name == candidate or raw_name in candidate or candidate in raw_name:
                return candidate, 0.88, "title_match"
        return raw_name, 0.7, "raw_name_fallback"

    if title_candidates:
        if len(title_candidates) == 1:
            return title_candidates[0], 0.82, "single_title_candidate"
        if known_names:
            return known_names[0], 0.76, "single_known_name"
        return title_candidates[0], 0.68, "title_candidate_fallback"

    if known_names:
        return known_names[0], 0.62, "known_name_fallback"

    return None, 0.0, "unresolved"


def build_thread_document(topic: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    tags = detail.get("tags", topic.get("tags", []))
    posts = detail.get("post_stream", {}).get("posts", [])
    normalized_posts = [normalize_post(post) for post in posts]

    return {
        "thread_schema_version": THREAD_SCHEMA_VERSION,
        "topic_signature": topic_signature(topic),
        "topic_id": topic.get("id"),
        "title": detail.get("title") or topic.get("title"),
        "slug": detail.get("slug") or topic.get("slug"),
        "category_id": detail.get("category_id") or topic.get("category_id"),
        "topic_url": build_topic_url(topic),
        "created_at": detail.get("created_at") or topic.get("created_at"),
        "last_posted_at": detail.get("last_posted_at") or topic.get("last_posted_at"),
        "posts_count": detail.get("posts_count") or topic.get("posts_count"),
        "reply_count": detail.get("reply_count") or topic.get("reply_count"),
        "tags": tags,
        "posts": normalized_posts,
    }


def save_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sanitize_filename(name: str) -> str:
    sanitized = re.sub(r'[\\/*?:"<>|]', "_", name).strip()
    sanitized = re.sub(r"\s+", "_", sanitized)
    return sanitized[:120] or "merchant"


def markdown_escape(text: str | None) -> str:
    if not text:
        return ""
    return text.replace("\r\n", "\n").strip()


def render_merchant_report(merchant: dict[str, Any]) -> tuple[str, str]:
    merchant_name = merchant["merchant_name"]
    filename = f"{sanitize_filename(merchant_name)}.md"
    lines = [
        f"# {merchant_name}",
        "",
        f"- 规范名: {merchant.get('normalized_name') or merchant_name}",
        f"- 观点数: {merchant.get('opinion_count', len(merchant.get('opinions', [])))}",
        "",
        "## 观点列表",
        "",
    ]

    for opinion in merchant.get("opinions", []):
        lines.extend(
            [
                f"### {opinion.get('topic_title', 'Untitled')} / #{opinion.get('post_number')}",
                "",
                "摘要：",
                "",
                markdown_escape(opinion.get("summary")) or "(无)",
                "",
                "原文摘录：",
                "",
                "> " + "\n> ".join(markdown_escape(opinion.get("original_excerpt")).splitlines()) if opinion.get("original_excerpt") else "> (无)",
                "",
                "<details>",
                "<summary>展开查看元信息</summary>",
                "",
                f"- 链接: {opinion.get('topic_url', '')}",
                f"- 作者: {opinion.get('author', 'unknown')}",
                f"- 时间: {opinion.get('created_at', '')}",
                f"- 角色: {opinion.get('speaker_role', '')}",
                f"- 情感: {opinion.get('sentiment', '')}",
                f"- 原称呼: {opinion.get('raw_name', '')}",
                f"- 归一化置信度: {opinion.get('normalization_confidence', '')}",
                f"- 归一化原因: {opinion.get('normalization_reason', '')}",
                f"- 标签: {', '.join(opinion.get('reason_tags', []))}",
                "",
                "</details>",
                "",
            ]
        )

    return filename, "\n".join(lines).strip() + "\n"


def render_reports(merchant_book: dict[str, Any]) -> None:
    report_links: list[tuple[str, str, int]] = []
    for merchant in merchant_book.get("merchants", []):
        filename, content = render_merchant_report(merchant)
        (MERCHANT_REPORTS_DIR / filename).write_text(content, encoding="utf-8")
        report_links.append((merchant["merchant_name"], filename, merchant.get("opinion_count", 0)))

    index_lines = [
        "# Merchant Book",
        "",
        f"- 生成时间: {merchant_book.get('generated_at', '')}",
        f"- 店家数: {merchant_book.get('merchant_count', 0)}",
        "",
        "## 目录",
        "",
    ]

    for merchant_name, filename, opinion_count in sorted(report_links, key=lambda item: item[0].lower()):
        index_lines.append(f"- [{merchant_name}](./merchants/{filename}) ({opinion_count})")

    (REPORTS_DIR / "index.md").write_text("\n".join(index_lines).strip() + "\n", encoding="utf-8")


def should_skip_thread(topic: dict[str, Any], thread_path: Path, force: bool) -> bool:
    if force or not thread_path.exists():
        return False
    existing = load_json(thread_path)
    return (
        existing.get("thread_schema_version") == THREAD_SCHEMA_VERSION
        and existing.get("topic_signature") == topic_signature(topic)
    )


def should_skip_extraction(thread_doc: dict[str, Any], extraction_path: Path, force: bool) -> bool:
    if force or not extraction_path.exists():
        return False
    existing = load_json(extraction_path)
    return (
        existing.get("extraction_version") == EXTRACTION_VERSION
        and existing.get("topic_signature") == thread_doc.get("topic_signature")
    )


def build_llm_chunks(thread_doc: dict[str, Any], max_chars: int = MAX_CHARS_PER_CHUNK) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_len = 0

    for post in thread_doc["posts"]:
        if not post["cleaned_text"]:
            continue

        payload_post = {
            "post_number": post["post_number"],
            "author": post["author"],
            "created_at": post["created_at"],
            "reply_to_post_number": post["reply_to_post_number"],
            "text": post["cleaned_text"],
            "is_low_information": post["is_low_information"],
        }
        serialized = json.dumps(payload_post, ensure_ascii=False)

        if current_chunk and current_len + len(serialized) > max_chars:
            chunks.append(current_chunk)
            current_chunk = []
            current_len = 0

        current_chunk.append(payload_post)
        current_len += len(serialized)

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def build_prompt(thread_doc: dict[str, Any], posts_chunk: list[dict[str, Any]], chunk_index: int, chunk_count: int) -> list[dict[str, str]]:
    system_prompt = """
你是一个中文论坛美食评论信息抽取器。你的任务是从帖子主楼和回复中抽取“对店家/餐厅/窗口/外卖商家”的具体观点。

规则：
1. 只依据输入文本，不要猜测，不要补充未出现的信息。
2. 保留不同人的不同意见，不要把正反观点压缩成一句笼统总结。
3. 只输出 JSON，不要输出解释、Markdown 或代码块。
4. 忽略闲聊、纯表情、纯引用、纯顶帖、无实际评价的信息。
5. opinion 必须一条对应一个来源 post_number。
6. original_excerpt 必须直接摘录自输入文本，不能改写。
7. 若无法确定标准店名，normalized_name 设为 null，但 raw_name 仍要填写。
8. speaker_role 只能是 "op" 或 "reply"。
9. summary 是对单条观点的简短整理，不要夸张，不要引入原文没有的判断。

输出 JSON 结构：
{
  "merchant_opinions": [
    {
      "raw_name": "文本中出现的店家名或称呼",
      "normalized_name": "统一后的店家名，无法确定则为 null",
      "post_number": 1,
      "author": "用户名",
      "created_at": "时间字符串",
      "speaker_role": "op|reply",
      "reply_to_post_number": null,
      "sentiment": "positive|negative|mixed|neutral",
      "summary": "单条观点的一两句话整理",
      "original_excerpt": "直接摘录的短原文",
      "reason_tags": ["味道", "价格", "卫生", "分量", "服务", "环境", "排队", "位置", "其他"]
    }
  ],
  "ignored_post_numbers": [2, 5]
}
""".strip()

    user_payload = {
        "topic_id": thread_doc["topic_id"],
        "title": thread_doc["title"],
        "topic_url": thread_doc["topic_url"],
        "chunk_index": chunk_index,
        "chunk_count": chunk_count,
        "posts": posts_chunk,
    }

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def extract_json_from_text(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)

    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", stripped)
    if not match:
        raise ValueError("No JSON object found in model response.")
    candidate = match.group(0)

    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        sanitized = sanitize_json_candidate(candidate)
        return json.loads(sanitized)


def sanitize_json_candidate(candidate: str) -> str:
    result: list[str] = []
    in_string = False
    escaped = False

    for ch in candidate:
        if in_string:
            if escaped:
                result.append(ch)
                escaped = False
                continue

            if ch == "\\":
                result.append(ch)
                escaped = True
                continue

            if ch == '"':
                result.append(ch)
                in_string = False
                continue

            if ch == "\n":
                result.append("\\n")
                continue

            if ch == "\r":
                result.append("\\r")
                continue

            if ch == "\t":
                result.append("\\t")
                continue

            if ord(ch) < 32:
                result.append(" ")
                continue

            result.append(ch)
            continue

        result.append(ch)
        if ch == '"':
            in_string = True

    return "".join(result)


def call_llm(
    endpoint: str,
    model: str,
    messages: list[dict[str, str]],
    timeout: int,
    debug_response_path: Path | None = None,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, LLM_RETRY_COUNT + 1):
        check_interrupted()
        try:
            response = requests.post(
                endpoint,
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": 0.1,
                },
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            if debug_response_path is not None:
                debug_response_path.write_text(content, encoding="utf-8")
            return extract_json_from_text(content)
        except HTTPError as exc:
            last_error = exc
            response = exc.response
            status_code = response.status_code if response is not None else None
            retryable = status_code == 429 or (status_code is not None and 500 <= status_code < 600)
            if not retryable or attempt == LLM_RETRY_COUNT:
                raise

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_seconds = float(retry_after)
            else:
                sleep_seconds = LLM_RETRY_BASE_SECONDS * (2 ** (attempt - 1))

            print(
                f"  extract: retryable HTTP {status_code}, retry in {sleep_seconds:.1f}s "
                f"(attempt {attempt}/{LLM_RETRY_COUNT})"
            )
            interruptible_sleep(sleep_seconds)
        except Exception as exc:
            last_error = exc
            if attempt == LLM_RETRY_COUNT:
                raise
            sleep_seconds = min(LLM_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), 30.0)
            print(f"  extract: transient error, retry in {sleep_seconds:.1f}s (attempt {attempt}/{LLM_RETRY_COUNT})")
            interruptible_sleep(sleep_seconds)

    raise RuntimeError(f"LLM request failed after retries: {last_error}")


def build_normalization_prompt(
    thread_doc: dict[str, Any],
    unresolved_opinions: list[dict[str, Any]],
    title_candidates: list[str],
    known_names: list[str],
) -> list[dict[str, str]]:
    system_prompt = """
你是一个中文论坛店名归一化助手。任务是根据帖子标题、主楼和评论片段，把泛指或别名映射成尽可能准确的店家标准名。

规则：
1. 只依据输入文本，不要猜测。
2. 如果原文是“这家店/这家馆子/他家”等泛指，应优先结合标题和主楼上下文判断具体店名。
3. normalized_name 应尽量使用帖子标题中出现的正式称呼；无法确定则返回 null。
4. 输出必须是 JSON，不要输出解释或 Markdown。

输出 JSON 结构：
{
  "results": [
    {
      "index": 0,
      "normalized_name": "标准店名或 null",
      "confidence": 0.0,
      "reason": "一句话说明依据"
    }
  ]
}
""".strip()

    opener_posts = [
        {
            "post_number": post["post_number"],
            "text": post["cleaned_text"][:1200],
        }
        for post in thread_doc["posts"]
        if post["post_number"] == 1
    ]

    payload = {
        "topic_title": thread_doc["title"],
        "title_candidates": title_candidates,
        "known_names": known_names,
        "op_post": opener_posts[0] if opener_posts else None,
        "opinions": [
            {
                "index": idx,
                "raw_name": opinion.get("raw_name"),
                "summary": opinion.get("summary"),
                "original_excerpt": opinion.get("original_excerpt"),
                "post_number": opinion.get("post_number"),
            }
            for idx, opinion in enumerate(unresolved_opinions)
        ],
    }

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]


def normalize_opinions_with_llm(
    thread_doc: dict[str, Any],
    opinions: list[dict[str, Any]],
    endpoint: str,
    model: str,
    timeout: int,
    title_candidates: list[str],
    known_names: list[str],
) -> None:
    unresolved = [
        opinion
        for opinion in opinions
        if not opinion.get("normalized_name")
        or opinion.get("normalization_confidence", 0.0) < 0.75
    ]
    if not unresolved:
        return

    unresolved = unresolved[:NORMALIZATION_LLM_LIMIT]
    messages = build_normalization_prompt(thread_doc, unresolved, title_candidates, known_names)
    response = call_llm(
        endpoint,
        model,
        messages,
        timeout,
        DEBUG_DIR / f"normalization_response_{thread_doc['topic_id']}.txt",
    )

    for item in response.get("results", []):
        index = item.get("index")
        if not isinstance(index, int):
            continue
        if index < 0 or index >= len(unresolved):
            continue

        opinion = unresolved[index]
        normalized_name = item.get("normalized_name")
        confidence = item.get("confidence", 0.0)
        reason = item.get("reason", "llm_normalized")

        if normalized_name:
            opinion["normalized_name"] = clean_candidate_name(str(normalized_name))
            opinion["normalization_confidence"] = float(confidence or 0.0)
            opinion["normalization_reason"] = str(reason)


def normalize_merchant_names(
    thread_doc: dict[str, Any],
    opinions: list[dict[str, Any]],
    endpoint: str,
    model: str,
    timeout: int,
) -> list[dict[str, Any]]:
    title_candidates = extract_title_candidates(thread_doc["title"])
    known_names = [
        clean_candidate_name(opinion.get("normalized_name") or opinion.get("raw_name") or "")
        for opinion in opinions
        if opinion.get("normalized_name") or (
            opinion.get("raw_name") and not is_generic_merchant_reference(opinion.get("raw_name"))
        )
    ]
    known_names = [name for name in known_names if name]
    known_names = list(dict.fromkeys(known_names))

    for opinion in opinions:
        normalized_name, confidence, reason = rule_normalize_name(opinion, title_candidates, known_names)
        opinion["normalized_name"] = normalized_name
        opinion["normalization_confidence"] = confidence
        opinion["normalization_reason"] = reason

    normalize_opinions_with_llm(thread_doc, opinions, endpoint, model, timeout, title_candidates, known_names)

    for opinion in opinions:
        if opinion.get("normalized_name"):
            opinion["normalized_name"] = clean_candidate_name(opinion["normalized_name"])

    return opinions


def merge_reviews(chunk_outputs: list[dict[str, Any]]) -> dict[str, Any]:
    opinions: list[dict[str, Any]] = []
    ignored_posts: set[int] = set()

    for chunk in chunk_outputs:
        for post_number in chunk.get("ignored_post_numbers", []):
            ignored_posts.add(post_number)

        for opinion in chunk.get("merchant_opinions", []):
            if not (opinion.get("normalized_name") or opinion.get("raw_name")):
                continue
            opinions.append(opinion)

    deduped_opinions = dedupe_opinions(opinions)
    deduped_opinions.sort(
        key=lambda item: (
            (item.get("normalized_name") or item.get("raw_name") or ""),
            item.get("post_number") or 0,
        )
    )
    return {
        "merchant_opinions": deduped_opinions,
        "ignored_post_numbers": sorted(ignored_posts),
    }


def dedupe_opinions(opinions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped = []
    for item in opinions:
        key = json.dumps(
            {
                "merchant": item.get("normalized_name") or item.get("raw_name"),
                "post_number": item.get("post_number"),
                "original_excerpt": item.get("original_excerpt"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def extract_thread_reviews(
    thread_doc: dict[str, Any],
    endpoint: str,
    model: str,
    timeout: int,
) -> dict[str, Any]:
    chunks = build_llm_chunks(thread_doc)
    chunk_outputs = []

    for index, chunk in enumerate(chunks, start=1):
        check_interrupted()
        messages = build_prompt(thread_doc, chunk, index, len(chunks))
        try:
            chunk_output = call_llm(
                endpoint,
                model,
                messages,
                timeout,
                DEBUG_DIR / f"llm_response_{thread_doc['topic_id']}_{index}.txt",
            )
        except Exception:
            debug_payload = {
                "topic_id": thread_doc["topic_id"],
                "title": thread_doc["title"],
                "chunk_index": index,
                "chunk_count": len(chunks),
                "messages": messages,
            }
            save_json(DEBUG_DIR / f"llm_request_{thread_doc['topic_id']}_{index}.json", debug_payload)
            raise
        chunk_outputs.append(chunk_output)
        interruptible_sleep(LLM_REQUEST_SLEEP_SECONDS)

    merged = merge_reviews(chunk_outputs)
    normalized_opinions = normalize_merchant_names(thread_doc, merged["merchant_opinions"], endpoint, model, timeout)
    return {
        "extraction_version": EXTRACTION_VERSION,
        "topic_signature": thread_doc["topic_signature"],
        "topic_id": thread_doc["topic_id"],
        "title": thread_doc["title"],
        "topic_url": thread_doc["topic_url"],
        "last_posted_at": thread_doc["last_posted_at"],
        "merchant_opinions": normalized_opinions,
        "ignored_post_numbers": merged["ignored_post_numbers"],
    }


def build_merchant_book(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    merchants: dict[str, dict[str, Any]] = {}

    for extraction in extractions:
        for opinion in extraction.get("merchant_opinions", []):
            key = (opinion.get("normalized_name") or opinion.get("raw_name") or "").strip()
            if not key:
                continue

            merchant = merchants.setdefault(
                key,
                {
                    "merchant_name": key,
                    "normalized_name": opinion.get("normalized_name"),
                    "opinions": [],
                },
            )

            merchant["opinions"].append(
                {
                    "topic_id": extraction["topic_id"],
                    "topic_title": extraction["title"],
                    "topic_url": extraction["topic_url"],
                    "post_number": opinion.get("post_number"),
                    "author": opinion.get("author"),
                    "created_at": opinion.get("created_at"),
                    "speaker_role": opinion.get("speaker_role"),
                    "reply_to_post_number": opinion.get("reply_to_post_number"),
                    "sentiment": opinion.get("sentiment"),
                    "summary": opinion.get("summary"),
                    "original_excerpt": opinion.get("original_excerpt"),
                    "reason_tags": opinion.get("reason_tags", []),
                    "raw_name": opinion.get("raw_name"),
                    "normalization_confidence": opinion.get("normalization_confidence"),
                    "normalization_reason": opinion.get("normalization_reason"),
                }
            )

    for merchant in merchants.values():
        merchant["opinions"].sort(key=lambda item: (item.get("topic_id") or 0, item.get("post_number") or 0))
        merchant["opinion_count"] = len(merchant["opinions"])

    merchant_book = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "merchant_count": len(merchants),
        "merchants": sorted(merchants.values(), key=lambda item: item["merchant_name"]),
    }
    return merchant_book


def process_topics(
    session: requests.Session | None,
    topics: list[dict[str, Any]],
    endpoint: str,
    model: str,
    timeout: int,
    force_fetch: bool,
    force_extract: bool,
    workers: int,
    extract_only: bool,
) -> list[dict[str, Any]]:
    extractions: list[dict[str, Any]] = []
    pending_extractions: list[tuple[int, dict[str, Any], Path]] = []

    for index, topic in enumerate(topics, start=1):
        check_interrupted()
        topic_id = topic["id"]
        thread_path = THREADS_DIR / f"{topic_id}.json"
        extraction_path = EXTRACTIONS_DIR / f"{topic_id}.json"
        print(f"[{index}/{len(topics)}] topic {topic_id} {topic.get('title', '')}")

        if extract_only:
            if not thread_path.exists():
                print("  thread: missing")
                continue
            thread_doc = load_json(thread_path)
            print("  thread: local")
        elif should_skip_thread(topic, thread_path, force_fetch):
            thread_doc = load_json(thread_path)
            print("  thread: skip")
        else:
            if session is None:
                raise RuntimeError("Session is required when crawling is enabled.")
            detail = fetch_topic_detail(session, topic)
            thread_doc = build_thread_document(topic, detail)
            save_json(thread_path, thread_doc)
            print("  thread: fetched")
            interruptible_sleep(REQUEST_SLEEP_SECONDS)

        if should_skip_extraction(thread_doc, extraction_path, force_extract):
            extraction = load_json(extraction_path)
            extractions.append(extraction)
            print("  extract: skip")
        else:
            pending_extractions.append((index, thread_doc, extraction_path))
            print("  extract: queued")

    if not pending_extractions:
        return extractions

    max_workers = max(1, workers)
    failures: list[tuple[int, Exception]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(extract_and_save_thread, thread_doc, extraction_path, endpoint, model, timeout): (index, thread_doc)
            for index, thread_doc, extraction_path in pending_extractions
        }

        try:
            for future in concurrent.futures.as_completed(future_map):
                check_interrupted()
                index, thread_doc = future_map[future]
                topic_id = thread_doc["topic_id"]
                try:
                    extraction = future.result()
                    extractions.append(extraction)
                    print(f"[{index}/{len(topics)}] topic {topic_id} extract: done")
                except Exception as exc:
                    failures.append((topic_id, exc))
                    print(f"[{index}/{len(topics)}] topic {topic_id} extract: failed: {exc}")
        except KeyboardInterrupt:
            print("Cancelling pending extraction tasks...")
            for future in future_map:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    if failures:
        failure_path = DEBUG_DIR / "extraction_failures.json"
        save_json(
            failure_path,
            [
                {"topic_id": topic_id, "error": str(exc)}
                for topic_id, exc in failures
            ],
        )
        print(f"Saved extraction failure report to {failure_path}")

    extractions.sort(key=lambda item: item["topic_id"])
    return extractions


def extract_and_save_thread(
    thread_doc: dict[str, Any],
    extraction_path: Path,
    endpoint: str,
    model: str,
    timeout: int,
) -> dict[str, Any]:
    extraction = extract_thread_reviews(thread_doc, endpoint, model, timeout)
    save_json(extraction_path, extraction)
    return extraction


def load_saved_topics(limit: int) -> list[dict[str, Any]]:
    if not INDEX_PATH.exists():
        raise FileNotFoundError(
            f"Saved topic index not found: {INDEX_PATH}. Run once without --extract-only first."
        )
    indexed_topics = load_json(INDEX_PATH)
    if not isinstance(indexed_topics, list):
        raise RuntimeError(f"Saved topic index is invalid: {INDEX_PATH}")
    return indexed_topics[:limit]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and incrementally extract Shuiyuan food reviews with a local LLM.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of latest topics to process.")
    parser.add_argument("--endpoint", default=DEFAULT_LLM_ENDPOINT, help="OpenAI-compatible local LLM endpoint.")
    parser.add_argument("--model", default=DEFAULT_LLM_MODEL, help="Model name sent to the local LLM endpoint.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="LLM request timeout in seconds.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent extraction workers.")
    parser.add_argument("--extract-only", action="store_true", help="Skip crawling and use saved topic/thread data only.")
    parser.add_argument("--force-fetch", action="store_true", help="Refetch thread JSON even if unchanged.")
    parser.add_argument("--force-extract", action="store_true", help="Re-run LLM extraction even if unchanged.")
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        args = parse_args()
        ensure_dirs()
        if args.extract_only:
            session = None
            topics = load_saved_topics(args.limit)
        else:
            cookie = read_cookie()
            session = build_session(cookie)
            topics = fetch_latest_topics(session, args.limit)
            indexed_topics = [
                {
                    "id": topic.get("id"),
                    "title": topic.get("title"),
                    "slug": topic.get("slug"),
                    "category_id": topic.get("category_id"),
                    "tags": topic.get("tags", []),
                    "posts_count": topic.get("posts_count"),
                    "reply_count": topic.get("reply_count"),
                    "created_at": topic.get("created_at"),
                    "last_posted_at": topic.get("last_posted_at"),
                    "topic_url": build_topic_url(topic),
                    "topic_signature": topic_signature(topic),
                }
                for topic in topics
            ]
            save_json(INDEX_PATH, indexed_topics)

        extractions = process_topics(
            session=session,
            topics=topics,
            endpoint=args.endpoint,
            model=args.model,
            timeout=args.timeout,
            workers=args.workers,
            force_fetch=args.force_fetch,
            force_extract=args.force_extract,
            extract_only=args.extract_only,
        )

        merchant_book = build_merchant_book(extractions)
        save_json(MERCHANT_BOOK_PATH, merchant_book)
        render_reports(merchant_book)

        print(f"Saved topic index to {INDEX_PATH}")
        print(f"Saved thread JSON to {THREADS_DIR}")
        print(f"Saved extraction JSON to {EXTRACTIONS_DIR}")
        print(f"Saved merchant book to {MERCHANT_BOOK_PATH}")
        print(f"Saved markdown reports to {REPORTS_DIR}")
    except KeyboardInterrupt:
        print("Stopped by user.")
        raise SystemExit(130)


if __name__ == "__main__":
    main()
