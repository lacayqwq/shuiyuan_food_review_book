import json
import re
from pathlib import Path
from typing import Any


DATA_DIR = Path("food_review_data")
MERCHANT_BOOK_PATH = DATA_DIR / "merchant_book.json"
REPORTS_DIR = DATA_DIR / "reports"
MERCHANT_REPORTS_DIR = REPORTS_DIR / "merchants"


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


def ensure_dirs() -> None:
    REPORTS_DIR.mkdir(exist_ok=True)
    MERCHANT_REPORTS_DIR.mkdir(exist_ok=True)


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
        excerpt = markdown_escape(opinion.get("original_excerpt"))
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
                "> " + "\n> ".join(excerpt.splitlines()) if excerpt else "> (无)",
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


def main() -> None:
    ensure_dirs()
    merchant_book = load_json(MERCHANT_BOOK_PATH)
    render_reports(merchant_book)
    print(f"Rendered markdown reports to {REPORTS_DIR}")


if __name__ == "__main__":
    main()
