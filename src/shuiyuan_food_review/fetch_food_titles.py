import json
import sys

from .utils import ReqParam, make_request


FOOD_CATEGORY_JSON = "https://shuiyuan.sjtu.edu.cn/c/leisure-entertainment/food/64.json"
LATEST_LIMIT = 20


def fetch_latest_food_topics(limit: int = LATEST_LIMIT) -> list[dict]:
    response = make_request(ReqParam(FOOD_CATEGORY_JSON), once=False)
    response.raise_for_status()

    data = json.loads(response.text)
    topics = data.get("topic_list", {}).get("topics", [])
    return topics[:limit]


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    topics = fetch_latest_food_topics()
    for index, topic in enumerate(topics, start=1):
        print(f"{index:02d}. {topic.get('title', '<untitled>')}")


if __name__ == "__main__":
    main()
