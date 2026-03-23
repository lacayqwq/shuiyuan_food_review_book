# Shuiyuan Food Review Book

[中文说明](./README.zh-CN.md)

A lightweight project for exporting Shuiyuan topics, extracting food-related opinions, normalizing merchant names, rendering markdown reports, and packaging the final result.

This project is based on the original `shuiyuan_exporter` and keeps the original exporter while adding a food-review analysis pipeline.

## Features

- Export Shuiyuan topics as markdown
- Crawl the `滋滋猪鸡` category and extract opinions from posts and replies
- Normalize merchant names and aggregate opinions by merchant
- Render readable markdown reports
- Export reports and JSON outputs as a zip archive

## Quick Start

Install dependencies:

```bash
pip install -r requirements.txt
```

Prepare `cookies.txt` in the project root with a valid Shuiyuan login cookie.

Run the review pipeline:

```bash
python food_review_pipeline.py --limit 50 --workers 2
```

Render markdown reports from an existing `merchant_book.json`:

```bash
python render_merchant_reports.py
```

Export reports into a zip archive:

```bash
python export_reports.py --overwrite
```

## CLI Entrypoints

- `main.py`: original topic exporter
- `food_review_pipeline.py`: crawl / extract / normalize / aggregate
- `render_merchant_reports.py`: render markdown reports
- `export_reports.py`: export a zip archive
- `fetch_food_titles.py`: quick category title check

## Local LLM API

The extraction pipeline expects an OpenAI-compatible Chat Completions endpoint.

Default endpoint:

```text
http://localhost:8088/api/v1/chat/completions
```

Minimal request shape:

```json
{
  "model": "your-model-name",
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "..."}
  ],
  "temperature": 0.1
}
```

Minimal response shape:

```json
{
  "choices": [
    {
      "message": {
        "content": "{ ...json string... }"
      }
    }
  ]
}
```

## Project Layout

```text
.
├─ src/shuiyuan_food_review/    # implementation modules
├─ tooling/                     # packaging / helper files
├─ main.py
├─ food_review_pipeline.py
├─ render_merchant_reports.py
├─ export_reports.py
└─ fetch_food_titles.py
```

## Output

Generated files are written to `food_review_data/`:

- `topic_index.json`
- `threads/*.json`
- `extractions/*.json`
- `merchant_book.json`
- `reports/index.md`
- `reports/merchants/*.md`

## Attribution

Derived from the original `shuiyuan_exporter` project. Keep upstream attribution and license when redistributing.
