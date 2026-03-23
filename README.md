# Shuiyuan Food Review Book

A cleaned-up, open-source-friendly project for:

- exporting Shuiyuan forum topics
- extracting food-related opinions from the `滋滋猪鸡` category
- normalizing merchant names
- rendering human-readable markdown reports
- exporting the final result as a zip archive

This project is based on the original `shuiyuan_exporter` repository and keeps the original topic-export functionality while adding an opinion-extraction pipeline.

## What Is Included

- `main.py`: original topic exporter entrypoint
- `food_review_pipeline.py`: crawl / extract / normalize / aggregate pipeline
- `render_merchant_reports.py`: render existing `merchant_book.json` into markdown reports
- `export_reports.py`: package reports and JSON output into a zip archive
- `fetch_food_titles.py`: quick test script for latest food-category topic titles

## What Is Not Included

This public project intentionally does **not** include:

- `cookies.txt`
- any grabbed forum content
- `food_review_data/`
- `posts/`
- `exports/`
- local model gateway code or secrets
- `.env` files or API keys

## Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

## Cookie Setup

You need a valid Shuiyuan login cookie.

1. Log in on the Shuiyuan web site.
2. Open browser devtools.
3. Find a request to `shuiyuan.sjtu.edu.cn`.
4. Copy the full `Cookie` request header value.
5. Save it into `cookies.txt` in the project root.

Do not commit `cookies.txt`.

## Local LLM API

The extraction pipeline expects an OpenAI-compatible Chat Completions endpoint.

Default endpoint:

```text
http://localhost:8088/api/v1/chat/completions
```

Expected request shape:

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

Expected response shape:

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

The model does not need to be OpenAI-hosted. It only needs to support the same HTTP interface.

## Common Workflows

Fetch and extract the latest 50 topics:

```bash
python food_review_pipeline.py --limit 50 --workers 2
```

Use existing saved threads only and re-run extraction:

```bash
python food_review_pipeline.py --limit 50 --extract-only --force-extract --workers 2
```

Render markdown reports from an existing `merchant_book.json`:

```bash
python render_merchant_reports.py
```

Export reports into a zip archive:

```bash
python export_reports.py --overwrite
```

Include thread and extraction JSON in the export:

```bash
python export_reports.py --with-threads --with-extractions --overwrite
```

## Output Structure

Generated files are written to `food_review_data/`:

- `topic_index.json`
- `threads/*.json`
- `extractions/*.json`
- `merchant_book.json`
- `reports/index.md`
- `reports/merchants/*.md`

## Opening the Project on GitHub

Before publishing:

1. Make sure `cookies.txt` is absent.
2. Make sure `food_review_data/`, `posts/`, and `exports/` are absent.
3. Check that no `.env` or private config files are present.
4. Review the generated merchant names and extracted text samples for anything you do not want to publish.

## Attribution

This project is derived from the original `shuiyuan_exporter` project. Keep the upstream license and attribution when redistributing.
