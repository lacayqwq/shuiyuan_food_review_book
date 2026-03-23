# 水源美食评论簿

[English](./README.md)

这是一个轻量级项目，用于导出水源帖子、抽取美食相关观点、归一化店家名称、生成 markdown 报告，并将结果打包导出。

本项目基于原始 `shuiyuan_exporter` 改造，保留了原始导出功能，并新增了面向 `滋滋猪鸡` 板块的食评分析流程。

## 功能

- 将水源帖子导出为 markdown
- 抓取 `滋滋猪鸡` 板块并抽取主楼与回复中的观点
- 归一化店家名称，并按店家聚合评论
- 生成可阅读的 markdown 报告
- 将报告和 JSON 结果打包导出为 zip

## 快速开始

安装依赖：

```bash
pip install -r requirements.txt
```

在项目根目录准备 `cookies.txt`，内容为有效的水源登录 cookie。

运行食评分析流程：

```bash
python food_review_pipeline.py --limit 50 --workers 2
```

基于已有 `merchant_book.json` 生成 markdown 报告：

```bash
python render_merchant_reports.py
```

导出结果为 zip：

```bash
python export_reports.py --overwrite
```

## 入口脚本

- `main.py`：原始帖子导出器
- `food_review_pipeline.py`：抓取 / 抽取 / 归一化 / 聚合流程
- `render_merchant_reports.py`：生成 markdown 报告
- `export_reports.py`：打包导出 zip
- `fetch_food_titles.py`：快速查看分类页最新标题

## 本地 LLM API

抽取流程要求一个兼容 OpenAI Chat Completions 的接口。

默认地址：

```text
http://localhost:8088/api/v1/chat/completions
```

请求体大致格式：

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

返回体大致格式：

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

## 项目结构

```text
.
├─ src/shuiyuan_food_review/    # 核心实现模块
├─ tooling/                     # 打包 / 辅助文件
├─ main.py
├─ food_review_pipeline.py
├─ render_merchant_reports.py
├─ export_reports.py
└─ fetch_food_titles.py
```

## 输出目录

生成结果写入 `food_review_data/`：

- `topic_index.json`
- `threads/*.json`
- `extractions/*.json`
- `merchant_book.json`
- `reports/index.md`
- `reports/merchants/*.md`

## 致谢

本项目派生自原始 `shuiyuan_exporter`，公开分发时请保留上游署名和许可证信息。
