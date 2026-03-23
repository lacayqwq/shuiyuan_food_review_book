import argparse
import zipfile
from pathlib import Path


DATA_DIR = Path("food_review_data")
REPORTS_DIR = DATA_DIR / "reports"
THREADS_DIR = DATA_DIR / "threads"
EXTRACTIONS_DIR = DATA_DIR / "extractions"
MERCHANT_BOOK_PATH = DATA_DIR / "merchant_book.json"
TOPIC_INDEX_PATH = DATA_DIR / "topic_index.json"
EXPORTS_DIR = Path("exports")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Shuiyuan food review outputs into a zip archive.")
    parser.add_argument("--name", default="food_review_export", help="Base filename for the exported zip.")
    parser.add_argument("--with-threads", action="store_true", help="Include saved thread JSON files.")
    parser.add_argument("--with-extractions", action="store_true", help="Include extraction JSON files.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the target zip if it already exists.")
    return parser.parse_args()


def ensure_export_inputs() -> None:
    if not REPORTS_DIR.exists():
        raise FileNotFoundError(f"Reports directory not found: {REPORTS_DIR}")
    if not MERCHANT_BOOK_PATH.exists():
        raise FileNotFoundError(f"Merchant book not found: {MERCHANT_BOOK_PATH}")
    if not TOPIC_INDEX_PATH.exists():
        raise FileNotFoundError(f"Topic index not found: {TOPIC_INDEX_PATH}")
    EXPORTS_DIR.mkdir(exist_ok=True)


def add_path_to_zip(zip_file: zipfile.ZipFile, path: Path, arc_prefix: str = "") -> None:
    if path.is_file():
        arcname = Path(arc_prefix) / path.name if arc_prefix else Path(path.name)
        zip_file.write(path, arcname=str(arcname))
        return

    for file in path.rglob("*"):
        if file.is_file():
            relative = file.relative_to(path)
            arcname = Path(arc_prefix) / path.name / relative if arc_prefix else Path(path.name) / relative
            zip_file.write(file, arcname=str(arcname))


def main() -> None:
    args = parse_args()
    ensure_export_inputs()

    zip_path = EXPORTS_DIR / f"{args.name}.zip"
    if zip_path.exists():
        if not args.overwrite:
            raise FileExistsError(f"Export already exists: {zip_path}. Use --overwrite to replace it.")
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        add_path_to_zip(zip_file, REPORTS_DIR)
        add_path_to_zip(zip_file, MERCHANT_BOOK_PATH, arc_prefix="food_review_data")
        add_path_to_zip(zip_file, TOPIC_INDEX_PATH, arc_prefix="food_review_data")

        if args.with_threads and THREADS_DIR.exists():
            add_path_to_zip(zip_file, THREADS_DIR)

        if args.with_extractions and EXTRACTIONS_DIR.exists():
            add_path_to_zip(zip_file, EXTRACTIONS_DIR)

    print(f"Exported archive to {zip_path}")


if __name__ == "__main__":
    main()
