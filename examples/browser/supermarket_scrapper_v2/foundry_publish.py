# foundry_publish.py
from __future__ import annotations
import asyncio
import inspect
import os
import json
import csv
import io
from pathlib import Path
from typing import Dict, Any, Iterable, List, Set

from supermarket_scrapper import main as run_scraper
from foundry import FoundryUploader

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = BASE_DIR / "output"        # contains Country/Supermarket/*.json
LOCAL_CSV = OUTPUT_DIR / "last_run.csv" # local CSV copy
CSV_FILENAME = os.environ.get("CSV_FILENAME", "supermarket.csv")

DESIRED_COLUMNS = [
    "scrapped_date", "country", "supermarket_name", "name", "subtype",
    "website_product_name", "bio",
    "price_per_kg", "price_per_unit", "currency",
    "original_price_info", "estimation_notes",
    "model_used", "tokens_used", "total_cost",
]

# --- SCRAPER RUN ---
def run_scraper_step():
    print("[publisher] launching scraper to refresh JSONs...")
    if inspect.iscoroutinefunction(run_scraper):
        asyncio.run(run_scraper())  # ✅ actually executes the async code
    else:
        run_scraper() # type: ignore
    print("[publisher] scraper run completed.\n")

# --- PARSING ---
def json_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Output directory not found: {root}")
    for country_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for market_dir in sorted(p for p in country_dir.iterdir() if p.is_dir()):
            for jf in sorted(market_dir.glob("*.json")):
                yield jf

def load_product_records() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for jf in json_files(OUTPUT_DIR):
        country = jf.parents[1].name
        supermarket = jf.parents[0].name
        try:
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[warn] failed reading {jf}: {e}")
            continue

        # normalize to list of dicts
        if isinstance(data, dict) and "products" in data and isinstance(data["products"], list):
            recs = data["products"]
        elif isinstance(data, list):
            recs = data
        elif isinstance(data, dict):
            recs = [data]
        else:
            print(f"[warn] unexpected shape in {jf}")
            continue

        for r in recs:
            if not isinstance(r, dict):
                continue
            r.setdefault("country", country)
            r.setdefault("supermarket_name", supermarket)
            rows.append(clean_nulls(r))
    return rows

def clean_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        out[k] = None if v in ("", [], {}, None) else v
    return out

def compute_headers(rows: List[Dict[str, Any]]) -> List[str]:
    all_keys: Set[str] = set()
    for r in rows:
        all_keys.update(r.keys())
    ordered = [c for c in DESIRED_COLUMNS if c in all_keys]
    extras = sorted(k for k in all_keys if k not in DESIRED_COLUMNS)
    return ordered + extras

def to_csv_bytes(rows: List[Dict[str, Any]], headers: List[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        full = {h: (r.get(h) if h in r else None) for h in headers}
        writer.writerow(full)
    return buf.getvalue().encode("utf-8")

# --- MAIN PIPELINE ---
def main():
    run_scraper_step()  # 1. run scraper first

    print(f"[publisher] parsing JSONs in: {OUTPUT_DIR}")
    rows = load_product_records()
    if not rows:
        print("[publisher] no product records found; nothing to upload.")
        # write a tiny breadcrumb CSV so we can inspect in artifacts
        header = "note\n"
        (OUTPUT_DIR / "last_run.csv").write_text(header + "no_rows_found\n", encoding="utf-8")
        print(f"[publisher] wrote EMPTY breadcrumb CSV: {(OUTPUT_DIR / 'last_run.csv').resolve()}")
        print("[publisher] skipping Foundry upload due to 0 rows.")
        return

    headers = compute_headers(rows)
    csv_bytes = to_csv_bytes(rows, headers)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_CSV.write_bytes(csv_bytes)
    print(f"[publisher] wrote local CSV: {LOCAL_CSV} ({len(rows)} rows)")

    # 2. upload to Foundry
    uploader = FoundryUploader()
    dataset_path = uploader.unique_dataset_path(CSV_FILENAME)
    resp = uploader.upload_bytes(csv_bytes, dataset_path)
    print(f"[publisher] uploaded to Foundry: {dataset_path} -> {resp.status_code}")

if __name__ == "__main__":
    main()
