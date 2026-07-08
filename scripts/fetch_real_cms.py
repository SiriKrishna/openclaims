"""
Fetch REAL CMS Medicare Part D prescriber data into DuckDB.

Instead of hardcoding a dataset UUID, this resolves it at runtime from CMS's
machine-readable DCAT catalog (data.cms.gov/data.json) by dataset title —
robust to CMS republishing new data years.

Usage:
  python scripts/fetch_real_cms.py                 # default: 50k rows
  python scripts/fetch_real_cms.py --max-rows 200000
  python scripts/fetch_real_cms.py --list          # just show resolved API URL

Then rebuild the warehouse:
  make dbt && make test && make dashboard
"""

import argparse
import json
import os
from datetime import date

import duckdb
import requests

CATALOG_URL = "https://data.cms.gov/data.json"
DATASET_TITLE = "Medicare Part D Prescribers - by Provider and Drug"
PAGE_SIZE = 5000

HERE = os.path.dirname(__file__)
DATA_DIR = os.path.join(HERE, "..", "data")
DB_PATH = os.path.join(DATA_DIR, "openclaims.duckdb")


def resolve_api_url() -> str:
    """Find the latest data-api endpoint for the dataset via the DCAT catalog."""
    print(f"Resolving dataset from catalog: {CATALOG_URL}")
    cat = requests.get(CATALOG_URL, timeout=120).json()

    matches = [
        ds for ds in cat.get("dataset", [])
        if ds.get("title", "").strip().lower() == DATASET_TITLE.lower()
    ]
    if not matches:
        raise SystemExit(
            f"Dataset titled '{DATASET_TITLE}' not found in catalog. "
            "Check the title on data.cms.gov."
        )

    ds = matches[0]
    # Distributions include API endpoints (format 'API') per data year;
    # 'latest' style access URLs look like .../data-api/v1/dataset/{uuid}/data
    api_dists = [
        d for d in ds.get("distribution", [])
        if "data-api" in (d.get("accessURL") or "")
    ]
    if not api_dists:
        raise SystemExit("No data-api distribution found for the dataset.")

    # Prefer the 'latest' distribution if present, else the first (newest year)
    latest = next(
        (d for d in api_dists if "latest" in (d.get("accessURL") or "").lower()),
        api_dists[0],
    )
    url = latest["accessURL"]
    print(f"Resolved API endpoint: {url}")
    print(f"Distribution title: {latest.get('title', 'n/a')}")
    return url


def fetch_pages(api_url: str, max_rows: int) -> str:
    today = date.today().isoformat()
    out_dir = os.path.join(DATA_DIR, "raw", f"ingest_date={today}")
    os.makedirs(out_dir, exist_ok=True)

    total, page = 0, 0
    while total < max_rows:
        params = {"size": PAGE_SIZE, "offset": page * PAGE_SIZE}
        resp = requests.get(api_url, params=params, timeout=180)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        path = os.path.join(out_dir, f"part_{page:05d}.json")
        with open(path, "w") as f:
            json.dump(rows, f)
        total += len(rows)
        page += 1
        print(f"  page {page}: +{len(rows)} rows (total {total:,})")

    if total == 0:
        raise SystemExit("API returned 0 rows — inspect the resolved URL manually.")
    print(f"Landed {total:,} rows -> {out_dir}")
    return out_dir


def load_duckdb(raw_dir: str) -> None:
    today = date.today().isoformat()
    glob = os.path.join(raw_dir, "*.json")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.part_d_prescribers AS
        SELECT *, '{today}'::DATE AS _ingest_date
        FROM read_json_auto('{glob}', union_by_name=true);
        """
    )
    n = con.execute("SELECT COUNT(*) FROM raw.part_d_prescribers").fetchone()[0]
    con.close()
    print(f"Loaded {n:,} REAL rows into raw.part_d_prescribers")
    print("Next: make dbt && make test && make dashboard")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--max-rows", type=int, default=50_000,
                   help="row cap for this pull (full file is ~25M rows)")
    p.add_argument("--list", action="store_true",
                   help="only resolve and print the API URL, don't download")
    args = p.parse_args()

    url = resolve_api_url()
    if args.list:
        raise SystemExit(0)
    raw_dir = fetch_pages(url, args.max_rows)
    load_duckdb(raw_dir)
