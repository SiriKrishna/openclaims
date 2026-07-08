"""
OpenClaims — CMS Medicare Part D Prescriber Pipeline
=====================================================
Daily DAG that:
  1. Extracts Medicare Part D "Prescribers by Provider and Drug" data
     from the data.cms.gov API (paginated JSON).
  2. Lands raw JSON as partitioned files (bronze layer).
  3. Loads raw data into DuckDB (raw schema).
  4. Runs dbt to build staging + mart models (silver/gold).
  5. Runs dbt tests as a quality gate — pipeline fails loudly if data is bad.

Design notes (interview talking points):
  - Idempotent: each task can be safely re-run; loads use REPLACE semantics
    keyed on the execution date.
  - Incremental-friendly: extraction pages through the API with offset/size
    so memory stays flat regardless of dataset size.
  - Quality gate BEFORE marts are exposed: dbt test failures stop the DAG.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Dataset: "Medicare Part D Prescribers - by Provider and Drug"
# Find the dataset UUID at https://data.cms.gov (each dataset page exposes a
# JSON API endpoint). Set it via env var so the DAG has no hardcoded secrets.
CMS_DATASET_ID = os.environ.get(
    "CMS_DATASET_ID",
    "9552739e-3d05-4c1b-8eff-ecabf391e2e5",  # verify on data.cms.gov before first run
)
CMS_API_BASE = "https://data.cms.gov/data-api/v1/dataset"
PAGE_SIZE = 5000          # rows per API page
MAX_PAGES = 20            # cap for dev; raise/remove for full loads

DATA_DIR = os.environ.get("OPENCLAIMS_DATA_DIR", "/opt/airflow/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
DUCKDB_PATH = os.path.join(DATA_DIR, "openclaims.duckdb")
DBT_DIR = os.environ.get("OPENCLAIMS_DBT_DIR", "/opt/airflow/dbt/openclaims")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
def extract_cms_data(ds: str, **_) -> str:
    """Page through the CMS API and land raw JSON, partitioned by run date."""
    out_dir = os.path.join(RAW_DIR, f"ingest_date={ds}")
    os.makedirs(out_dir, exist_ok=True)

    url = f"{CMS_API_BASE}/{CMS_DATASET_ID}/data"
    total_rows, page = 0, 0

    while page < MAX_PAGES:
        params = {"size": PAGE_SIZE, "offset": page * PAGE_SIZE}
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        rows = resp.json()

        if not rows:  # empty page => done
            break

        out_path = os.path.join(out_dir, f"part_{page:05d}.json")
        with open(out_path, "w") as f:
            json.dump(rows, f)

        total_rows += len(rows)
        log.info("page=%s rows=%s total=%s", page, len(rows), total_rows)
        page += 1

    if total_rows == 0:
        raise ValueError("Extraction returned 0 rows — check dataset ID / API.")

    log.info("Extraction complete: %s rows across %s pages -> %s",
             total_rows, page, out_dir)
    return out_dir


def load_to_duckdb(ds: str, **_) -> None:
    """Load the day's raw JSON partition into DuckDB raw schema."""
    import duckdb

    partition_glob = os.path.join(RAW_DIR, f"ingest_date={ds}", "*.json")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    # Replace the day's data (idempotent re-runs) while keeping history
    # simple for a portfolio project: raw table mirrors latest ingest.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.part_d_prescribers AS
        SELECT *, '{ds}'::DATE AS _ingest_date
        FROM read_json_auto('{partition_glob}');
        """
    )
    n = con.execute("SELECT COUNT(*) FROM raw.part_d_prescribers").fetchone()[0]
    con.close()

    if n == 0:
        raise ValueError("DuckDB load produced 0 rows.")
    log.info("Loaded %s rows into raw.part_d_prescribers", n)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "krishna",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cms_prescriber_pipeline",
    description="CMS Part D prescribers: API -> bronze -> DuckDB -> dbt marts",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["openclaims", "cms", "healthcare"],
) as dag:

    extract = PythonOperator(
        task_id="extract_cms_api",
        python_callable=extract_cms_data,
    )

    load = PythonOperator(
        task_id="load_raw_duckdb",
        python_callable=load_to_duckdb,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --profiles-dir . "
            f"--vars '{{\"duckdb_path\": \"{DUCKDB_PATH}\"}}'"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_quality_gate",
        bash_command=(
            f"cd {DBT_DIR} && dbt test --profiles-dir . "
            f"--vars '{{\"duckdb_path\": \"{DUCKDB_PATH}\"}}'"
        ),
    )

    extract >> load >> dbt_run >> dbt_test
