# OpenClaims — Medicare Part D Drug Spend Pipeline

An end-to-end, production-style data platform built on public CMS Medicare data.
**Airflow → DuckDB → dbt → Streamlit**, with data quality gates, idempotent loads,
and a PySpark path for full-scale (25M+ row) processing.

> Built by a healthcare data engineer to answer a real question:
> *which drugs drive Medicare Part D spend, where, and which prescribers are cost outliers?*

## Architecture

```
 data.cms.gov API                    DuckDB                       Streamlit
┌─────────────────┐    ┌──────────────────────────────┐    ┌──────────────────┐
│  Part D          │    │  raw.part_d_prescribers      │    │  Spend by state  │
│  Prescribers     │───▶│    ↓ dbt (staging views)     │───▶│  Top drugs       │
│  by Provider/Drug│    │  stg_prescribers             │    │  Outlier         │
└─────────────────┘    │    ↓ dbt (mart tables)       │    │  prescribers     │
   Airflow DAG          │  mart_drug_spend_by_state    │    └──────────────────┘
   (extract, load,      │  mart_top_prescribers        │
   dbt run, dbt test)   └──────────────────────────────┘
                              ▲
        PySpark job ──────────┘  (silver Parquet path for full-scale data)
```

**Pipeline stages** (see `dags/cms_prescriber_pipeline.py`):

1. `extract_cms_api` — pages through the CMS JSON API (flat memory footprint at any dataset size), lands bronze JSON partitioned by ingest date
2. `load_raw_duckdb` — idempotent load into `raw` schema (safe re-runs)
3. `dbt_run` — staging views (rename/type/clean) → mart tables (aggregations, national benchmarks, outlier flags)
4. `dbt_test_quality_gate` — 7 tests; **the DAG fails before bad data reaches the marts**

## Engineering decisions

- **Quality gate as a DAG task, not an afterthought.** `dbt test` runs as a first-class Airflow task after `dbt run`. Failed tests fail the pipeline.
- **Idempotency everywhere.** Extraction partitions by run date; loads use replace semantics; the PySpark job writes with overwrite + partitioning. Any task can be re-run safely.
- **Outlier detection in the mart layer.** `mart_top_prescribers` benchmarks each prescriber's cost-per-claim against the national average for that drug and flags >3× outliers — the same pattern used in payment-integrity analytics.
- **DuckDB for dev, Spark for scale.** The full annual Part D file is ~25M rows. `spark_jobs/transform_prescribers.py` implements the same transformations with explicit schemas, window-function dedup, and state-partitioned Parquet — swap the warehouse target without touching the dbt layer.
- **Offline-first development.** `scripts/seed_sample_data.py` generates API-shaped sample data (string-typed numerics and all), so the full pipeline runs without network access.

## Quickstart (no Docker needed)

```bash
pip install -r requirements.txt
make seed        # generate sample data + load DuckDB
make dbt         # build staging + marts
make test        # run the 7-test quality gate
make dashboard   # open the Streamlit app
```

## Run with Airflow

```bash
docker compose up -d          # Airflow standalone on http://localhost:8080
# set CMS_DATASET_ID in docker-compose.yml (verify the dataset UUID on data.cms.gov)
# then trigger the `cms_prescriber_pipeline` DAG
```

## Data

[Medicare Part D Prescribers — by Provider and Drug](https://data.cms.gov) —
public, de-identified at the prescriber level (rows with <11 claims are suppressed
by CMS). One row per prescriber (NPI) × drug × year.

## Repo layout

```
dags/                      Airflow DAG (extract → load → dbt run → dbt test)
dbt/openclaims/            dbt project (DuckDB adapter)
  models/staging/          typed & cleaned views + tests
  models/marts/            spend-by-state, prescriber outliers + tests
spark_jobs/                PySpark transformation for full-scale data
dashboard/                 Streamlit app reading the marts
scripts/                   offline sample-data seeder
```

## Roadmap

- [ ] Incremental dbt models keyed on ingest date
- [ ] Snowflake target profile alongside DuckDB
- [ ] Great Expectations / dbt-expectations for distribution-level checks
- [ ] Year-over-year spend trend marts (CMS publishes multiple years)
