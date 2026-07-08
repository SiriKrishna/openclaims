# OpenClaims — Status & Roadmap

## ✅ Done (v1 — July 2026)

- **Real data:** 50k rows of the 2024-12-01 CMS release loaded via runtime DCAT catalog resolution (no hardcoded dataset IDs)
- **Quality gate win:** uniqueness test caught wrong grain assumption on real data (2,148 violations); staging grain corrected to (npi, brand, generic), severity raised to error
- Deprecations fixed: dbt test `arguments:` nesting, Streamlit `width='stretch'`

- End-to-end pipeline scaffolded and verified locally: seed → DuckDB → dbt → dashboard
- Airflow DAG written: extract (paginated CMS API) → load (idempotent) → `dbt run` → `dbt test` as a quality gate
- dbt project: 1 staging model, 2 marts, 7 passing data tests (`dbt_utils` via git package)
- Marts: state-level drug spend with in-state ranking; prescriber cost benchmarking with >3× national avg cost/claim outlier flag
- PySpark job for full-scale path: explicit schema, window-function dedup, state-partitioned Parquet
- Streamlit dashboard: KPIs, spend by state, top drugs per state, outlier prescriber table
- Offline-first dev: sample-data seeder mirrors the CMS API shape (string-typed numerics included)
- Published to GitHub

## 🔜 Next up (priority order)

1. **Airflow path** — test docker compose end to end; refactor DAG extract to
   reuse the DCAT catalog resolution from `scripts/fetch_real_cms.py`.
2. **ML anomaly detection layer (Phase 2)** — per-prescriber feature vectors
   (cost/claim vs same-specialty-and-state peers, drug-mix concentration,
   brand-vs-generic ratio, volume z-scores) → Isolation Forest → clustering to
   characterize anomaly types → "flagged for review" dashboard view with
   per-feature explanations. Validation idea: cross-reference flagged NPIs
   against the public OIG LEIE exclusion list.
   *Framing note: outputs are statistical anomalies warranting review — never
   accusations.*
3. **CI** — GitHub Actions: seed + `dbt build` + `dbt test` on every PR.
4. **Public dashboard deploy** — Streamlit Community Cloud.

## 🧹 Small fixes queued

- Nest `dbt_utils.unique_combination_of_columns` args under `arguments:` (dbt 1.11 deprecation)
- Replace deprecated `use_container_width` with `width='stretch'` in `dashboard/app.py`
- Incremental dbt models keyed on `_ingest_date`
- Optional Snowflake target profile alongside DuckDB
- Multi-year CMS pulls → YoY trend marts
