.PHONY: up down seed dbt test dashboard

up:            ## Start Airflow (UI at http://localhost:8080, user: admin)
	docker compose up -d

down:
	docker compose down

seed:          ## Generate local sample data + load DuckDB (no Docker needed)
	python scripts/seed_sample_data.py

dbt:           ## Run dbt models locally
	cd dbt/openclaims && dbt deps --profiles-dir . && dbt run --profiles-dir .

test:          ## Run dbt tests (quality gate)
	cd dbt/openclaims && dbt test --profiles-dir .

dashboard:     ## Launch the Streamlit dashboard
	streamlit run dashboard/app.py
