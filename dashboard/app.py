"""
OpenClaims dashboard — Medicare Part D drug spend explorer.
Reads the dbt marts straight out of DuckDB.

Run:  streamlit run dashboard/app.py
"""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "openclaims.duckdb")

st.set_page_config(page_title="OpenClaims — Part D Spend", layout="wide")
st.title("OpenClaims — Medicare Part D Drug Spend")
st.caption("CMS public data · Airflow → DuckDB → dbt → Streamlit")


@st.cache_data(ttl=300)
def load(query: str) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(query).df()
    con.close()
    return df


try:
    spend = load("SELECT * FROM main_marts.mart_drug_spend_by_state")
    prescribers = load("SELECT * FROM main_marts.mart_top_prescribers")
except Exception as e:
    st.error(
        "Marts not found. Run the pipeline first:\n\n"
        "`make seed && make dbt`\n\n"
        f"({e})"
    )
    st.stop()

# ---- KPIs -----------------------------------------------------------------
total_cost = spend["total_drug_cost"].sum()
total_claims = spend["total_claims"].sum()
outliers = int(prescribers["is_cost_outlier"].sum())

c1, c2, c3 = st.columns(3)
c1.metric("Total drug cost", f"${total_cost:,.0f}")
c2.metric("Total claims", f"{total_claims:,.0f}")
c3.metric("Cost-outlier records", f"{outliers:,}")

# ---- State spend ----------------------------------------------------------
st.subheader("Drug spend by state")
by_state = (
    spend.groupby("state", as_index=False)["total_drug_cost"].sum()
    .sort_values("total_drug_cost", ascending=False)
)
st.plotly_chart(
    px.bar(by_state, x="state", y="total_drug_cost",
           labels={"total_drug_cost": "Total drug cost ($)"}),
    use_container_width=True,
)

# ---- Top drugs in a chosen state -----------------------------------------
st.subheader("Top drugs within a state")
state = st.selectbox("State", sorted(spend["state"].unique()))
top = (
    spend[spend["state"] == state]
    .sort_values("spend_rank_in_state")
    .head(10)
    [["generic_name", "prescriber_count", "total_claims",
      "total_drug_cost", "avg_cost_per_claim"]]
)
st.dataframe(top, use_container_width=True, hide_index=True)

# ---- Outlier prescribers ---------------------------------------------------
st.subheader("Cost-outlier prescribers (>3× national avg cost/claim)")
out_df = (
    prescribers[prescribers["is_cost_outlier"] == True]  # noqa: E712
    .sort_values("total_drug_cost", ascending=False)
    .head(25)
    [["npi", "prescriber_last_name", "state", "prescriber_type",
      "generic_name", "total_claims", "cost_per_claim",
      "natl_avg_cost_per_claim"]]
)
st.dataframe(out_df, use_container_width=True, hide_index=True)
