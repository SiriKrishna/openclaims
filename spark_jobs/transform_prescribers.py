"""
OpenClaims — PySpark transformation job
========================================
Reads the raw CMS Part D prescriber JSON (bronze) and produces a cleaned,
typed Parquet dataset (silver) suitable for warehouse loading or analysis.

Why Spark here (interview talking point):
  The full Part D prescriber file is ~25M rows / several GB per year.
  DuckDB handles the dev sample fine, but this job demonstrates the
  distributed pattern you'd use at production scale: explicit schema,
  column pruning, null handling, dedup, and partitioned Parquet output.

Run locally:
  spark-submit spark_jobs/transform_prescribers.py \
      --input data/raw/ingest_date=2026-07-07 \
      --output data/silver/prescribers
"""

import argparse

from pyspark.sql import SparkSession, functions as F, Window


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("openclaims-transform-prescribers")
        .config("spark.sql.shuffle.partitions", "8")  # sized for local dev
        .getOrCreate()
    )


def transform(spark: SparkSession, input_path: str, output_path: str) -> None:
    raw = spark.read.json(input_path)

    cleaned = (
        raw
        # --- standardize column names (CMS uses Prscrbr_* abbreviations) ---
        .select(
            F.col("Prscrbr_NPI").alias("npi"),
            F.initcap(F.trim(F.col("Prscrbr_Last_Org_Name"))).alias("prescriber_last_name"),
            F.initcap(F.trim(F.col("Prscrbr_First_Name"))).alias("prescriber_first_name"),
            F.upper(F.trim(F.col("Prscrbr_State_Abrvtn"))).alias("state"),
            F.trim(F.col("Prscrbr_Type")).alias("prescriber_type"),
            F.trim(F.col("Brnd_Name")).alias("brand_name"),
            F.trim(F.col("Gnrc_Name")).alias("generic_name"),
            F.col("Tot_Clms").cast("long").alias("total_claims"),
            F.col("Tot_Day_Suply").cast("long").alias("total_day_supply"),
            F.col("Tot_Drug_Cst").cast("double").alias("total_drug_cost"),
        )
        # --- basic quality filters ---
        .where(F.col("npi").isNotNull())
        .where(F.col("total_claims") > 0)
        # --- derived metrics ---
        .withColumn(
            "cost_per_claim",
            F.round(F.col("total_drug_cost") / F.col("total_claims"), 2),
        )
    )

    # --- dedup: keep the highest-claim record per (npi, generic_name) ---
    w = Window.partitionBy("npi", "generic_name").orderBy(F.desc("total_claims"))
    deduped = (
        cleaned
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    (
        deduped
        .repartition("state")
        .write.mode("overwrite")
        .partitionBy("state")
        .parquet(output_path)
    )

    print(f"Wrote {deduped.count():,} rows to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = build_spark()
    transform(spark, args.input, args.output)
    spark.stop()
