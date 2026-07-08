"""
Seed script — generates a realistic sample of the CMS Part D prescriber
dataset so you can develop and demo the full pipeline offline (no API needed).

Produces:
  data/raw/ingest_date=<today>/part_00000.json   (bronze, same shape as CMS API)
  data/openclaims.duckdb                         (raw.part_d_prescribers loaded)

Run:  python scripts/seed_sample_data.py
Then: make dbt && make test && make dashboard
"""

import json
import os
import random
from datetime import date

import duckdb

random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
TODAY = date.today().isoformat()
RAW_DIR = os.path.join(DATA_DIR, "raw", f"ingest_date={TODAY}")
DB_PATH = os.path.join(DATA_DIR, "openclaims.duckdb")

STATES = ["FL", "GA", "TX", "CA", "NY", "OH", "PA", "IL", "NC", "MI"]
TYPES = ["Internal Medicine", "Family Practice", "Cardiology",
         "Psychiatry", "Nurse Practitioner", "Endocrinology"]
DRUGS = [
    ("Eliquis", "Apixaban", 550.0),
    ("Jardiance", "Empagliflozin", 610.0),
    ("Ozempic", "Semaglutide", 935.0),
    ("Lipitor", "Atorvastatin Calcium", 12.0),
    ("Metformin HCl", "Metformin Hydrochloride", 8.0),
    ("Lisinopril", "Lisinopril", 6.0),
    ("Xarelto", "Rivaroxaban", 540.0),
    ("Amlodipine Besylate", "Amlodipine Besylate", 7.0),
    ("Trulicity", "Dulaglutide", 890.0),
    ("Levothyroxine Sodium", "Levothyroxine Sodium", 10.0),
]
LAST = ["Smith", "Johnson", "Patel", "Garcia", "Nguyen", "Brown",
        "Krishnan", "Lee", "Martinez", "Wilson"]
FIRST = ["James", "Maria", "Ravi", "Linda", "Anh", "Robert",
         "Priya", "David", "Sofia", "Karen"]


def make_rows(n_prescribers: int = 800) -> list[dict]:
    rows = []
    for i in range(n_prescribers):
        npi = str(1000000000 + i)
        state = random.choice(STATES)
        ptype = random.choice(TYPES)
        last, first = random.choice(LAST), random.choice(FIRST)

        for brand, generic, base_cost in random.sample(DRUGS, k=random.randint(2, 6)):
            claims = random.randint(11, 900)
            # cost varies around the drug's base; a few outliers sneak in
            multiplier = random.choice([1, 1, 1, 1, random.uniform(3.5, 6)])
            cost = round(claims * base_cost * random.uniform(0.8, 1.2) * multiplier, 2)
            rows.append({
                "Prscrbr_NPI": npi,
                "Prscrbr_Last_Org_Name": last.upper(),
                "Prscrbr_First_Name": first,
                "Prscrbr_State_Abrvtn": state,
                "Prscrbr_Type": ptype,
                "Brnd_Name": brand,
                "Gnrc_Name": generic,
                "Tot_Clms": str(claims),                      # CMS API returns strings
                "Tot_Day_Suply": str(claims * 30),
                "Tot_Drug_Cst": str(cost),
            })
    return rows


def main() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    rows = make_rows()

    out_path = os.path.join(RAW_DIR, "part_00000.json")
    with open(out_path, "w") as f:
        json.dump(rows, f)
    print(f"Wrote {len(rows):,} sample rows -> {out_path}")

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.part_d_prescribers AS
        SELECT *, '{TODAY}'::DATE AS _ingest_date
        FROM read_json_auto('{out_path}');
        """
    )
    n = con.execute("SELECT COUNT(*) FROM raw.part_d_prescribers").fetchone()[0]
    con.close()
    print(f"Loaded {n:,} rows into raw.part_d_prescribers ({DB_PATH})")


if __name__ == "__main__":
    main()
