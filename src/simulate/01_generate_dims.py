import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.environ["CLAIMS_DB_URL"]
engine = create_engine(DB_URL)

rng = np.random.default_rng(42)

def main():
    # -----------------------
    # CPT dim
    # -----------------------
    cpt_groups = [
        "Imaging", "Labs", "PT_OT", "Minor_Surgery", "Injections",
        "Endoscopy", "Cardio_Diagnostics", "Derm_Procedures",
        "Ortho_Procedures", "Pain_Management", "Urgent_Care", "Other"
    ]

    n_cpt = 220
    cpt_codes = [str(rng.integers(10000, 99999)).zfill(5) for _ in range(n_cpt)]
    cpt_group = rng.choice(cpt_groups, size=n_cpt, replace=True)

    base_weight = np.exp(rng.normal(loc=0.0, scale=0.7, size=n_cpt))
    cpt_dim = pd.DataFrame({
        "cpt_code": cpt_codes,
        "cpt_group": cpt_group,
        "baseline_allowed_weight": np.round(base_weight, 4),
        "description": [f"{g} procedure" for g in cpt_group]
    }).drop_duplicates(subset=["cpt_code"])

    # -----------------------
    # Geo demo dim
    # -----------------------
    n_counties = 120
    county_fips = [str(26000 + i) for i in range(n_counties)]
    median_income = rng.integers(38000, 125000, size=n_counties)
    pct_over_65 = np.round(rng.uniform(0.08, 0.28, size=n_counties), 3)
    urbanicity = np.round(rng.uniform(0.05, 0.95, size=n_counties), 3)

    geo_demo = pd.DataFrame({
        "county_fips": county_fips,
        "median_income": median_income,
        "pct_over_65": pct_over_65,
        "urbanicity_index": urbanicity
    })

    # -----------------------
    # Providers dim
    # -----------------------
    n_providers = 2000
    specialties = [
        "Radiology", "Orthopedics", "Dermatology", "Gastroenterology",
        "Cardiology", "Physical Therapy", "Urgent Care", "General Surgery",
        "Endocrinology", "Neurology", "Ophthalmology"
    ]
    contract_types = ["PPO", "HMO", "FFS"]

    provider_ids = np.arange(100000, 100000 + n_providers)
    provider_specialty = rng.choice(specialties, size=n_providers, replace=True)

    missing_mask = rng.random(n_providers) < 0.06
    provider_specialty = provider_specialty.astype(object)
    provider_specialty[missing_mask] = None

    providers = pd.DataFrame({
        "provider_id": provider_ids,
        "specialty": provider_specialty,
        "org_flag": rng.random(n_providers) < 0.35,
        "contract_type": rng.choice(contract_types, size=n_providers, replace=True, p=[0.55, 0.25, 0.20]),
        "address_state": rng.choice(["MI", "OH", "IL", "IN"], size=n_providers, replace=True, p=[0.6, 0.15, 0.15, 0.10]),
        "county_fips": rng.choice(county_fips, size=n_providers, replace=True),
        "provider_zip": [str(rng.integers(48000, 49999)).zfill(5) for _ in range(n_providers)]
    })

    # -----------------------
    # Members dim
    # -----------------------
    n_members = 200_000
    member_ids = np.arange(1, n_members + 1, dtype=np.int64)

    risk = np.clip(rng.lognormal(mean=-0.1, sigma=0.6, size=n_members), 0.2, 6.0)
    member_zip = np.array([str(rng.integers(48000, 49999)).zfill(5) for _ in range(n_members)])

    members = pd.DataFrame({
        "member_id": member_ids,
        "member_zip": member_zip,
        "member_risk_score": np.round(risk, 4),
        "risk_score_version": "v1"
    })

    with engine.begin() as conn:
        cpt_dim.to_sql("cpt_dim", conn, schema="hc", if_exists="append", index=False, method="multi", chunksize=5000)
        geo_demo.to_sql("geo_demo_dim", conn, schema="hc", if_exists="append", index=False, method="multi", chunksize=5000)
        providers.to_sql("providers_dim", conn, schema="hc", if_exists="append", index=False, method="multi", chunksize=5000)
        members.to_sql("members_dim", conn, schema="hc", if_exists="append", index=False, method="multi", chunksize=10_000)

    print("Loaded dims OK.")

if __name__ == "__main__":
    main()
