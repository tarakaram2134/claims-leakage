import os
import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.environ["CLAIMS_DB_URL"]
engine = create_engine(DB_URL)

rng = np.random.default_rng(7)

N_CLAIMS = 1_200_000
START_DATE = np.datetime64("2024-03-01")  # 24 months ending ~2026-02
END_DATE = np.datetime64("2026-03-01")

OUT_CSV = "data/staged/claims_fact.csv"

PLACE_OF_SERVICE = ["OP_HOSP", "ASC", "CLINIC", "URGENT_CARE"]
POS_P = [0.35, 0.18, 0.37, 0.10]

def _sample_service_dates(n: int) -> np.ndarray:
    days = (END_DATE - START_DATE).astype(int)

    # Seasonality: slightly higher volume in late fall/winter
    day_idx = rng.integers(0, days, size=n)
    dates = START_DATE + day_idx.astype("timedelta64[D]")

    # Soft seasonality bump via rejection sampling (cheap and good enough)
    months = (dates.astype("datetime64[M]") - START_DATE.astype("datetime64[M]")).astype(int) % 12
    seasonal = np.where(np.isin(months, [10, 11, 0, 1]), 1.12, 1.0)  # Nov-Feb bump
    keep = rng.random(n) < np.clip(seasonal / 1.12, 0, 1)

    # If we dropped some, resample them
    missing = np.where(~keep)[0]
    if len(missing) > 0:
        resamp = _sample_service_dates(len(missing))
        dates[missing] = resamp
    return dates

def _paid_lag_days(n: int) -> np.ndarray:
    # Right-skewed lag: most within 7-30 days, long tail up to ~120+
    base = rng.gamma(shape=2.2, scale=8.5, size=n)  # mean ~18.7
    tail = rng.random(n) < 0.03
    base[tail] += rng.gamma(shape=2.0, scale=25.0, size=tail.sum())
    return np.clip(base, 0, 180).astype(int)

def _make_allowed_amount(base_price: np.ndarray, provider_factor: np.ndarray, risk: np.ndarray, geo_factor: np.ndarray) -> np.ndarray:
    # heavy tail multiplicative noise
    noise = rng.lognormal(mean=0.0, sigma=0.55, size=len(base_price))
    allowed = base_price * provider_factor * (0.75 + 0.35 * risk) * geo_factor * noise
    return np.round(allowed, 2)

def main():
    os.makedirs("data/staged", exist_ok=True)

    with engine.connect() as conn:
        providers = pd.read_sql(text("SELECT provider_id, county_fips, contract_type FROM hc.providers_dim"), conn)
        cpt = pd.read_sql(text("SELECT cpt_code, baseline_allowed_weight FROM hc.cpt_dim"), conn)
        members = pd.read_sql(text("SELECT member_id, member_zip, member_risk_score FROM hc.members_dim"), conn)

        geo = pd.read_sql(text("SELECT county_fips, median_income, pct_over_65, urbanicity_index FROM hc.geo_demo_dim"), conn)

    # Map provider -> geo factor (urbanicity + income proxy)
    prov_geo = providers.merge(geo, on="county_fips", how="left")
    prov_geo["geo_factor"] = (
        0.92
        + 0.25 * prov_geo["urbanicity_index"].fillna(0.5)
        + 0.10 * (prov_geo["median_income"].fillna(65000) / 100000.0)
    )
    prov_geo["geo_factor"] = prov_geo["geo_factor"].clip(0.85, 1.35)

    provider_ids = prov_geo["provider_id"].to_numpy()
    provider_geo_factor = prov_geo["geo_factor"].to_numpy()

    # Provider base price factor: most near 1.0, some high
    base_provider_factor = rng.lognormal(mean=0.0, sigma=0.18, size=len(provider_ids))
    base_provider_factor = np.clip(base_provider_factor, 0.75, 1.55)

    # Select “bad actors” (about 2.5%)
    n_bad = max(20, int(0.025 * len(provider_ids)))
    bad_provider_set = set(rng.choice(provider_ids, size=n_bad, replace=False).tolist())

    # CPT base price scaling
    cpt_codes = cpt["cpt_code"].to_numpy()
    cpt_weight = cpt["baseline_allowed_weight"].to_numpy()
    # Convert weights into dollar-ish baseline
    # This gives realistic spread: a lot of ~$80-$400, some $1k+
    base_cpt_price = 75 + 260 * cpt_weight

    # Member arrays
    member_ids = members["member_id"].to_numpy()
    member_zip = members["member_zip"].to_numpy()
    member_risk = members["member_risk_score"].to_numpy()

    # Precompute provider id -> index for vector lookup
    prov_index = {pid: i for i, pid in enumerate(provider_ids)}
    cpt_index = {code: i for i, code in enumerate(cpt_codes)}

    # We write in chunks to avoid RAM pain
    chunk_size = 200_000
    n_chunks = math.ceil(N_CLAIMS / chunk_size)

    wrote_header = False

    for chunk_i in range(n_chunks):
        n = min(chunk_size, N_CLAIMS - chunk_i * chunk_size)

        # Core IDs
        prov = rng.choice(provider_ids, size=n, replace=True)
        mem_idx = rng.integers(0, len(member_ids), size=n)
        mem = member_ids[mem_idx]
        mem_zip = member_zip[mem_idx]
        risk = member_risk[mem_idx]

        # Dates
        svc_date = _sample_service_dates(n)
        lag = _paid_lag_days(n)
        paid_date = svc_date + lag.astype("timedelta64[D]")

        # Year-2 flag (last 12 months of the 24-month window)
        year2 = svc_date >= np.datetime64("2025-03-01")

        # CPT selection
        # Bad providers shift toward higher weight CPTs in year 2 (coding intensity drift proxy)
        cpt_idx = rng.integers(0, len(cpt_codes), size=n)

        bad_mask = np.array([p in bad_provider_set for p in prov])
        drift_mask = bad_mask & year2
        if drift_mask.any():
            # Re-sample CPTs for drift rows with probability proportional to weight^1.6
            weights = cpt_weight ** 1.6
            weights = weights / weights.sum()
            cpt_idx[drift_mask] = rng.choice(np.arange(len(cpt_codes)), size=drift_mask.sum(), p=weights)

        cpt_code = cpt_codes[cpt_idx]
        base_price = base_cpt_price[cpt_idx]

        # Provider factor + drift (price inflation / contract leakage proxy)
        prov_factor = np.array([base_provider_factor[prov_index[p]] for p in prov])

        # Bad providers also have price drift up in year2
        prov_factor = prov_factor * np.where(drift_mask, rng.normal(loc=1.12, scale=0.03, size=n), 1.0)
        prov_factor = np.clip(prov_factor, 0.70, 1.90)

        # Geo factor
        geo_factor = np.array([provider_geo_factor[prov_index[p]] for p in prov])

        # Allowed/Paid/Billed
        allowed = _make_allowed_amount(base_price, prov_factor, risk, geo_factor)

        # billed is typically higher than allowed; bad providers have larger gaps
        billed_multiplier = rng.normal(loc=1.55, scale=0.18, size=n)
        billed_multiplier = np.clip(billed_multiplier, 1.05, 3.5)
        billed_multiplier = billed_multiplier * np.where(bad_mask, 1.12, 1.0)
        billed = np.round(allowed * billed_multiplier, 2)

        # paid is allowed minus member cost share; allow some variation by contract type (proxy)
        cost_share = rng.normal(loc=0.14, scale=0.05, size=n)
        cost_share = np.clip(cost_share, 0.02, 0.35)
        paid = np.round(allowed * (1.0 - cost_share), 2)

        # Units
        units = rng.integers(1, 4, size=n)

        # POS
        pos = rng.choice(PLACE_OF_SERVICE, size=n, p=POS_P)

        # DX codes (simple realistic ICD-10-like)
        dx = np.array([f"{chr(65 + rng.integers(0, 26))}{rng.integers(0, 99):02d}" for _ in range(n)])

        # In-network flag: mostly in-network; slightly higher OON for some providers
        in_net = rng.random(n) > np.where(bad_mask, 0.10, 0.04)

        # Claim status
        status_roll = rng.random(n)
        status = np.where(status_roll < 0.965, "final", np.where(status_roll < 0.99, "adjusted", "reversed"))

        # Inject CPT inconsistencies (about 3%)
        cpt_dirty = cpt_code.astype(object).copy()
        dirty_roll = rng.random(n)
        # 1% invalid
        invalid = dirty_roll < 0.010
        cpt_dirty[invalid] = "9" + str(rng.integers(1000, 9999))  # wrong length-ish
        # 1.5% whitespace / formatting
        fmt = (dirty_roll >= 0.010) & (dirty_roll < 0.025)
        cpt_dirty[fmt] = [" " + x + " " for x in cpt_dirty[fmt]]

        out = pd.DataFrame({
            "member_id": mem,
            "provider_id": prov,
            "cpt_code": cpt_dirty,
            "diagnosis_code": dx,
            "place_of_service": pos,
            "service_date": pd.to_datetime(svc_date).date,
            "paid_date": pd.to_datetime(paid_date).date,
            "units": units,
            "billed_amount": billed,
            "allowed_amount": allowed,
            "paid_amount": paid,
            "in_network_flag": in_net,
            "member_zip": mem_zip,
            "claim_status": status
        })

        out.to_csv(OUT_CSV, mode="a", index=False, header=not wrote_header)
        wrote_header = True
        print(f"Wrote chunk {chunk_i + 1}/{n_chunks}: {len(out):,} rows")

    # Bulk load via COPY
    copy_sql = f"""
    \\copy hc.claims_fact
    (member_id, provider_id, cpt_code, diagnosis_code, place_of_service,
     service_date, paid_date, units, billed_amount, allowed_amount, paid_amount,
     in_network_flag, member_zip, claim_status)
    FROM '{os.path.abspath(OUT_CSV)}'
    WITH (FORMAT csv, HEADER true);
    """

    print("Starting COPY load...")
    os.system(f"psql -U tarak -d claims_leakage -c \"{copy_sql}\"")

    print("Done. Verify row count:")
    os.system("psql -U tarak -d claims_leakage -c \"SELECT COUNT(*) FROM hc.claims_fact;\"")

if __name__ == "__main__":
    main()
