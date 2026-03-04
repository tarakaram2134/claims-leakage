import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.environ["CLAIMS_DB_URL"]
engine = create_engine(DB_URL)

OUT_TABLE = "hc.provider_outlier_scores_eb"

def main():
    query = """
    SELECT
      provider_id,
      service_month,
      specialty,
      place_of_service,
      county_fips,
      claim_count,
      residual_apc,
      z_residual,
      residual_percentile
    FROM hc.provider_outlier_scores
    WHERE residual_apc IS NOT NULL
      AND claim_count IS NOT NULL
      AND claim_count > 0;
    """
    df = pd.read_sql(text(query), engine)

    # Peer group: month + specialty + POS (the same grouping used for residual percentile)
    peer_cols = ["service_month", "specialty", "place_of_service"]

    # Estimate peer-level residual variance and typical volume
    peer_stats = (
        df.groupby(peer_cols)
          .agg(
              peer_resid_mean=("residual_apc", "mean"),
              peer_resid_var=("residual_apc", "var"),
              peer_n_providers=("provider_id", "nunique"),
              peer_claims_median=("claim_count", "median"),
          )
          .reset_index()
    )

    # Fill edge cases: variance can be NaN if too few providers in group
    peer_stats["peer_resid_var"] = peer_stats["peer_resid_var"].fillna(0.0)

    merged = df.merge(peer_stats, on=peer_cols, how="left")

    # EB shrinkage target is 0 residual (peer mean baseline already subtracted),
    # but we still use peer variance to set shrink strength.
    #
    # k is a pseudo-count controlling shrinkage. Higher peer variance -> less shrink (k smaller).
    # Lower peer variance -> more shrink (k larger).
    #
    # Simple, transparent choice:
    # k = (peer_claims_median) * (peer_resid_var / (peer_resid_var + c))
    # with c stabilizing small variances.
    c = np.nanpercentile(merged["peer_resid_var"].to_numpy(), 50) + 1e-6  # median variance
    merged["k"] = merged["peer_claims_median"] * (merged["peer_resid_var"] / (merged["peer_resid_var"] + c))
    merged["k"] = merged["k"].clip(lower=10, upper=500)  # guardrails

    # Credibility weight
    merged["w"] = merged["claim_count"] / (merged["claim_count"] + merged["k"])
    merged["eb_residual_apc"] = merged["w"] * merged["residual_apc"]

    # Re-rank within peer groups using EB residual
    merged["eb_percent_rank"] = (
        merged.groupby(peer_cols)["eb_residual_apc"]
              .rank(pct=True, method="average")
    )

    out = merged[[
        "provider_id", "service_month", "specialty", "place_of_service", "county_fips",
        "claim_count", "residual_apc", "eb_residual_apc", "w",
        "residual_percentile", "eb_percent_rank"
    ]].copy()

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_TABLE};"))
        out.to_sql("provider_outlier_scores_eb", conn, schema="hc", index=False, if_exists="replace", method="multi", chunksize=5000)

    print(f"Wrote {len(out):,} rows to {OUT_TABLE}")

if __name__ == "__main__":
    main()
