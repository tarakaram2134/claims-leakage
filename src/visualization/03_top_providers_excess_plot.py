import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


def main() -> None:
    db_url = os.environ["CLAIMS_DB_URL"]
    engine = create_engine(db_url)

    query = """
    WITH recent AS (
      SELECT *
      FROM hc.provider_outlier_scores_eb
      WHERE service_month >= (date_trunc('month', CURRENT_DATE) - INTERVAL '6 months')
    ),
    provider_rollup AS (
      SELECT
        provider_id,
        specialty,
        COUNT(*) FILTER (WHERE eb_percent_rank >= 0.95) AS eb_high_months,
        COUNT(*) AS months_observed,
        SUM(claim_count) AS claims_6m,
        AVG(eb_residual_apc) AS avg_eb_residual_apc,
        SUM(eb_residual_apc * claim_count) AS est_excess_allowed_6m,
        AVG(w) AS avg_weight
      FROM recent
      GROUP BY 1,2
    )
    SELECT
      provider_id,
      specialty,
      claims_6m,
      eb_high_months,
      months_observed,
      avg_eb_residual_apc,
      est_excess_allowed_6m,
      avg_weight
    FROM provider_rollup
    WHERE months_observed >= 4
      AND claims_6m >= 150
      AND eb_high_months >= 2
    ORDER BY est_excess_allowed_6m DESC
    LIMIT 10;
    """

    df = pd.read_sql(text(query), engine)
    df["label"] = df["provider_id"].astype(str) + " (" + df["specialty"].astype(str) + ")"

    # Reverse for nicer horizontal bar ordering (top at top)
    df = df.sort_values("est_excess_allowed_6m", ascending=True)

    fig = plt.figure()
    plt.barh(df["label"], df["est_excess_allowed_6m"])
    plt.title("Top 10 Providers by Estimated 6M Excess Allowed (EB)")
    plt.xlabel("Estimated Excess Allowed (6 months)")
    plt.tight_layout()

    out_path = "reports/figures/03_top10_provider_excess_allowed.png"
    fig.savefig(out_path, dpi=200)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
