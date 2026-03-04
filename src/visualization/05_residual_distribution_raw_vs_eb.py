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
    SELECT
      residual_apc,
      eb_residual_apc
    FROM hc.provider_outlier_scores_eb
    WHERE residual_apc IS NOT NULL
      AND eb_residual_apc IS NOT NULL;
    """

    df = pd.read_sql(text(query), engine)

    # Heavy tails exist; cap for visualization so the histogram is readable
    raw = df["residual_apc"].clip(lower=-2000, upper=2000)
    eb = df["eb_residual_apc"].clip(lower=-2000, upper=2000)

    fig = plt.figure()
    plt.hist(raw, bins=80, alpha=0.5, label="Raw residual (capped ±2000)")
    plt.hist(eb, bins=80, alpha=0.5, label="EB residual (capped ±2000)")

    plt.title("Provider Residual Allowed/Claim: Raw vs Empirical Bayes (Shrinkage)")
    plt.xlabel("Residual Allowed per Claim")
    plt.ylabel("Count (provider-months)")
    plt.tight_layout()
    plt.legend()

    out_path = "reports/figures/05_residual_distribution_raw_vs_eb.png"
    fig.savefig(out_path, dpi=200)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
