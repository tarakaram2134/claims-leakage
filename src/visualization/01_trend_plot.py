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
    WITH monthly AS (
      SELECT
        date_trunc('month', service_date)::date AS month,
        COUNT(*) AS claim_count,
        SUM(allowed_amount) AS allowed_total,
        (SUM(allowed_amount) / NULLIF(COUNT(*),0)) AS allowed_per_claim
      FROM hc.claims_fact
      WHERE claim_status = 'final'
      GROUP BY 1
    )
    SELECT
      month,
      claim_count,
      allowed_total,
      allowed_per_claim,
      SUM(claim_count) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS claims_12m,
      SUM(allowed_total) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS allowed_12m,
      (SUM(allowed_total) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
        / NULLIF(SUM(claim_count) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),0)
      ) AS allowed_per_claim_12m
    FROM monthly
    ORDER BY month;
    """

    df = pd.read_sql(text(query), engine)
    df["month"] = pd.to_datetime(df["month"])

    fig = plt.figure()
    plt.plot(df["month"], df["allowed_per_claim_12m"])
    plt.title("Rolling 12-Month Allowed Per Claim (Outpatient)")
    plt.xlabel("Month")
    plt.ylabel("Allowed per Claim (12M rolling)")
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_path = "reports/figures/01_rolling_12m_allowed_per_claim.png"
    fig.savefig(out_path, dpi=200)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
