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
    WITH claims AS (
      SELECT
        service_month,
        COALESCE(cpt_group, 'UNKNOWN') AS cpt_group,
        allowed_amount
      FROM hc.claims_enriched_v
      WHERE claim_status = 'final'
        AND service_month >= DATE '2024-03-01'
        AND service_month <  DATE '2026-03-01'
    ),
    year_tag AS (
      SELECT
        CASE
          WHEN service_month >= DATE '2024-03-01' AND service_month < DATE '2025-03-01' THEN 'baseline'
          WHEN service_month >= DATE '2025-03-01' AND service_month < DATE '2026-03-01' THEN 'current'
        END AS yr,
        cpt_group,
        COUNT(*) AS claims,
        AVG(allowed_amount) AS apc,
        SUM(allowed_amount) AS allowed
      FROM claims
      GROUP BY 1,2
    ),
    totals AS (
      SELECT
        yr,
        SUM(claims) AS total_claims,
        SUM(allowed) AS total_allowed,
        (SUM(allowed) / NULLIF(SUM(claims),0)) AS overall_apc
      FROM year_tag
      GROUP BY 1
    ),
    mix AS (
      SELECT
        y.yr,
        y.cpt_group,
        y.claims,
        y.apc,
        (y.claims::numeric / NULLIF(t.total_claims,0)) AS share
      FROM year_tag y
      JOIN totals t USING (yr)
    ),
    base AS (SELECT * FROM mix WHERE yr='baseline'),
    curr AS (SELECT * FROM mix WHERE yr='current'),
    joined AS (
      SELECT
        COALESCE(b.cpt_group, c.cpt_group) AS cpt_group,
        COALESCE(b.share,0) AS share_base,
        COALESCE(c.share,0) AS share_curr,
        COALESCE(b.apc,0) AS apc_base,
        COALESCE(c.apc,0) AS apc_curr
      FROM base b
      FULL JOIN curr c
        ON c.cpt_group = b.cpt_group
    ),
    effects AS (
      SELECT
        SUM((share_curr - share_base) * apc_base) AS mix_term,
        SUM(share_curr * (apc_curr - apc_base)) AS price_term
      FROM joined
    ),
    t AS (
      SELECT
        (SELECT total_claims FROM totals WHERE yr='baseline') AS claims_base,
        (SELECT total_claims FROM totals WHERE yr='current')  AS claims_curr,
        (SELECT total_allowed FROM totals WHERE yr='baseline') AS allowed_base,
        (SELECT total_allowed FROM totals WHERE yr='current')  AS allowed_curr,
        (SELECT overall_apc FROM totals WHERE yr='baseline') AS overall_apc_base
    )
    SELECT
      (allowed_curr - allowed_base) AS total_allowed_delta,
      (claims_curr - claims_base) * overall_apc_base AS volume_effect,
      claims_curr * mix_term AS case_mix_effect,
      claims_curr * price_term AS price_effect
    FROM t, effects;
    """

    row = pd.read_sql(text(query), engine).iloc[0]

    labels = ["Volume", "Case-mix", "Price"]
    values = [row["volume_effect"], row["case_mix_effect"], row["price_effect"]]

    fig = plt.figure()
    plt.bar(labels, values)
    plt.title("YoY Allowed Cost Decomposition (Mar 2025–Feb 2026 vs Prior Year)")
    plt.ylabel("Allowed Dollars (Delta)")
    plt.tight_layout()

    out_path = "reports/figures/02_cost_decomposition.png"
    fig.savefig(out_path, dpi=200)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
