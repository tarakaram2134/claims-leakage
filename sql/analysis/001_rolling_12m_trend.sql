WITH monthly AS (
  SELECT
    date_trunc('month', service_date)::date AS month,
    COUNT(*) AS claim_count,
    SUM(allowed_amount) AS allowed_total,
    AVG(allowed_amount) AS allowed_per_claim
  FROM hc.claims_fact
  WHERE claim_status = 'final'
  GROUP BY 1
),
rolled AS (
  SELECT
    month,
    claim_count,
    allowed_total,
    allowed_per_claim,
    SUM(claim_count) OVER (
      ORDER BY month
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS claims_12m,
    SUM(allowed_total) OVER (
      ORDER BY month
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS allowed_12m,
    (SUM(allowed_total) OVER (
       ORDER BY month
       ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
     ) / NULLIF(SUM(claim_count) OVER (
       ORDER BY month
       ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
     ), 0)
    ) AS allowed_per_claim_12m
  FROM monthly
)
SELECT *
FROM rolled
ORDER BY month;
