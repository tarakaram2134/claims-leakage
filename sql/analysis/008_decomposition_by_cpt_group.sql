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
      ELSE NULL
    END AS yr,
    cpt_group,
    COUNT(*) AS claims,
    AVG(allowed_amount) AS apc
  FROM claims
  GROUP BY 1,2
),
totals AS (
  SELECT yr, SUM(claims) AS total_claims
  FROM year_tag
  WHERE yr IS NOT NULL
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
  WHERE y.yr IS NOT NULL
),
base AS (SELECT * FROM mix WHERE yr='baseline'),
curr AS (SELECT * FROM mix WHERE yr='current'),
joined AS (
  SELECT
    COALESCE(b.cpt_group, c.cpt_group) AS cpt_group,
    COALESCE(b.share,0) AS share_base,
    COALESCE(c.share,0) AS share_curr,
    COALESCE(b.apc,0) AS apc_base,
    COALESCE(c.apc,0) AS apc_curr,
    (SELECT total_claims FROM totals WHERE yr='current') AS claims_curr
  FROM base b
  FULL JOIN curr c
    ON c.cpt_group = b.cpt_group
)
SELECT
  cpt_group,
  ROUND(((share_curr - share_base) * apc_base * claims_curr)::numeric, 2) AS case_mix_effect,
  ROUND((share_curr * (apc_curr - apc_base) * claims_curr)::numeric, 2) AS price_effect
FROM joined
ORDER BY (ABS((share_curr - share_base) * apc_base * claims_curr) + ABS(share_curr * (apc_curr - apc_base) * claims_curr)) DESC
LIMIT 12;
