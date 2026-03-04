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
  ROUND(avg_eb_residual_apc::numeric, 2) AS avg_eb_residual_apc,
  ROUND(est_excess_allowed_6m::numeric, 2) AS est_excess_allowed_6m,
  ROUND(avg_weight::numeric, 3) AS avg_credibility_weight
FROM provider_rollup
WHERE months_observed >= 4
  AND claims_6m >= 150
  AND eb_high_months >= 2
ORDER BY est_excess_allowed_6m DESC
LIMIT 25;
