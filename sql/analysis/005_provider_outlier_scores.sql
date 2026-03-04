DROP TABLE IF EXISTS hc.provider_outlier_scores;

CREATE TABLE hc.provider_outlier_scores AS
SELECT
  p.provider_id,
  p.service_month,
  p.specialty,
  p.place_of_service,
  p.county_fips,
  p.claim_count,
  p.allowed_per_claim,
  p.avg_risk,
  b.peer_allowed_per_claim,
  b.peer_apc_sd,
  (p.allowed_per_claim - b.peer_allowed_per_claim) AS residual_apc,
  CASE
    WHEN b.peer_apc_sd IS NULL OR b.peer_apc_sd = 0 THEN NULL
    ELSE (p.allowed_per_claim - b.peer_allowed_per_claim) / b.peer_apc_sd
  END AS z_residual,
  percent_rank() OVER (
    PARTITION BY p.service_month, p.specialty, p.place_of_service
    ORDER BY (p.allowed_per_claim - b.peer_allowed_per_claim)
  ) AS residual_percentile
FROM hc.provider_month_agg p
LEFT JOIN hc.peer_benchmark_month b
  ON b.service_month = p.service_month
 AND b.specialty = p.specialty
 AND b.place_of_service = p.place_of_service;
