DROP TABLE IF EXISTS hc.peer_benchmark_month;

CREATE TABLE hc.peer_benchmark_month AS
SELECT
  service_month,
  specialty,
  place_of_service,
  COUNT(*) AS provider_rows,
  SUM(claim_count) AS claims_in_peer,
  (SUM(allowed_total) / NULLIF(SUM(claim_count),0)) AS peer_allowed_per_claim,
  AVG(avg_risk) AS peer_avg_risk,
  STDDEV_POP(allowed_per_claim) AS peer_apc_sd
FROM hc.provider_month_agg
GROUP BY 1,2,3;
