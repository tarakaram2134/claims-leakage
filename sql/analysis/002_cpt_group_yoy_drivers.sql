WITH m AS (
  SELECT
    service_month,
    cpt_group,
    COUNT(*) AS claim_count,
    SUM(allowed_amount) AS allowed_total,
    AVG(allowed_amount) AS allowed_per_claim
  FROM hc.claims_enriched_v
  WHERE claim_status = 'final'
  GROUP BY 1, 2
),
yoy AS (
  SELECT
    a.service_month,
    a.cpt_group,
    a.allowed_total AS allowed_total_curr,
    b.allowed_total AS allowed_total_prev,
    a.claim_count AS claims_curr,
    b.claim_count AS claims_prev,
    a.allowed_per_claim AS apc_curr,
    b.allowed_per_claim AS apc_prev,
    (a.allowed_total - b.allowed_total) AS allowed_delta,
    (a.claim_count - b.claim_count) AS claims_delta,
    (a.allowed_per_claim - b.allowed_per_claim) AS apc_delta
  FROM m a
  LEFT JOIN m b
    ON b.cpt_group = a.cpt_group
   AND b.service_month = (a.service_month - INTERVAL '12 months')
)
SELECT *
FROM yoy
WHERE service_month >= (date_trunc('month', CURRENT_DATE) - INTERVAL '12 months')
ORDER BY allowed_delta DESC NULLS LAST;
