DROP TABLE IF EXISTS hc.provider_month_agg;

CREATE TABLE hc.provider_month_agg AS
WITH base AS (
  SELECT
    provider_id,
    service_month,
    COALESCE(specialty, 'UNKNOWN') AS specialty,
    COALESCE(contract_type, 'UNKNOWN') AS contract_type,
    COALESCE(cpt_group, 'UNKNOWN') AS cpt_group,
    COALESCE(place_of_service, 'UNKNOWN') AS place_of_service,
    COALESCE(provider_county_fips, '00000') AS county_fips,
    COUNT(*) AS claim_count,
    SUM(allowed_amount) AS allowed_total,
    AVG(allowed_amount) AS allowed_per_claim,
    AVG(member_risk_score) AS avg_risk
  FROM hc.claims_enriched_v
  WHERE claim_status = 'final'
    AND cpt_code_clean IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7
),
provider_totals AS (
  SELECT
    provider_id,
    service_month,
    SUM(claim_count) AS provider_claims
  FROM base
  GROUP BY 1,2
),
mix AS (
  SELECT
    b.*,
    (b.claim_count::numeric / NULLIF(pt.provider_claims,0)) AS cpt_mix_share
  FROM base b
  JOIN provider_totals pt
    ON pt.provider_id = b.provider_id
   AND pt.service_month = b.service_month
)
SELECT
  provider_id,
  service_month,
  specialty,
  contract_type,
  place_of_service,
  county_fips,
  SUM(claim_count) AS claim_count,
  SUM(allowed_total) AS allowed_total,
  (SUM(allowed_total) / NULLIF(SUM(claim_count),0)) AS allowed_per_claim,
  (SUM(avg_risk * claim_count) / NULLIF(SUM(claim_count),0)) AS avg_risk,
  SUM(-cpt_mix_share * LN(NULLIF(cpt_mix_share,0))) AS mix_entropy
FROM mix
GROUP BY 1,2,3,4,5,6;
