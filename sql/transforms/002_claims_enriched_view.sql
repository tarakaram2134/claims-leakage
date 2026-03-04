CREATE OR REPLACE VIEW hc.claims_enriched_v AS
SELECT
  cf.claim_id,
  cf.member_id,
  cf.provider_id,
  NULLIF(LEFT(TRIM(cf.cpt_code), 5), '') AS cpt_code_clean,
  cd.cpt_group,
  cf.diagnosis_code,
  cf.place_of_service,
  cf.service_date,
  date_trunc('month', cf.service_date)::date AS service_month,
  cf.paid_date,
  (cf.paid_date - cf.service_date) AS lag_days,
  cf.units,
  cf.billed_amount,
  cf.allowed_amount,
  cf.paid_amount,
  cf.in_network_flag,
  cf.member_zip,
  md.member_risk_score,
  pd.specialty,
  pd.contract_type,
  pd.county_fips AS provider_county_fips,
  gd.median_income,
  gd.pct_over_65,
  gd.urbanicity_index,
  cf.claim_status
FROM hc.claims_fact cf
LEFT JOIN hc.cpt_dim cd
  ON cd.cpt_code = NULLIF(LEFT(TRIM(cf.cpt_code), 5), '')
LEFT JOIN hc.members_dim md
  ON md.member_id = cf.member_id
LEFT JOIN hc.providers_dim pd
  ON pd.provider_id = cf.provider_id
LEFT JOIN hc.geo_demo_dim gd
  ON gd.county_fips = pd.county_fips;
