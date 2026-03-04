CREATE OR REPLACE VIEW hc.claims_clean_v AS
SELECT
  claim_id,
  member_id,
  provider_id,
  NULLIF(LEFT(TRIM(cpt_code), 5), '') AS cpt_code_clean,
  diagnosis_code,
  place_of_service,
  service_date,
  paid_date,
  units,
  billed_amount,
  allowed_amount,
  paid_amount,
  in_network_flag,
  member_zip,
  claim_status,
  created_at
FROM hc.claims_fact;
