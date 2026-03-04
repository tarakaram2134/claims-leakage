-- Time filters + rolling windows
CREATE INDEX IF NOT EXISTS idx_claims_service_date
  ON hc.claims_fact (service_date);

-- Provider benchmarking
CREATE INDEX IF NOT EXISTS idx_claims_provider_date
  ON hc.claims_fact (provider_id, service_date);

-- Member utilization / PMPM support
CREATE INDEX IF NOT EXISTS idx_claims_member_date
  ON hc.claims_fact (member_id, service_date);

-- CPT driver analysis
CREATE INDEX IF NOT EXISTS idx_claims_cpt_date
  ON hc.claims_fact (cpt_code, service_date);

-- Geo analysis
CREATE INDEX IF NOT EXISTS idx_claims_member_zip
  ON hc.claims_fact (member_zip);

-- Paid lag / completion analysis
CREATE INDEX IF NOT EXISTS idx_claims_paid_date
  ON hc.claims_fact (paid_date);

-- Helpful for “outlier drilldowns”
CREATE INDEX IF NOT EXISTS idx_claims_provider_cpt
  ON hc.claims_fact (provider_id, cpt_code);
