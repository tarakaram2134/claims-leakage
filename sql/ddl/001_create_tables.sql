-- =========================
-- Schema
-- =========================
CREATE SCHEMA IF NOT EXISTS hc;

-- =========================
-- Dimensions
-- =========================
CREATE TABLE IF NOT EXISTS hc.providers_dim (
  provider_id      INT PRIMARY KEY,
  specialty        TEXT,
  org_flag         BOOLEAN NOT NULL DEFAULT FALSE,
  contract_type    TEXT NOT NULL,
  address_state    TEXT NOT NULL,
  county_fips      TEXT,
  provider_zip     TEXT
);

CREATE TABLE IF NOT EXISTS hc.cpt_dim (
  cpt_code               VARCHAR(5) PRIMARY KEY,
  cpt_group              TEXT NOT NULL,
  baseline_allowed_weight NUMERIC(10,4) NOT NULL,
  description            TEXT
);

CREATE TABLE IF NOT EXISTS hc.geo_demo_dim (
  county_fips        TEXT PRIMARY KEY,
  median_income      INT,
  pct_over_65        NUMERIC(6,3),
  urbanicity_index   NUMERIC(6,3)
);

-- Optional: member dimension (helps PMPM later without bloating claims_fact)
CREATE TABLE IF NOT EXISTS hc.members_dim (
  member_id          BIGINT PRIMARY KEY,
  member_zip         TEXT NOT NULL,
  member_risk_score  NUMERIC(8,4) NOT NULL,
  risk_score_version TEXT NOT NULL DEFAULT 'v1'
);

-- =========================
-- Fact
-- =========================
CREATE TABLE IF NOT EXISTS hc.claims_fact (
  claim_id         BIGSERIAL PRIMARY KEY,
  member_id        BIGINT NOT NULL REFERENCES hc.members_dim(member_id),
  provider_id      INT NOT NULL REFERENCES hc.providers_dim(provider_id),
  cpt_code         VARCHAR(5) NOT NULL,
  diagnosis_code   VARCHAR(7) NOT NULL,
  place_of_service TEXT NOT NULL,
  service_date     DATE NOT NULL,
  paid_date        DATE NOT NULL,
  units            INT NOT NULL CHECK (units > 0 AND units <= 99),

  billed_amount    NUMERIC(12,2) NOT NULL CHECK (billed_amount >= 0),
  allowed_amount   NUMERIC(12,2) NOT NULL CHECK (allowed_amount >= 0),
  paid_amount      NUMERIC(12,2) NOT NULL CHECK (paid_amount >= 0),

  in_network_flag  BOOLEAN NOT NULL DEFAULT TRUE,
  member_zip       TEXT NOT NULL,

  -- A couple of realism fields for later drilling
  claim_status     TEXT NOT NULL DEFAULT 'final',  -- final, adjusted, reversed
  created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Link CPT dim without FK at first (we'll allow messy CPTs on purpose)
-- Later, we'll validate and create a cleaned view that enforces CPT membership.
