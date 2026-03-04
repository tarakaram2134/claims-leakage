Healthcare Claims Leakage Analytics
Cost Decomposition, Provider Benchmarking & Empirical Bayes Risk Adjustment
Overview

This project builds a healthcare analytics pipeline to identify outpatient cost leakage using SQL-based benchmarking and Empirical Bayes shrinkage.

Using 1.2M synthetic claims stored in PostgreSQL, the analysis:

Quantifies rolling cost trends

Decomposes YoY spend into volume, case-mix, and price effects

Benchmarks providers against peer groups

Applies statistical shrinkage to reduce noise

Generates an audit-ready provider shortlist

# Key Results

---

## 1. Rolling 12-Month Allowed per Claim

![Rolling 12M Allowed per Claim](reports/figures/01_rolling_12m_allowed_per_claim.png)

Allowed per claim shows sustained inflation while claim volume remains relatively stable.

---

## 2. Year-over-Year Cost Decomposition

![Cost Decomposition](reports/figures/02_cost_decomposition.png)

Mar 2025–Feb 2026 vs prior 12 months:

- Total increase: +$9.93M  
- Volume effect: +$0.29M  
- Case-mix effect: +$0.49M  
- Price effect: +$9.16M  

Conclusion: Cost growth is overwhelmingly price-driven.

---

## 3. CPT Group Contribution (Price vs Case Mix)

![CPT Group Contributions](reports/figures/04_cpt_group_price_vs_mix.png)

Largest price-driven categories:

- Dermatology Procedures  
- Imaging  
- PT/OT  
- Minor Surgery  

---

## 4. Top Providers by Estimated Excess Allowed (Empirical Bayes Adjusted)

![Top Providers Excess Allowed](reports/figures/03_top10_provider_excess_allowed.png)

Top providers demonstrate:

- Sustained high EB percentile months  
- Credibility weights between 0.36–0.39  
- Estimated 6-month excess between $36K–$69K  

---

## 5. Raw vs Empirical Bayes Residual Distribution

![Residual Distribution Raw vs EB](reports/figures/05_residual_distribution_raw_vs_eb.png)

Shrinkage reduces heavy tails and stabilizes outlier detection.
Methodology

Providers grouped by:

Specialty

Place of Service

County

Residual:

provider_allowed_per_claim − peer_allowed_per_claim

Empirical Bayes adjustment:

EB_residual = w × residual

Flag criteria:

≥ 2 high EB percentile months

≥ 150 claims in 6 months

≥ 4 months observed

Project Structure
claims-leakage/
├── sql/
├── src/
├── reports/
│   └── figures/
├── data/
└── README.md
Author

Tarak Ram Donepudi
MS Computer Science

Now Verify