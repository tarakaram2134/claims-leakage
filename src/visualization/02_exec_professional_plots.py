import os
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


FIG_DIR = Path("reports/figures")
FIG_DIR.mkdir(parents=True, exist_ok=True)


def get_engine():
    db_url = os.environ.get("CLAIMS_DB_URL")
    if not db_url:
        raise RuntimeError("CLAIMS_DB_URL is not set. Example: export CLAIMS_DB_URL='postgresql+psycopg2://tarak@/claims_leakage'")
    return create_engine(db_url)


def fetch_cost_decomposition(engine):
    """
    Pulls your already-validated decomposition numbers for base vs current 12-month windows.
    This matches the logic you ran in sql/analysis/007_cost_decomposition.sql.
    """
    q = """
    WITH base_window AS (
      SELECT
        date_trunc('month', service_date)::date AS m,
        COUNT(*) AS claims,
        SUM(allowed_amount) AS allowed_total
      FROM hc.claims_enriched_v
      WHERE service_date >= DATE '2024-03-01'
        AND service_date <  DATE '2025-03-01'
      GROUP BY 1
    ),
    curr_window AS (
      SELECT
        date_trunc('month', service_date)::date AS m,
        COUNT(*) AS claims,
        SUM(allowed_amount) AS allowed_total
      FROM hc.claims_enriched_v
      WHERE service_date >= DATE '2025-03-01'
        AND service_date <  DATE '2026-03-01'
      GROUP BY 1
    ),
    base AS (
      SELECT SUM(claims) AS claims_base, SUM(allowed_total) AS allowed_base
      FROM base_window
    ),
    curr AS (
      SELECT SUM(claims) AS claims_curr, SUM(allowed_total) AS allowed_curr
      FROM curr_window
    ),
    mix_base AS (
      SELECT
        cpt_group,
        COUNT(*) AS claims_base,
        SUM(allowed_amount) AS allowed_base,
        (SUM(allowed_amount) / NULLIF(COUNT(*),0)) AS apc_base
      FROM hc.claims_enriched_v
      WHERE service_date >= DATE '2024-03-01'
        AND service_date <  DATE '2025-03-01'
      GROUP BY 1
    ),
    mix_curr AS (
      SELECT
        cpt_group,
        COUNT(*) AS claims_curr,
        SUM(allowed_amount) AS allowed_curr,
        (SUM(allowed_amount) / NULLIF(COUNT(*),0)) AS apc_curr
      FROM hc.claims_enriched_v
      WHERE service_date >= DATE '2025-03-01'
        AND service_date <  DATE '2026-03-01'
      GROUP BY 1
    ),
    joined AS (
      SELECT
        COALESCE(b.cpt_group, c.cpt_group) AS cpt_group,
        COALESCE(b.claims_base, 0) AS claims_base,
        COALESCE(c.claims_curr, 0) AS claims_curr,
        COALESCE(b.apc_base, 0) AS apc_base,
        COALESCE(c.apc_curr, 0) AS apc_curr
      FROM mix_base b
      FULL OUTER JOIN mix_curr c USING (cpt_group)
    ),
    base_tot AS (
      SELECT SUM(claims_base) AS claims_base FROM joined
    ),
    shares AS (
      SELECT
        cpt_group,
        claims_base,
        claims_curr,
        apc_base,
        apc_curr,
        (claims_base::numeric / NULLIF((SELECT claims_base FROM base_tot),0)) AS share_base,
        (claims_curr::numeric / NULLIF((SELECT claims_base FROM base_tot),0)) AS share_curr_scaled
      FROM joined
    ),
    effects AS (
      SELECT
        SUM( (share_curr_scaled - share_base) * apc_base ) * (SELECT claims_base FROM base_tot) AS case_mix_effect,
        SUM( share_curr_scaled * (apc_curr - apc_base) ) * (SELECT claims_base FROM base_tot) AS price_effect
      FROM shares
    )
    SELECT
      b.claims_base,
      c.claims_curr,
      b.allowed_base,
      c.allowed_curr,
      (c.allowed_curr - b.allowed_base) AS total_allowed_delta,
      ((c.claims_curr - b.claims_base)::numeric * (b.allowed_base / NULLIF(b.claims_base,0))) AS volume_effect,
      e.case_mix_effect,
      e.price_effect,
      (
        ((c.claims_curr - b.claims_base)::numeric * (b.allowed_base / NULLIF(b.claims_base,0)))
        + e.case_mix_effect
        + e.price_effect
      ) AS reconstructed_delta
    FROM base b
    CROSS JOIN curr c
    CROSS JOIN effects e;
    """
    df = pd.read_sql(text(q), engine)
    if df.empty:
        raise RuntimeError("Cost decomposition query returned no rows.")
    return df.iloc[0].to_dict()


def fetch_exec_kpis(engine):
    """
    KPIs for the executive panel:
    - base 12m allowed
    - current 12m allowed
    - delta and pct change
    - price effect share
    """
    decomp = fetch_cost_decomposition(engine)

    allowed_base = float(decomp["allowed_base"])
    allowed_curr = float(decomp["allowed_curr"])
    delta = float(decomp["total_allowed_delta"])
    pct = (delta / allowed_base) if allowed_base else np.nan

    price_effect = float(decomp["price_effect"])
    price_share = (price_effect / delta) if delta else np.nan

    return {
        "allowed_base": allowed_base,
        "allowed_curr": allowed_curr,
        "delta": delta,
        "pct": pct,
        "price_effect": price_effect,
        "case_mix_effect": float(decomp["case_mix_effect"]),
        "volume_effect": float(decomp["volume_effect"]),
        "price_share": price_share,
    }


def fetch_provider_scatter(engine):
    """
    Provider scatter uses EB outputs:
    - x: claims volume in last 6 months
    - y: avg EB residual APC in last 6 months
    - size: estimated excess allowed in last 6 months
    """
    q = """
    WITH recent AS (
      SELECT *
      FROM hc.provider_outlier_scores_eb
      WHERE service_month >= (date_trunc('month', CURRENT_DATE) - INTERVAL '6 months')
    ),
    rollup AS (
      SELECT
        provider_id,
        specialty,
        SUM(claim_count) AS claims_6m,
        AVG(eb_residual_apc) AS avg_eb_residual_apc,
        SUM(eb_residual_apc * claim_count) AS est_excess_allowed_6m,
        AVG(w) AS avg_weight
      FROM recent
      GROUP BY 1,2
    )
    SELECT *
    FROM rollup
    WHERE claims_6m > 0;
    """
    df = pd.read_sql(text(q), engine)
    if df.empty:
        raise RuntimeError("Provider scatter query returned no rows. Check hc.provider_outlier_scores_eb exists and has data.")
    return df


def money(x):
    # human-friendly formatting for labels
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def plot_exec_kpi_panel(kpis):
    fig = plt.figure(figsize=(12, 4.5))
    ax = fig.add_subplot(111)
    ax.axis("off")

    title = "Outpatient Cost Growth Summary (12M vs Prior 12M)"
    subtitle = "Signals whether growth is utilization-driven or pricing-driven (with decomposition)."

    ax.text(0.01, 0.92, title, fontsize=16, fontweight="bold", ha="left", va="top")
    ax.text(0.01, 0.84, subtitle, fontsize=10.5, ha="left", va="top")

    # KPI blocks
    blocks = [
        ("Baseline allowed (12M)", money(kpis["allowed_base"])),
        ("Current allowed (12M)", money(kpis["allowed_curr"])),
        ("YoY delta", money(kpis["delta"])),
        ("YoY change", f"{kpis['pct']*100:.2f}%"),
        ("Price effect", money(kpis["price_effect"])),
        ("Price share of delta", f"{kpis['price_share']*100:.1f}%"),
    ]

    x0, y0 = 0.01, 0.70
    w, h = 0.31, 0.20
    pad_x, pad_y = 0.02, 0.06

    for i, (label, value) in enumerate(blocks):
        col = i % 3
        row = i // 3
        bx = x0 + col * (w + pad_x)
        by = y0 - row * (h + pad_y)

        ax.add_patch(plt.Rectangle((bx, by - h), w, h, fill=False, linewidth=1.0))
        ax.text(bx + 0.02, by - 0.05, label, fontsize=10, ha="left", va="top")
        ax.text(bx + 0.02, by - 0.12, value, fontsize=16, fontweight="bold", ha="left", va="top")

    # Mini decomposition text
    ax.text(0.01, 0.20, "Decomposition components:", fontsize=11, fontweight="bold", ha="left")
    ax.text(0.01, 0.14, f"Volume effect: {money(abs(kpis['volume_effect']))}", fontsize=10.5, ha="left")
    ax.text(0.25, 0.14, f"Case-mix effect: {money(abs(kpis['case_mix_effect']))}", fontsize=10.5, ha="left")
    ax.text(0.55, 0.14, f"Price effect: {money(abs(kpis['price_effect']))}", fontsize=10.5, ha="left")

    out = FIG_DIR / "06_exec_kpi_panel.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_cost_decomp_waterfall(kpis):
    # Waterfall: Start at 0 and add volume, case-mix, price to reach total delta.
    components = [
        ("Volume", kpis["volume_effect"]),
        ("Case-mix", kpis["case_mix_effect"]),
        ("Price", kpis["price_effect"]),
    ]

    values = [v for _, v in components]
    labels = [l for l, _ in components]
    total = kpis["delta"]

    # cumulative bars
    cum = np.cumsum([0] + values[:-1])
    fig = plt.figure(figsize=(10.5, 5))
    ax = fig.add_subplot(111)

    for i, (label, val) in enumerate(components):
        ax.bar(i, val, bottom=cum[i])
        ax.text(i, cum[i] + val, money(val) if val >= 0 else f"-{money(abs(val))}",
                ha="center", va="bottom" if val >= 0 else "top", fontsize=10)

    # total bar
    ax.bar(len(components), total)
    ax.text(len(components), total, money(total) if total >= 0 else f"-{money(abs(total))}",
            ha="center", va="bottom" if total >= 0 else "top", fontsize=10, fontweight="bold")

    ax.set_xticks(list(range(len(components) + 1)))
    ax.set_xticklabels(labels + ["Total Δ"])
    ax.set_ylabel("Allowed $ change (12M)")
    ax.set_title("YoY Allowed Spend Decomposition (Waterfall)")

    out = FIG_DIR / "07_cost_decomposition_waterfall.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_provider_outlier_scatter(df):
    # Keep it readable: focus on top specialties by provider count.
    top_specs = df["specialty"].value_counts().head(10).index.tolist()
    df_plot = df[df["specialty"].isin(top_specs)].copy()

    # bubble sizing (excess allowed)
    size = df_plot["est_excess_allowed_6m"].clip(lower=0)
    if size.max() > 0:
        sizes = 30 + 470 * (size / size.max())
    else:
        sizes = np.full(len(df_plot), 60)

    fig = plt.figure(figsize=(11, 6))
    ax = fig.add_subplot(111)

    # categorical coloring via integer codes
    spec_codes = df_plot["specialty"].astype("category").cat.codes
    scatter = ax.scatter(
        df_plot["claims_6m"],
        df_plot["avg_eb_residual_apc"],
        s=sizes,
        c=spec_codes,
        alpha=0.75,
        linewidths=0.3
    )

    ax.axhline(0, linewidth=1.0)
    # a practical “review” line: top tail of avg EB residual
    thresh = float(df_plot["avg_eb_residual_apc"].quantile(0.95))
    ax.axhline(thresh, linestyle="--", linewidth=1.0)

    ax.set_title("Provider Outliers (EB-adjusted): Volume vs Residual Allowed/Claim")
    ax.set_xlabel("Claims volume (last 6 months)")
    ax.set_ylabel("Avg EB residual allowed per claim (last 6 months)")

    # legend for specialties (top 10 only)
    spec_map = dict(enumerate(df_plot["specialty"].astype("category").cat.categories))
    handles = []
    labels = []
    for code, spec in spec_map.items():
        handles.append(plt.Line2D([0], [0], marker="o", linestyle="", markersize=7))
        labels.append(spec)

    ax.legend(handles, labels, title="Specialty (top 10 by count)", loc="best", fontsize=9, title_fontsize=9)

    note = f"Dashed line = 95th percentile residual (~{thresh:.1f}). Bubble size ~ excess allowed (6M)."
    ax.text(0.01, -0.12, note, transform=ax.transAxes, fontsize=9, ha="left")

    out = FIG_DIR / "08_provider_outlier_scatter.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


def main():
    engine = get_engine()

    kpis = fetch_exec_kpis(engine)
    df_scatter = fetch_provider_scatter(engine)

    plot_exec_kpi_panel(kpis)
    plot_cost_decomp_waterfall(kpis)
    plot_provider_outlier_scatter(df_scatter)


if __name__ == "__main__":
    main()
