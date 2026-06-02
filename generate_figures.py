"""
Generate fig1_e2e_latency.png, fig1b_staleness.png, and fig2_layer_lag.png
from benchmark result CSVs.
Output: /mnt/c/Users/tranm/Downloads/

Staleness uses corrected post-first-Gold values from staleness_corrected.csv.
E2E and lag metrics are loaded directly from per-pipeline result CSVs.
"""

import csv
import math
import os
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BASE   = Path(__file__).parent / "benchmark_result"
OUTDIR = Path("/mnt/c/Users/tranm/Downloads")

SPARK_FILES = {
    1500: BASE / "spark_result/50_result.csv",
    3000: BASE / "spark_result/100_result.csv",
    6000: BASE / "spark_result/200_result.csv",
}
FLINK_FILES = {
    1500: BASE / "flink_result/50_result.csv",
    3000: BASE / "flink_result/100_result.csv",
    6000: BASE / "flink_result/200_result.csv",
}
# Rate → users_per_tick mapping for staleness_corrected lookup
RATE_TO_UPT = {1500: 50, 3000: 100, 6000: 200}
RATES = [1500, 3000, 6000]


# ── Data loading ──────────────────────────────────────────────────────────────
def load_measured(path):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r["is_warmup"].strip().lower() in ("false", "0"):
                rows.append(r)
    return rows


def mean(vals):
    return sum(vals) / len(vals) if vals else 0.0


def std(vals):
    if len(vals) < 2:
        return 0.0
    m = mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))


def extract(files, key):
    """Returns {rate: (mean, std)} for a given metric key."""
    result = {}
    for rate, path in files.items():
        rows = load_measured(path)
        vals = [float(r[key]) for r in rows if r.get(key, "") not in ("", "-1.0", "-1")]
        result[rate] = (mean(vals), std(vals))
    return result


def load_corrected_staleness():
    """Load post-first-Gold corrected staleness from staleness_corrected.csv.
    Returns {pipeline_name: {rate: (mean_corr_avg, std_corr_avg)}}.
    """
    path = BASE / "staleness_corrected.csv"
    data = {"spark": {}, "flink": {}}
    # Collect per-run corr_avg_s grouped by (pipeline, rate)
    groups: dict[tuple, list[float]] = {}
    upt_to_rate = {50: 1500, 100: 3000, 200: 6000}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row["is_warmup"].strip().lower() in ("true", "1"):
                continue
            pipeline = row["pipeline"]          # "spark" or "flink"
            upt      = int(row["rate"])          # users_per_tick: 50/100/200
            rate     = upt_to_rate.get(upt, upt) # map to events/s: 1500/3000/6000
            key = (pipeline, rate)
            groups.setdefault(key, []).append(float(row["corr_avg_s"]))
    for (pipeline, rate), vals in groups.items():
        if pipeline in data:
            data[pipeline][rate] = (mean(vals), std(vals))
    return data


# ── Pull all metrics ──────────────────────────────────────────────────────────
corrected_staleness = load_corrected_staleness()

metrics = {}
for label, files in [("flink", FLINK_FILES), ("spark", SPARK_FILES)]:
    metrics[label] = {
        "e2e":        extract(files, "gold_e2e_s"),
        "staleness":  corrected_staleness[label],   # corrected post-first-Gold
        "bronze_lag": extract(files, "bronze_lag_s"),
        "silver_lag": extract(files, "silver_lag_s"),
        "gold_lag":   extract(files, "gold_lag_s"),
    }


# ── matplotlib setup ──────────────────────────────────────────────────────────
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        13,
    "axes.titlesize":   14,
    "axes.labelsize":   13,
    "xtick.labelsize":  12,
    "ytick.labelsize":  12,
    "legend.fontsize":  12,
    "figure.dpi":       200,
})

COL_FLINK = "#2166ac"   # blue
COL_SPARK = "#d6604d"   # red-orange
HATCH_F   = ""
HATCH_S   = "///"


def _draw_bar_chart(ax, met_key, title, ylabel, show_legend=True, footnote=None):
    """Draw a paired bar chart on ax for the given metric."""
    x      = np.arange(len(RATES))
    width  = 0.30
    offset = 0.17

    flink_means = [metrics["flink"][met_key][r][0] for r in RATES]
    flink_stds  = [metrics["flink"][met_key][r][1] for r in RATES]
    spark_means = [metrics["spark"][met_key][r][0] for r in RATES]
    spark_stds  = [metrics["spark"][met_key][r][1] for r in RATES]

    bars_f = ax.bar(
        x - offset, flink_means, width,
        yerr=flink_stds, capsize=4,
        color=COL_FLINK, hatch=HATCH_F, edgecolor="black", linewidth=0.8,
        error_kw={"elinewidth": 1.2, "ecolor": "black"},
        label="Pipeline B (Flink+Iceberg)",
        zorder=3,
    )
    bars_s = ax.bar(
        x + offset, spark_means, width,
        yerr=spark_stds, capsize=4,
        color=COL_SPARK, hatch=HATCH_S, edgecolor="black", linewidth=0.8,
        error_kw={"elinewidth": 1.2, "ecolor": "black"},
        label="Pipeline A (Spark+Delta Lake)",
        zorder=3,
    )

    # Value labels on Flink bars
    for bar, val in zip(bars_f, flink_means):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(spark_means) * 0.01,
            f"{val:.0f}s",
            ha="center", va="bottom", fontsize=8, color=COL_FLINK, fontweight="bold",
        )

    # Ratio annotations above Spark bars
    for sm, fm, se, bar in zip(spark_means, flink_means, spark_stds, bars_s):
        ratio = sm / fm if fm > 0 else 0
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + se + max(spark_means) * 0.02,
            f"{ratio:.1f}×",
            ha="center", va="bottom", fontsize=8.5, color="black", style="italic",
        )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{r:,}" for r in RATES])
    ax.set_xlabel("Offered rate (events/s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_ylim(0, max(spark_means) * 1.30)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)

    if show_legend:
        ax.legend(loc="upper left", framealpha=0.9)

    if footnote:
        ax.annotate(
            footnote,
            xy=(0, -0.18), xycoords="axes fraction",
            fontsize=7.5, color="gray", style="italic",
        )


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 1a — End-to-End Latency  (standalone)
# ══════════════════════════════════════════════════════════════════════════════
fig1, ax1 = plt.subplots(figsize=(7, 5.5))
_draw_bar_chart(
    ax1, "e2e",
    title="End-to-End Latency",
    ylabel="Latency (s)",
    show_legend=True,
)
fig1.suptitle(
    "Figure 1. End-to-end latency across three load levels.\n"
    "Error bars = SD of 3 measured runs. Ratios (italic) = Spark ÷ Flink.",
    fontsize=9.5, y=0.01, va="bottom",
)
fig1.tight_layout(rect=[0, 0.07, 1, 1])
out1 = OUTDIR / "fig1_e2e_latency.png"
fig1.savefig(out1, bbox_inches="tight")
print(f"Saved {out1}")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 1b — Average Gold Staleness  (standalone, corrected data)
# ══════════════════════════════════════════════════════════════════════════════
fig1b, ax1b = plt.subplots(figsize=(7, 5.5))
_draw_bar_chart(
    ax1b, "staleness",
    title="Average Gold Staleness (post-first-commit)",
    ylabel="Staleness (s)",
    show_legend=True,
    footnote="†Measured from first Gold commit of each run; pre-commit idle samples excluded.",
)
fig1b.suptitle(
    "Figure 2. Average Gold staleness across three load levels.\n"
    "Error bars = SD of 3 measured runs. Ratios (italic) = Spark ÷ Flink.",
    fontsize=9.5, y=0.01, va="bottom",
)
fig1b.tight_layout(rect=[0, 0.07, 1, 1])
out1b = OUTDIR / "fig1b_staleness.png"
fig1b.savefig(out1b, bbox_inches="tight")
print(f"Saved {out1b}")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — Per-layer catch-up lag
# ══════════════════════════════════════════════════════════════════════════════
LAYERS     = ["Bronze", "Silver", "Gold"]
LAG_KEYS   = ["bronze_lag", "silver_lag", "gold_lag"]

# Layout: 3 rate columns, each column shows Bronze/Silver/Gold bars (Flink vs Spark)
fig2, axes2 = plt.subplots(1, 3, figsize=(16, 5.5), sharey=False)
fig2.subplots_adjust(wspace=0.28)

LAYER_COLORS_F = ["#4393c3", "#2166ac", "#053061"]   # blue shades (Bronze→Silver→Gold)
LAYER_COLORS_S = ["#f4a582", "#d6604d", "#67001f"]   # red shades

for col_idx, rate in enumerate(RATES):
    ax = axes2[col_idx]

    x     = np.arange(len(LAYERS))
    width = 0.30
    offset = 0.17

    flink_vals = [metrics["flink"][k][rate][0] for k in LAG_KEYS]
    spark_vals = [metrics["spark"][k][rate][0] for k in LAG_KEYS]

    bars_f = ax.bar(
        x - offset, flink_vals, width,
        color=LAYER_COLORS_F, edgecolor="black", linewidth=0.7,
        label="Flink+Iceberg", zorder=3,
    )
    bars_s = ax.bar(
        x + offset, spark_vals, width,
        color=LAYER_COLORS_S, hatch="///", edgecolor="black", linewidth=0.7,
        label="Spark+Delta Lake", zorder=3,
    )

    # Ratio labels above Spark bars (Bronze and Silver only — Gold is similar)
    for i, (sv, fv) in enumerate(zip(spark_vals, flink_vals)):
        bar = bars_s[i]
        if i < 2:  # Bronze and Silver
            ratio = sv / fv if fv > 0 else 0
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(spark_vals) * 0.02,
                f"{ratio:.1f}×",
                ha="center", va="bottom", fontsize=8.5, style="italic",
            )
        # Flink value labels
        bar_f = bars_f[i]
        ax.text(
            bar_f.get_x() + bar_f.get_width() / 2,
            bar_f.get_height() + max(spark_vals) * 0.01,
            f"{fv:.0f}s",
            ha="center", va="bottom", fontsize=7.5, color="#053061", fontweight="bold",
        )

    ax.set_xticks(x)
    ax.set_xticklabels(LAYERS)
    ax.set_title(f"{rate:,} events/s")
    ax.set_ylabel("Catch-up lag (s)" if col_idx == 0 else "")
    ax.set_ylim(0, max(spark_vals) * 1.30)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)

    if col_idx == 0:
        legend_handles = [
            mpatches.Patch(facecolor=COL_FLINK, edgecolor="black", label="Pipeline B (Flink+Iceberg)"),
            mpatches.Patch(facecolor=COL_SPARK, edgecolor="black", hatch="///", label="Pipeline A (Spark+Delta Lake)"),
        ]
        ax.legend(handles=legend_handles, loc="upper right", framealpha=0.9, fontsize=9)

fig2.suptitle(
    "Figure 2. Per-layer catch-up lag (Bronze, Silver, Gold) after the producer stops.\n"
    "Ratios (italic) above Spark bars = Spark ÷ Flink. Numbers above Flink bars = absolute lag.",
    fontsize=9.5, y=0.01, va="bottom",
)
fig2.tight_layout(rect=[0, 0.07, 1, 1])
out2 = OUTDIR / "fig2_layer_lag.png"
fig2.savefig(out2, bbox_inches="tight")
print(f"Saved {out2}")

print("Done.")
