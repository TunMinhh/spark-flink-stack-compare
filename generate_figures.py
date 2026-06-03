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


def extract_gold_ready_e2e(files):
    """Returns {rate: (mean, std)} for Gold-ready E2E = producer_stop_s + gold_lag_s."""
    result = {}
    for rate, path in files.items():
        rows = load_measured(path)
        vals = [
            float(r["producer_stop_s"]) + float(r["gold_lag_s"])
            for r in rows
            if r.get("producer_stop_s", "") not in ("", "-1.0", "-1")
            and r.get("gold_lag_s", "") not in ("", "-1.0", "-1")
        ]
        result[rate] = (mean(vals), std(vals))
    return result


def load_corrected_staleness():
    """Load post-first-Gold corrected staleness.
    - Spark : from staleness_corrected.csv  (pre-computed corr_avg_s)
    - Flink : computed inline from flink_result staleness + result CSVs
    Returns {pipeline_name: {rate: (mean_corr_avg, std_corr_avg)}}.
    """
    upt_to_rate = {50: 1500, 100: 3000, 200: 6000}
    data = {"spark": {}, "flink": {}}
    groups: dict[tuple, list[float]] = {}

    # ── Spark: read from staleness_corrected.csv ─────────────────────────────
    corr_path = BASE / "staleness_corrected.csv"
    with open(corr_path, newline="") as f:
        for row in csv.DictReader(f):
            if row["is_warmup"].strip().lower() in ("true", "1"):
                continue
            upt  = int(row["rate"])
            rate = upt_to_rate.get(upt, upt)
            groups.setdefault(("spark", rate), []).append(float(row["corr_avg_s"]))

    # ── Flink: compute inline from raw staleness + result CSVs ───────────────
    flink_result_files = {
        1500: BASE / "flink_result/50_result.csv",
        3000: BASE / "flink_result/100_result.csv",
        6000: BASE / "flink_result/200_result.csv",
    }
    flink_staleness_files = {
        1500: BASE / "flink_result/50_staleness.csv",
        3000: BASE / "flink_result/100_staleness.csv",
        6000: BASE / "flink_result/200_staleness.csv",
    }
    for rate in RATES:
        stal_path = flink_staleness_files[rate]
        res_path  = flink_result_files[rate]
        if stal_path.exists():
            # Build {run_label: first_gold_latency_s} from result CSV
            first_gold: dict[str, float] = {}
            with open(res_path, newline="") as f:
                for row in csv.DictReader(f):
                    if row["is_warmup"].strip().lower() in ("true", "1"):
                        continue
                    lbl = f"{row['pipeline']}_rate{row['req_per_sec']}_run{row['run']}"
                    try:
                        first_gold[lbl] = float(row["first_gold_latency_s"])
                    except (ValueError, KeyError):
                        first_gold[lbl] = 0.0
            # Filter post-first-gold, compute corrected avg per run
            run_samples: dict[str, list[float]] = {}
            with open(stal_path, newline="") as f:
                for row in csv.DictReader(f):
                    if row["is_warmup"].strip().lower() in ("true", "1"):
                        continue
                    lbl = row["run_label"]
                    fg  = first_gold.get(lbl, 0.0)
                    if float(row["wall_s"]) >= fg:
                        run_samples.setdefault(lbl, []).append(float(row["staleness_s"]))
            for lbl, vals in run_samples.items():
                if vals:
                    groups.setdefault(("flink", rate), []).append(mean(vals))
        else:
            # Fallback: use avg_staleness_s from result CSV directly
            with open(res_path, newline="") as f:
                for row in csv.DictReader(f):
                    if row["is_warmup"].strip().lower() in ("true", "1"):
                        continue
                    try:
                        groups.setdefault(("flink", rate), []).append(
                            float(row["avg_staleness_s"])
                        )
                    except (ValueError, KeyError):
                        pass

    # ── Aggregate runs → (mean, std) per pipeline+rate ───────────────────────
    for (pipeline, rate), vals in groups.items():
        if pipeline in data:
            data[pipeline][rate] = (mean(vals), std(vals))
    return data


# ── Pull all metrics ──────────────────────────────────────────────────────────
corrected_staleness = load_corrected_staleness()

metrics = {}
for label, files in [("flink", FLINK_FILES), ("spark", SPARK_FILES)]:
    metrics[label] = {
        "e2e":        extract_gold_ready_e2e(files),
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
    "legend.fontsize":  11,
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

    max_val = max(spark_means)

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

    # Flink: value label just above bar (blue bold)
    for bar, val in zip(bars_f, flink_means):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_val * 0.012,
            f"{val:.0f}s",
            ha="center", va="bottom", fontsize=9, color=COL_FLINK, fontweight="bold",
        )

    # Spark: value label centered inside bar (white bold) + ratio above bar
    for sm, fm, se, bar in zip(spark_means, flink_means, spark_stds, bars_s):
        ratio = sm / fm if fm > 0 else 0
        # Value inside bar
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() * 0.50,
            f"{sm:.0f}s",
            ha="center", va="center", fontsize=9, color="white", fontweight="bold",
            zorder=4,
        )
        # Ratio above bar top + error bar clearance
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + se + max_val * 0.035,
            f"{ratio:.1f}×",
            ha="center", va="bottom", fontsize=9, color="black", style="italic",
        )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{r:,}" for r in RATES])
    ax.set_xlabel("Offered rate (events/s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_ylim(0, max_val * 1.55)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)

    if show_legend:
        # upper right: Spark bars (left/center) won't conflict; Flink bars are short
        ax.legend(loc="upper right", framealpha=0.95, fontsize=10,
                  handlelength=1.4, handleheight=0.9, borderpad=0.6)

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
    title="End-to-End Gold-Ready Latency",
    ylabel="Latency (s)",
    show_legend=True,
)
fig1.suptitle(
    "Figure 1. Gold-ready E2E latency = producer_stop + gold_lag, across three load levels.\n"
    "Error bars = SD of 3 measured runs. Values inside/above bars. Ratios (italic) = Spark ÷ Flink.",
    fontsize=9, y=0.01, va="bottom",
)
fig1.tight_layout(rect=[0, 0.08, 1, 1])
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
    "Error bars = SD of 3 measured runs. Values inside/above bars. Ratios (italic) = Spark ÷ Flink.",
    fontsize=9, y=0.01, va="bottom",
)
fig1b.tight_layout(rect=[0, 0.08, 1, 1])
out1b = OUTDIR / "fig1b_staleness.png"
fig1b.savefig(out1b, bbox_inches="tight")
print(f"Saved {out1b}")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — Per-layer catch-up lag
# ══════════════════════════════════════════════════════════════════════════════
LAYERS     = ["Bronze", "Silver", "Gold"]
LAG_KEYS   = ["bronze_lag", "silver_lag", "gold_lag"]

fig2, axes2 = plt.subplots(1, 3, figsize=(16, 5.5), sharey=False)

LAYER_COLORS_F = ["#4393c3", "#2166ac", "#053061"]   # blue shades (Bronze→Silver→Gold)
LAYER_COLORS_S = ["#f4a582", "#d6604d", "#67001f"]   # red shades

for col_idx, rate in enumerate(RATES):
    ax = axes2[col_idx]

    x      = np.arange(len(LAYERS))
    width  = 0.30
    offset = 0.17

    flink_vals = [metrics["flink"][k][rate][0] for k in LAG_KEYS]
    spark_vals = [metrics["spark"][k][rate][0] for k in LAG_KEYS]
    max_sv = max(spark_vals)

    bars_f = ax.bar(
        x - offset, flink_vals, width,
        color=LAYER_COLORS_F, edgecolor="black", linewidth=0.7,
        zorder=3,
    )
    bars_s = ax.bar(
        x + offset, spark_vals, width,
        color=LAYER_COLORS_S, hatch="///", edgecolor="black", linewidth=0.7,
        zorder=3,
    )

    # Flink: value label just above bar
    for fv, bar_f in zip(flink_vals, bars_f):
        ax.text(
            bar_f.get_x() + bar_f.get_width() / 2,
            bar_f.get_height() + max_sv * 0.012,
            f"{fv:.0f}s",
            ha="center", va="bottom", fontsize=8, color="#053061", fontweight="bold",
        )

    # Spark: value inside bar (white bold) + ratio above bar for all three layers
    for i, (sv, fv, bar_s) in enumerate(zip(spark_vals, flink_vals, bars_s)):
        ratio = sv / fv if fv > 0 else 0
        # Value centered inside Spark bar
        ax.text(
            bar_s.get_x() + bar_s.get_width() / 2,
            bar_s.get_height() * 0.50,
            f"{sv:.0f}s",
            ha="center", va="center", fontsize=8, color="white", fontweight="bold",
            zorder=4,
        )
        # Ratio annotation above bar
        ax.text(
            bar_s.get_x() + bar_s.get_width() / 2,
            bar_s.get_height() + max_sv * 0.03,
            f"{ratio:.1f}×",
            ha="center", va="bottom", fontsize=8.5, style="italic",
        )

    ax.set_xticks(x)
    ax.set_xticklabels(LAYERS)
    ax.set_title(f"{rate:,} events/s")
    ax.set_ylabel("Catch-up lag (s)" if col_idx == 0 else "")
    ax.set_ylim(0, max_sv * 1.50)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)

# Shared legend above all subplots (fig-level, avoids any ax overlap)
_leg_handles = [
    mpatches.Patch(facecolor=COL_FLINK, edgecolor="black",
                   label="Pipeline B (Flink+Iceberg)"),
    mpatches.Patch(facecolor=COL_SPARK, edgecolor="black", hatch="///",
                   label="Pipeline A (Spark+Delta Lake)"),
]
fig2.legend(handles=_leg_handles, loc="upper center", ncol=2,
            bbox_to_anchor=(0.5, 1.0), fontsize=10, framealpha=0.95,
            handlelength=1.4, handleheight=0.9)

fig2.suptitle(
    "Figure 2. Per-layer catch-up lag (Bronze, Silver, Gold) after the producer stops.\n"
    "Values inside Spark bars and above Flink bars. Ratios (italic) = Spark ÷ Flink.",
    fontsize=9.5, y=0.01, va="bottom",
)
fig2.tight_layout(rect=[0, 0.08, 1, 0.91])
out2 = OUTDIR / "fig2_layer_lag.png"
fig2.savefig(out2, bbox_inches="tight")
print(f"Saved {out2}")

print("Done.")
