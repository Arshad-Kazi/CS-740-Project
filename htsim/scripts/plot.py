#!/usr/bin/env python3
"""
Draw the Part 3 figures: average flow completion time (FCT) vs. load,
ECMP compared with CONGA.

For each workload (uniform, pareto, enterprise, datamining) it makes three
figures - all flows, small flows (< 100KB), large flows (>= 100KB) - so 12
figures in total. It also writes:
  - overview.<fmt>   all 12 figures on one page (4 rows x 3 columns)
  - fct_table.md     the same numbers as a table, with CONGA's change vs ECMP

Usage (from the htsim directory, after run_experiments.sh):
  python3 scripts/plot.py [results_dir] [--format png|pdf|both]

Output: <results_dir>/plots/<workload>_<all|small|large>.<fmt>
Requires matplotlib:  python3 -m pip install --user matplotlib
"""
import argparse
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")  # draw to files, no window
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summarize import WORKLOADS, collect_rows  # noqa: E402

# ---------------- Style ----------------
# Colors: categorical slots 1 and 2 of the reference palette (blue, orange).
# Marker shape is a second identity channel, so the figures still work when
# printed in grayscale or read with color-vision deficiency.
# The two lines often lie on top of each other, so ECMP uses a large hollow
# circle drawn above CONGA's small filled square: where the points coincide
# the reader sees a blue ring around an orange square, i.e. both series.
SERIES = [
    # key,    label,   color,     marker, hollow, size, line z, marker z
    ("ecmp",  "ECMP",  "#2a78d6", "o",    True,   8.5,  2,      4),
    ("conga", "CONGA", "#eb6834", "s",    False,  5.0,  3,      3),
]
SURFACE = "#ffffff"
INK = "#0b0b0b"        # titles
INK_2 = "#52514e"      # axis titles, legend, labels
MUTED = "#898781"      # tick labels
GRID = "#e1e0d9"       # hairline gridlines
AXIS = "#c3c2b7"       # baseline / axis

GROUPS = [
    # key,     summary column,  title text
    ("all",   "fct_all_us",   "all flows"),
    ("small", "fct_small_us", "small flows (< 100 KB)"),
    ("large", "fct_large_us", "large flows (≥ 100 KB)"),
]
COUNT_COLUMN = {"all": "finished", "small": "n_small", "large": "n_large"}
WORKLOAD_TITLE = {
    "uniform": "Uniform",
    "pareto": "Pareto",
    "enterprise": "Enterprise",
    "datamining": "Data mining",
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
})


def is_number(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def series_points(rows, workload, scheme, column):
    """(loads in %, values) for one line, skipping loads with no data."""
    pts = [(r["load"] * 100, r[column]) for r in rows
           if r["workload"] == workload and r["scheme"] == scheme and is_number(r[column])]
    pts.sort()
    return [p[0] for p in pts], [p[1] for p in pts]


def flow_count_range(rows, workload, group):
    counts = [r[COUNT_COLUMN[group]] for r in rows if r["workload"] == workload]
    counts = [c for c in counts if c > 0]
    return (min(counts), max(counts)) if counts else None


def pct(x):
    """Signed percentage with a real minus sign, e.g. -1.2 -> '−1.2%'."""
    if abs(x) < 0.05:
        return "0.0%"   # avoid printing '−0.0%'
    return f"{x:+.1f}%".replace("-", "−")


def change_range(lines):
    """Smallest and largest (CONGA - ECMP) / ECMP over the loads both have."""
    e = dict(zip(*lines["ecmp"]))
    c = dict(zip(*lines["conga"]))
    changes = [(c[x] - e[x]) / e[x] * 100 for x in e if x in c and e[x] > 0]
    return (min(changes), max(changes)) if changes else None


def subtitle_text(lines, counts, compact):
    if counts:
        lo, hi = counts
        n = f"{lo:,}–{hi:,}" if lo != hi else f"{lo:,}"
    rng = change_range(lines)
    if compact:
        parts = []
        if rng:
            parts.append(f"CONGA vs ECMP: {pct(rng[0])} to {pct(rng[1])}")
        if counts:
            parts.append(f"{n} flows/point")
        return " · ".join(parts) if parts else "no flows in this group"
    out = []
    if rng:
        out.append(f"CONGA vs ECMP average FCT: {pct(rng[0])} to {pct(rng[1])} (negative = CONGA faster)")
    out.append(f"{n} finished flows per point" if counts else "no flows in this group")
    return "\n".join(out)


def config_of(rows):
    """(cores, loadbase) of the runs in this results folder."""
    configs = {(r["cores"], r["loadbase"]) for r in rows}
    if len(configs) > 1:
        print(f"WARNING: results mix several configurations: {sorted(configs)}")
    return sorted(configs)[0]


def config_label(cores):
    """'4 cores (2:1 oversubscribed)' - empty for the lab's 12-core network."""
    if cores == 12:
        return ""
    ratio = (32 * 10) / (cores * 40)   # server capacity / uplink capacity per leaf
    return f"{cores} cores ({ratio:.3g}:1 oversubscribed)"


def draw(ax, rows, workload, group, compact=False):
    """Draw one figure (one workload, one flow group) on the given axes."""
    gkey, column, gtitle = next(g for g in GROUPS if g[0] == group)
    title_size, sub_size, tick_size = (10, 7.5, 7.5) if compact else (12.5, 9, 9)

    cores, loadbase = config_of(rows)
    title = f"{WORKLOAD_TITLE[workload]}: {gtitle}"
    if config_label(cores) and not compact:
        title += f" · {config_label(cores)}"
    counts = flow_count_range(rows, workload, group)

    # Collect both lines first, to pick the unit and axis range.
    lines = {key: series_points(rows, workload, key, column) for key, *_ in SERIES}
    all_values = [v for xs, ys in lines.values() for v in ys]

    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(0.75)

    sub = subtitle_text(lines, counts, compact)
    n_sub_lines = sub.count("\n") + 1
    ax.set_title(title, loc="left", color=INK, fontsize=title_size, fontweight="semibold",
                 pad=(18 if compact else 10 + 13 * n_sub_lines))
    ax.text(0, 1.02, sub, transform=ax.transAxes, color=INK_2, fontsize=sub_size,
            va="bottom", linespacing=1.4)

    if not all_values:
        # Uniform has no small flows: every flow is exactly 100 KB.
        msg = ("No flows smaller than 100 KB:\nevery uniform flow is exactly 100 KB."
               if workload == "uniform" and group == "small" else "No data for this figure.")
        ax.text(0.5, 0.5, msg, transform=ax.transAxes, ha="center", va="center",
                color=INK_2, fontsize=sub_size + 1)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
        return

    # Show microseconds, or milliseconds when the values get large.
    scale, unit = (1000.0, "ms") if max(all_values) >= 10_000 else (1.0, "µs")

    for key, label, color, marker, hollow, size, line_z, marker_z in SERIES:
        xs, ys = lines[key]
        if not xs:
            continue
        ys = [y / scale for y in ys]
        size = size * (0.85 if compact else 1.0)
        ax.plot(xs, ys, color=color, linewidth=1.5, solid_joinstyle="round",
                solid_capstyle="round", zorder=line_z, clip_on=False)
        if hollow:
            ax.plot(xs, ys, linestyle="none", marker=marker, markersize=size,
                    markerfacecolor="none", markeredgecolor=color, markeredgewidth=1.6,
                    zorder=marker_z, clip_on=False)
        else:
            ax.plot(xs, ys, linestyle="none", marker=marker, markersize=size,
                    markerfacecolor=color, markeredgecolor=SURFACE, markeredgewidth=1.0,
                    zorder=marker_z, clip_on=False)

    # Axes: load on x, average FCT on y (from zero, one axis only).
    ax.set_xlim(5, 105)
    ax.set_xticks(range(10, 101, 10))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylim(0, max(all_values) / scale * 1.12)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, steps=[1, 2, 2.5, 5, 10]))
    ax.yaxis.set_major_formatter(FuncFormatter(
        lambda v, _: f"{v:,.0f}" if v >= 10 or v == 0 else f"{v:g}"))
    ax.tick_params(colors=MUTED, labelsize=tick_size, length=0, pad=4)
    ax.grid(axis="y", color=GRID, linewidth=0.75, linestyle="-")
    ax.set_axisbelow(True)
    base = "core link capacity" if loadbase == "core" else "server link capacity"
    ax.set_xlabel(f"Load (% of {base})", color=INK_2, fontsize=tick_size + 0.5)
    ax.set_ylabel(f"Average FCT ({unit})", color=INK_2, fontsize=tick_size + 0.5)

    if not compact:
        ax.legend(handles=legend_handles(), loc="upper left", frameon=False,
                  fontsize=tick_size + 0.5, labelcolor=INK_2, handlelength=2.2)
        add_end_labels(ax, lines, scale, tick_size)


def add_end_labels(ax, lines, scale, size):
    """Label each line at its right end - only when the two ends are far enough
    apart not to collide. Otherwise the legend alone carries identity."""
    ends = {}
    for key, label, *_ in SERIES:
        xs, ys = lines[key]
        if xs:
            ends[label] = (xs[-1], ys[-1] / scale)
    if len(ends) < 2:
        return
    ymax = ax.get_ylim()[1]
    (_, y1), (_, y2) = ends.values()
    if abs(y1 - y2) / ymax < 0.07:
        return
    for label, (x, y) in ends.items():
        ax.annotate(label, (x, y), xytext=(8, 0), textcoords="offset points",
                    va="center", ha="left", color=INK_2, fontsize=size, clip_on=False)


def legend_handles():
    handles = []
    for _, label, color, marker, hollow, size, _, _ in SERIES:
        handles.append(Line2D(
            [0], [0], color=color, linewidth=1.5, marker=marker, markersize=size,
            markerfacecolor="none" if hollow else color,
            markeredgecolor=color if hollow else SURFACE,
            markeredgewidth=1.6 if hollow else 1.0, label=label))
    return handles


def write_table(rows, path):
    """Table view of every figure: average FCT in us, ECMP vs CONGA."""
    by = {(r["workload"], r["load"], r["scheme"]): r for r in rows}
    loads = sorted({r["load"] for r in rows})

    def cell(x):
        return f"{x:,.1f}" if is_number(x) else "n/a"

    def change(e, c):
        if not (is_number(e) and is_number(c)) or e == 0:
            return "n/a"
        return pct((c - e) / e * 100)

    out = ["# Average flow completion time (µs), ECMP vs CONGA", "",
           "Change = (CONGA − ECMP) / ECMP. A negative change means CONGA is faster.", ""]
    for wl in WORKLOADS:
        if not any(k[0] == wl for k in by):
            continue
        out += [f"## {WORKLOAD_TITLE[wl]}", "",
                "| Load | All: ECMP | All: CONGA | Change | Small: ECMP | Small: CONGA | Change "
                "| Large: ECMP | Large: CONGA | Change | Flows finished / started |",
                "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for load in loads:
            e, c = by.get((wl, load, "ecmp")), by.get((wl, load, "conga"))
            if not e and not c:
                continue
            cells = [f"{load * 100:.0f}%"]
            for _, col, _ in GROUPS:
                ev = e[col] if e else float("nan")
                cv = c[col] if c else float("nan")
                cells += [cell(ev), cell(cv), change(ev, cv)]
            ref = e or c
            cells.append(f"{ref['finished']:,} / {ref['started']:,}")
            out.append("| " + " | ".join(cells) + " |")
        out.append("")
    with open(path, "w") as f:
        f.write("\n".join(out))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results_dir", nargs="?", default="results")
    parser.add_argument("--format", choices=["png", "pdf", "both"], default="png",
                        help="png for viewing, pdf for a vector figure in the report")
    args = parser.parse_args()

    rows = collect_rows(args.results_dir)
    if not rows:
        sys.exit(f"No finished runs in {args.results_dir}/raw - run scripts/run_experiments.sh first.")
    formats = ["png", "pdf"] if args.format == "both" else [args.format]
    out_dir = os.path.join(args.results_dir, "plots")
    os.makedirs(out_dir, exist_ok=True)

    written = []
    # 12 individual figures.
    for wl in WORKLOADS:
        for group, _, _ in GROUPS:
            fig, ax = plt.subplots(figsize=(6.4, 4.2))
            draw(ax, rows, wl, group)
            for fmt in formats:
                path = os.path.join(out_dir, f"{wl}_{group}.{fmt}")
                fig.savefig(path, dpi=200, bbox_inches="tight")
                written.append(path)
            plt.close(fig)

    # Overview: all 12 on one page, one row per workload.
    fig, axes = plt.subplots(len(WORKLOADS), len(GROUPS), figsize=(13, 15))
    for i, wl in enumerate(WORKLOADS):
        for j, (group, _, _) in enumerate(GROUPS):
            draw(axes[i][j], rows, wl, group, compact=True)
    fig.legend(handles=legend_handles(), loc="upper center", ncol=2, frameon=False,
               fontsize=10, labelcolor=INK_2, bbox_to_anchor=(0.5, 1.0))
    cores, _ = config_of(rows)
    if config_label(cores):
        fig.text(0.01, 1.0, config_label(cores), ha="left", va="top",
                 color=INK, fontsize=11, fontweight="semibold")
    fig.tight_layout(rect=(0, 0, 1, 0.975), h_pad=3.0, w_pad=2.0)
    for fmt in formats:
        path = os.path.join(out_dir, f"overview.{fmt}")
        fig.savefig(path, dpi=150, bbox_inches="tight")
        written.append(path)
    plt.close(fig)

    table = os.path.join(out_dir, "fct_table.md")
    write_table(rows, table)
    written.append(table)

    print(f"Wrote {len(written)} files to {out_dir}/")
    for p in written:
        print("  " + os.path.basename(p))


if __name__ == "__main__":
    main()
