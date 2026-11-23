#!/usr/bin/env python3
"""
Draw the report figures: the same 12 plots as plot.py (average FCT vs load,
ECMP vs CONGA, for each workload and flow group), but small and compact, for
a LaTeX page with three plots per row. The plot title and the x-axis title
are left out: the LaTeX subcaption and figure caption give them. Only the
panels named with --legend get a legend.

Usage (from the htsim directory):
  python3 scripts/report_figs.py <results_dir> <output_dir> [--legend uniform_all ...]

Output: <output_dir>/<workload>_<all|small|large>.pdf
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from plot import (AXIS, GRID, GROUPS, INK_2, MUTED, SERIES, WORKLOADS,  # noqa: E402
                  collect_rows, legend_handles, plt, series_points)
from matplotlib.ticker import FuncFormatter, MaxNLocator  # noqa: E402

FONT = 7          # points, at the final size on the page
WIDTH = 2.3       # inches: about a third of a 7-inch text width
HEIGHT = 1.3
MARKER_SCALE = 0.6


def draw_panel(rows, workload, group, path, with_legend):
    _, column, _ = next(g for g in GROUPS if g[0] == group)
    lines = {key: series_points(rows, workload, key, column) for key, *_ in SERIES}
    values = [v for _, ys in lines.values() for v in ys]

    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(0.6)

    if not values:
        # Uniform has no small flows: every flow is exactly 100 KB.
        ax.text(0.5, 0.5, "No flows < 100 KB:\nevery uniform flow\nis exactly 100 KB",
                transform=ax.transAxes, ha="center", va="center", color=INK_2, fontsize=FONT)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
    else:
        scale, unit = (1000.0, "ms") if max(values) >= 10_000 else (1.0, "µs")
        for key, label, color, marker, hollow, size, line_z, marker_z in SERIES:
            xs, ys = lines[key]
            ys = [y / scale for y in ys]
            ax.plot(xs, ys, color=color, linewidth=1.1, zorder=line_z, clip_on=False)
            ax.plot(xs, ys, linestyle="none", marker=marker, markersize=size * MARKER_SCALE,
                    markerfacecolor="none" if hollow else color,
                    markeredgecolor=color if hollow else "white",
                    markeredgewidth=1.1 if hollow else 0.6, zorder=marker_z, clip_on=False)
        ax.set_xlim(5, 105)
        ax.set_xticks(range(10, 101, 10))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}"))
        ax.set_ylim(0, max(values) / scale * 1.12)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=4, steps=[1, 2, 2.5, 5, 10]))
        ax.yaxis.set_major_formatter(FuncFormatter(
            lambda v, _: f"{v:,.0f}" if v >= 10 or v == 0 else f"{v:g}"))
        ax.tick_params(colors=MUTED, labelsize=FONT - 0.5, length=0, pad=2)
        ax.grid(axis="y", color=GRID, linewidth=0.5)
        ax.set_axisbelow(True)
        ax.set_ylabel(f"Avg FCT ({unit})", color=INK_2, fontsize=FONT, labelpad=2)
        if with_legend:
            handles = legend_handles()
            for h in handles:
                h.set_markersize(h.get_markersize() * MARKER_SCALE)
                h.set_linewidth(1.1)
            ax.legend(handles=handles, loc="upper left", frameon=False, fontsize=FONT - 0.5,
                      labelcolor=INK_2, handlelength=1.8, borderaxespad=0.2)

    # Same page size and margins for every panel, so LaTeX scales them all
    # by the same amount and the text has the same size everywhere.
    fig.subplots_adjust(left=0.205, right=0.985, top=0.96, bottom=0.135)
    fig.savefig(path)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results_dir")
    parser.add_argument("output_dir")
    parser.add_argument("--legend", nargs="*", default=["uniform_all"],
                        help="panels that get a legend, e.g. uniform_all pareto_small")
    args = parser.parse_args()

    rows = collect_rows(args.results_dir)
    os.makedirs(args.output_dir, exist_ok=True)
    for wl in WORKLOADS:
        for group, _, _ in GROUPS:
            name = f"{wl}_{group}"
            draw_panel(rows, wl, group, os.path.join(args.output_dir, name + ".pdf"),
                       name in args.legend)
    print(f"Wrote {len(WORKLOADS) * len(GROUPS)} figures to {args.output_dir}")


if __name__ == "__main__":
    main()
