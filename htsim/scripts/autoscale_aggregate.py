#!/usr/bin/env python3
"""
Aggregate per-seed rows from run_autoscale.sh into one table per config.

p99 on a heavy-tailed workload is noisy: a single run's tail is set by a
handful of large flows, so one seed cannot separate two policies. Every
config is therefore run over several seeds and reported as a median with
the min-max spread, so a difference can be read as real only when the
spreads do not overlap.
"""
import argparse
import csv
import math
from collections import OrderedDict

COLS = ["tag", "seed", "base_p99_us", "peak_p99_us", "ratio",
        "degradation_ms", "recovery_ms", "peak_fleet", "final_fleet",
        "server_seconds"]


def median(xs):
    xs = sorted(x for x in xs if not math.isnan(x))
    if not xs:
        return float("nan")
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runs_csv")
    ap.add_argument("--sort", default=None,
                    help="sort rows by this column instead of first-seen order")
    args = ap.parse_args()

    groups = OrderedDict()
    with open(args.runs_csv) as fh:
        for row in csv.reader(fh):
            if not row or len(row) < len(COLS):
                continue
            rec = dict(zip(COLS, row))
            groups.setdefault(rec["tag"], []).append(rec)

    def num(rows, key):
        out = []
        for r in rows:
            try:
                out.append(float(r[key]))
            except (ValueError, KeyError):
                out.append(float("nan"))
        return out

    table = []
    for tag, rows in groups.items():
        ratios = num(rows, "ratio")
        degr = num(rows, "degradation_ms")
        cost = num(rows, "server_seconds")
        peak = num(rows, "peak_fleet")
        table.append({
            "tag": tag,
            "n": len(rows),
            "ratio_med": median(ratios),
            "degr_med": median(degr),
            "degr_min": min(degr), "degr_max": max(degr),
            "cost_med": median(cost),
            "peak_med": median(peak),
        })

    if args.sort:
        table.sort(key=lambda r: r.get(args.sort, 0))

    print("%-18s %3s  %10s  %22s  %9s  %8s" %
          ("config", "n", "p99 ratio", "degradation_ms (med [min-max])",
           "server_s", "peak_fleet"))
    print("-" * 86)
    for r in table:
        print("%-18s %3d  %10.1f  %8.0f [%5.0f-%5.0f]      %9.2f  %8.0f" %
              (r["tag"], r["n"], r["ratio_med"], r["degr_med"],
               r["degr_min"], r["degr_max"], r["cost_med"], r["peak_med"]))
    print()
    print("degradation_ms = integral of relative p99 inflation after the surge.")
    print("Lower is better. Overlapping [min-max] spreads mean the difference")
    print("between two configs is not resolved by this many seeds.")


if __name__ == "__main__":
    main()
