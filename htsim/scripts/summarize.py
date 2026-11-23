#!/usr/bin/env python3
"""
Summarize the Part 3 experiment results produced by run_experiments.sh.

For every (scheme, workload, load) it computes the average flow completion
time of all flows, small flows (< 100KB) and large flows (>= 100KB), and
writes them to <results_dir>/summary.csv. It also prints a side-by-side
ECMP vs CONGA table.

Usage:  python3 scripts/summarize.py [results_dir]      (default: results)
"""
import csv
import os
import sys
from collections import defaultdict

SMALL_LIMIT = 100_000  # bytes: small flows are < 100KB, large flows >= 100KB
WORKLOADS = ["uniform", "pareto", "enterprise", "datamining"]
SCHEMES = ["ecmp", "conga"]


def mean(values):
    return sum(values) / len(values) if values else float("nan")


def parse_run(path):
    small, large = [], []
    started, runtime, stats = 0, None, {}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if not parts:
                continue
            if parts[0] == "F":
                size, fct = int(parts[1]), float(parts[3])
                (small if size < SMALL_LIMIT else large).append(fct)
            elif parts[0] == "STARTED":
                started = int(parts[1])
            elif parts[0] == "DONE":
                runtime = int(parts[1])
            elif parts[0] == "FabricStats":
                # "FabricStats key value key value ..."
                stats = dict(zip(parts[1::2], parts[2::2]))
    return {
        "started": started,
        "finished": len(small) + len(large),
        "n_small": len(small),
        "n_large": len(large),
        "fct_all_us": mean(small + large),
        "fct_small_us": mean(small),
        "fct_large_us": mean(large),
        "uplink_maxmean": float(stats.get("uplink_maxmean_avg", "nan")),
        "chosen_path_metric": float(stats.get("avg_chosen_path_metric", "nan")),
        "avg_path_metric": float(stats.get("avg_path_metric", "nan")),
        # Older results (before --cores/--loadbase existed) used 12 cores and server load.
        "cores": int(stats.get("cores", 12)),
        "loadbase": stats.get("loadbase", "server"),
        "runtime_s": runtime,
    }


def collect_rows(results_dir):
    """Parse every finished run in <results_dir>/raw into one dict per run."""
    raw_dir = os.path.join(results_dir, "raw")
    rows = []
    for name in sorted(os.listdir(raw_dir)):
        if not name.endswith(".txt"):
            continue
        scheme, workload, load = name[:-4].split("_")
        row = {"scheme": scheme, "workload": workload, "load": float(load)}
        row.update(parse_run(os.path.join(raw_dir, name)))
        if row["runtime_s"] is None:
            continue  # incomplete run
        rows.append(row)

    rows.sort(key=lambda r: (WORKLOADS.index(r["workload"]), r["load"], SCHEMES.index(r["scheme"])))
    return rows


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    rows = collect_rows(results_dir)
    out_csv = os.path.join(results_dir, "summary.csv")
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} runs to {out_csv}\n")

    # Side-by-side table: average FCT (us), ECMP vs CONGA.
    by_key = defaultdict(dict)
    for r in rows:
        by_key[(r["workload"], r["load"])][r["scheme"]] = r

    def fmt(x):
        return f"{x:9.1f}" if x == x else "      n/a"  # x != x for NaN

    for wl in WORKLOADS:
        keys = sorted(k for k in by_key if k[0] == wl)
        if not keys:
            continue
        print(f"== {wl} ==   avg FCT in us (ECMP / CONGA), flows finished/started")
        print(" load |      all: ECMP     CONGA |    small: ECMP     CONGA |    large: ECMP     CONGA | finished/started")
        for k in keys:
            e, c = by_key[k].get("ecmp"), by_key[k].get("conga")
            cells = []
            for metric in ("fct_all_us", "fct_small_us", "fct_large_us"):
                ev = e[metric] if e else float("nan")
                cv = c[metric] if c else float("nan")
                cells.append(f"{fmt(ev)} {fmt(cv)}")
            ref = e or c
            print(f" {int(round(k[1] * 100)):3d}% | " + " | ".join(f"       {cell}" for cell in cells)
                  + f" | {ref['finished']}/{ref['started']}")
        print()


if __name__ == "__main__":
    main()
