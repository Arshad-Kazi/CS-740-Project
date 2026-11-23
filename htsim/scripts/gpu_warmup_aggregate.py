#!/usr/bin/env python3
"""Aggregate per-seed rows from run_gpu_warmup.sh into one row per config."""
import argparse, csv, math
from collections import OrderedDict

COLS = ["tag", "seed", "base_p99_us", "degradation_ms", "warm_p50_ms",
        "warm_p99_ms", "warm_max_ms", "warm_done", "warm_stuck",
        "storage_mb", "local_peer_mb", "remote_peer_mb", "last_ready_ms",
        "peak_fleet", "node_seconds"]


def median(xs):
    xs = sorted(x for x in xs if not math.isnan(x))
    if not xs:
        return float("nan")
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runs_csv")
    args = ap.parse_args()

    groups = OrderedDict()
    with open(args.runs_csv) as fh:
        for row in csv.reader(fh):
            if len(row) < len(COLS):
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

    print("%-22s %3s %10s %9s %8s %8s %8s %7s %6s" %
          ("config", "n", "degrade_ms", "warm_p50", "warm_max",
           "stor_MB", "peer_MB", "fleet", "node_s"))
    print("-" * 92)
    for tag, rows in groups.items():
        degr = num(rows, "degradation_ms")
        peer_mb = [a + b for a, b in zip(num(rows, "local_peer_mb"),
                                         num(rows, "remote_peer_mb"))]
        stuck = num(rows, "warm_stuck")
        flag = "  STUCK" if median(stuck) > 0 else ""
        print("%-22s %3d %10.0f %9.1f %8.1f %8.0f %8.0f %7.0f %6.2f%s" %
              (tag, len(rows), median(degr), median(num(rows, "warm_p50_ms")),
               median(num(rows, "warm_max_ms")), median(num(rows, "storage_mb")),
               median(peer_mb), median(num(rows, "peak_fleet")),
               median(num(rows, "node_seconds")), flag))
    print()
    print("degrade_ms = integral of relative p99 FCT inflation after the surge")
    print("             (lower is better; already includes any donor cost).")
    print("node_s     = node-seconds consumed: the compute bill for the scaling.")


if __name__ == "__main__":
    main()
