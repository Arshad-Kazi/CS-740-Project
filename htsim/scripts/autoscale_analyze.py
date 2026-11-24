#!/usr/bin/env python3
"""
Analyse an autoscale_testbed (--expt=4) run.

Reads htsim stdout and produces two time series plus a one-line summary:

  fleet      from "AutoScale" ticks: active / booting servers, mean utilisation
  latency    from "Flow" completion lines, bucketed by flow END time:
             p50 / p99 flow completion time, and completions per bucket

The headline numbers are about the controller's transient, not its steady
state: how far p99 FCT rises after the surge begins, and how long it takes
to come back. Those are what the boot delay and control interval trade off.

Usage:
  autoscale_analyze.py RUN.log [--bucket-ms 2] [--csv-prefix out/run]
"""
import argparse
import re
import sys

FLOW_RE = re.compile(
    r"^Flow\s+(\S+)\s+(\d+)\s+size\s+(\d+)\s+start\s+(\d+)\s+end\s+(\d+)\s+fct\s+([\d.]+)"
)
SCALE_RE = re.compile(
    r"^AutoScale\s+t\s+([\d.]+)\s+active\s+(\d+)\s+booting\s+(\d+)"
    r"\s+mean_util\s+([\d.]+)\s+max_util\s+([\d.]+)\s+action\s+(\S+)\s+delta\s+(\d+)"
)
STATS_RE = re.compile(r"^RunStats\s+(.*)$")


def percentile(xs, q):
    """Nearest-rank percentile; xs must be sorted and non-empty."""
    if not xs:
        return 0.0
    k = max(0, min(len(xs) - 1, int(round(q / 100.0 * len(xs) + 0.5)) - 1))
    return xs[k]


def parse(path):
    flows, ticks, runstats = [], [], None
    with open(path, errors="replace") as fh:
        for line in fh:
            m = FLOW_RE.match(line)
            if m:
                name, _fid, size, start, end, fct = m.groups()
                if name.startswith("transfer") or name.startswith("wt-") or name.startswith("warm"):
                    continue
                flows.append({
                    "gen": "surge" if name.startswith("surge") else "base",
                    "size": int(size),
                    "start_us": float(start),
                    "end_us": float(end),
                    "fct_us": float(fct),
                })
                continue
            m = SCALE_RE.match(line)
            if m:
                t, active, booting, mean_u, max_u, action, delta = m.groups()
                ticks.append({
                    "t_us": float(t), "active": int(active), "booting": int(booting),
                    "mean_util": float(mean_u), "max_util": float(max_u),
                    "action": action, "delta": int(delta),
                })
                continue
            m = STATS_RE.match(line)
            if m:
                runstats = m.group(1)
    return flows, ticks, runstats


def bucket_latency(flows, bucket_us):
    """Group completed flows into time buckets by when they finished."""
    buckets = {}
    for f in flows:
        b = int(f["end_us"] // bucket_us)
        buckets.setdefault(b, []).append(f["fct_us"])
    rows = []
    for b in sorted(buckets):
        fcts = sorted(buckets[b])
        rows.append({
            "t_ms": b * bucket_us / 1000.0,
            "n": len(fcts),
            "p50_us": percentile(fcts, 50),
            "p99_us": percentile(fcts, 99),
            "mean_us": sum(fcts) / len(fcts),
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--bucket-ms", type=float, default=2.0)
    ap.add_argument("--csv-prefix", default=None)
    ap.add_argument("--baseline-ms", type=float, default=None,
                    help="end of the pre-surge window used as the baseline "
                         "(default: the surge start inferred from the fleet trace)")
    ap.add_argument("--row", default=None,
                    help="emit one CSV row tagged with this name instead of a "
                         "human-readable summary (for aggregating over seeds)")
    ap.add_argument("--seed", default="", help="seed label for --row")
    args = ap.parse_args()

    flows, ticks, runstats = parse(args.log)
    if not flows:
        sys.exit("no completed flows found in %s" % args.log)

    bucket_us = args.bucket_ms * 1000.0
    lat = bucket_latency(flows, bucket_us)

    # Infer the surge window from the generator prefix on each flow.
    surge = [f for f in flows if f["gen"] == "surge"]
    surge_start_ms = min(f["start_us"] for f in surge) / 1000.0 if surge else None

    baseline_end_ms = args.baseline_ms
    if baseline_end_ms is None:
        baseline_end_ms = surge_start_ms if surge_start_ms is not None else 0.0

    base_rows = [r for r in lat if r["t_ms"] < baseline_end_ms]
    post_rows = [r for r in lat if r["t_ms"] >= baseline_end_ms]

    base_p99 = (sum(r["p99_us"] for r in base_rows) / len(base_rows)) if base_rows else 0.0
    peak_p99 = max((r["p99_us"] for r in post_rows), default=0.0)

    # Recovery: first bucket after the peak where p99 falls back within 20%
    # of the pre-surge baseline and stays there.
    recover_ms = None
    if base_p99 > 0 and post_rows:
        peak_t = max(post_rows, key=lambda r: r["p99_us"])["t_ms"]
        tail = [r for r in post_rows if r["t_ms"] >= peak_t]
        for i, r in enumerate(tail):
            if r["p99_us"] <= base_p99 * 1.2 and all(
                    x["p99_us"] <= base_p99 * 1.5 for x in tail[i:i + 3]):
                recover_ms = r["t_ms"] - peak_t
                break

    # Degradation integral: the area under relative p99 inflation over the
    # whole post-surge window, in ms. Far more robust than a threshold
    # crossing, because one freak flow moves it only in proportion to how
    # long it actually inflated the tail. This is the headline metric.
    degradation_ms = 0.0
    if base_p99 > 0:
        for r in post_rows:
            degradation_ms += max(0.0, r["p99_us"] / base_p99 - 1.0) * args.bucket_ms

    # Cost: server-seconds, pulled out of the RunStats line.
    server_seconds = float("nan")
    if runstats:
        toks = runstats.split()
        if "server_seconds" in toks:
            server_seconds = float(toks[toks.index("server_seconds") + 1])

    if args.row:
        print("%s,%s,%.3f,%.3f,%.4f,%.3f,%s,%d,%d,%.4f" % (
            args.row, args.seed, base_p99, peak_p99,
            peak_p99 / base_p99 if base_p99 else 0.0,
            degradation_ms,
            ("%.1f" % recover_ms) if recover_ms is not None else "",
            max((t["active"] for t in ticks), default=0),
            ticks[-1]["active"] if ticks else 0,
            server_seconds))
        return

    if args.csv_prefix:
        with open(args.csv_prefix + "-latency.csv", "w") as fh:
            fh.write("t_ms,completions,p50_us,p99_us,mean_us\n")
            for r in lat:
                fh.write("%.3f,%d,%.3f,%.3f,%.3f\n" %
                         (r["t_ms"], r["n"], r["p50_us"], r["p99_us"], r["mean_us"]))
        with open(args.csv_prefix + "-fleet.csv", "w") as fh:
            fh.write("t_ms,active,booting,mean_util,max_util,action,delta\n")
            for t in ticks:
                fh.write("%.3f,%d,%d,%.4f,%.4f,%s,%d\n" %
                         (t["t_us"] / 1000.0, t["active"], t["booting"],
                          t["mean_util"], t["max_util"], t["action"], t["delta"]))
        print("wrote %s-latency.csv and %s-fleet.csv" %
              (args.csv_prefix, args.csv_prefix))

    peak_active = max((t["active"] for t in ticks), default=0)
    final_active = ticks[-1]["active"] if ticks else 0
    start_active = ticks[0]["active"] if ticks else 0

    print("flows_completed   %d (base %d, surge %d)" %
          (len(flows), len(flows) - len(surge), len(surge)))
    if surge_start_ms is not None:
        print("surge_start_ms    %.1f" % surge_start_ms)
    print("fleet             start %d -> peak %d -> final %d"
          % (start_active, peak_active, final_active))
    print("p99_fct_base_us   %.1f" % base_p99)
    print("p99_fct_peak_us   %.1f  (%.1fx baseline)" %
          (peak_p99, peak_p99 / base_p99 if base_p99 else 0))
    print("p99_recovery_ms   %s" %
          ("%.1f" % recover_ms if recover_ms is not None else "not reached"))
    print("degradation_ms    %.1f  (integral of relative p99 inflation)" % degradation_ms)
    if runstats:
        print("runstats          %s" % runstats)


if __name__ == "__main__":
    main()
