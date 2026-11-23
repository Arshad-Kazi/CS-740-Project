#!/usr/bin/env python3
"""
Analyse a gpu_warmup_testbed (--expt=5) run.

Reports two things that have to be read together:

  capacity   how fast the fleet actually grew -- warm-up durations, time to
             first and last ready node, and where the bytes came from
  latency    what users saw -- p99 flow completion time bucketed by flow end
             time, and the integral of its inflation after the surge

Peer-assisted warm-up buys capacity sooner but taxes the donors' NICs, so a
verdict based on warm-up time alone would be misleading. degradation_ms is
the bottom line: it already contains whatever the donation cost.

Usage:
  gpu_warmup_analyze.py RUN.log [--bucket-ms 2] [--row TAG --seed N]
"""
import argparse
import re
import sys

FLOW_RE = re.compile(
    r"^Flow\s+(\S+)\s+(\d+)\s+size\s+(\d+)\s+start\s+(\d+)\s+end\s+(\d+)\s+fct\s+([\d.]+)")
SCALE_RE = re.compile(
    r"^AutoScale\s+t\s+([\d.]+)\s+active\s+(\d+)\s+booting\s+(\d+)"
    r"\s+mean_util\s+([\d.]+)\s+max_util\s+([\d.]+)")
WARM_RE = re.compile(r"^WarmStats\s+(.*)$")


def percentile(xs, q):
    if not xs:
        return 0.0
    k = max(0, min(len(xs) - 1, int(round(q / 100.0 * len(xs) + 0.5)) - 1))
    return xs[k]


def parse(path):
    flows, ticks, warm = [], [], {}
    with open(path, errors="replace") as fh:
        for line in fh:
            m = FLOW_RE.match(line)
            if m:
                name, _fid, size, start, end, fct = m.groups()
                flows.append({
                    "warm": name.startswith("warm"),
                    "gen": "surge" if name.startswith("surge") else "base",
                    "end_us": float(end), "fct_us": float(fct),
                })
                continue
            m = SCALE_RE.match(line)
            if m:
                t, active, pending, mean_u, max_u = m.groups()
                ticks.append({"t_ms": float(t) / 1000.0, "active": int(active),
                              "pending": int(pending), "mean_util": float(mean_u),
                              "max_util": float(max_u)})
                continue
            m = WARM_RE.match(line)
            if m:
                toks = m.group(1).split()
                for i in range(0, len(toks) - 1, 2):
                    warm[toks[i]] = toks[i + 1]
    return flows, ticks, warm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--bucket-ms", type=float, default=2.0)
    ap.add_argument("--csv-prefix", default=None)
    ap.add_argument("--row", default=None)
    ap.add_argument("--seed", default="")
    args = ap.parse_args()

    flows, ticks, warm = parse(args.log)

    # Warm-up transfers are not user requests; they must not enter the
    # latency statistics even though htsim logs them the same way.
    serving = [f for f in flows if not f["warm"]]
    if not serving:
        sys.exit("no completed serving flows in %s" % args.log)

    bucket_us = args.bucket_ms * 1000.0
    buckets = {}
    for f in serving:
        buckets.setdefault(int(f["end_us"] // bucket_us), []).append(f["fct_us"])

    lat = []
    for b in sorted(buckets):
        fcts = sorted(buckets[b])
        lat.append({"t_ms": b * bucket_us / 1000.0, "n": len(fcts),
                    "p50_us": percentile(fcts, 50), "p99_us": percentile(fcts, 99)})

    surge = [f for f in serving if f["gen"] == "surge"]
    surge_start_ms = min(f["end_us"] - f["fct_us"] for f in surge) / 1000.0 if surge else 0.0

    base_rows = [r for r in lat if r["t_ms"] < surge_start_ms]
    post_rows = [r for r in lat if r["t_ms"] >= surge_start_ms]
    base_p99 = (sum(r["p99_us"] for r in base_rows) / len(base_rows)) if base_rows else 0.0
    peak_p99 = max((r["p99_us"] for r in post_rows), default=0.0)

    degradation_ms = 0.0
    if base_p99 > 0:
        for r in post_rows:
            degradation_ms += max(0.0, r["p99_us"] / base_p99 - 1.0) * args.bucket_ms

    g = lambda k, d="0": warm.get(k, d)
    peak_fleet = max((t["active"] for t in ticks), default=0)

    if args.csv_prefix:
        with open(args.csv_prefix + "-latency.csv", "w") as fh:
            fh.write("t_ms,completions,p50_us,p99_us\n")
            for r in lat:
                fh.write("%.3f,%d,%.3f,%.3f\n" % (r["t_ms"], r["n"], r["p50_us"], r["p99_us"]))
        with open(args.csv_prefix + "-fleet.csv", "w") as fh:
            fh.write("t_ms,active,warming,mean_util,max_util\n")
            for t in ticks:
                fh.write("%.3f,%d,%d,%.4f,%.4f\n" % (t["t_ms"], t["active"],
                                                     t["pending"], t["mean_util"], t["max_util"]))

    if args.row:
        print("%s,%s,%.3f,%.4f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s" % (
            args.row, args.seed, base_p99, degradation_ms,
            g("warm_p50_ms"), g("warm_p99_ms"), g("warm_max_ms"),
            g("warm_done"), g("warm_stuck"),
            g("storage_mb"), g("local_peer_mb"), g("remote_peer_mb"),
            g("last_ready_ms"), peak_fleet, g("node_seconds")))
        return

    print("serving_flows     %d (base %d, surge %d)" %
          (len(serving), len(serving) - len(surge), len(surge)))
    print("surge_start_ms    %.1f" % surge_start_ms)
    print("p99_fct_base_us   %.1f" % base_p99)
    print("p99_fct_peak_us   %.1f  (%.1fx)" %
          (peak_p99, peak_p99 / base_p99 if base_p99 else 0))
    print("degradation_ms    %.1f   <-- bottom line, includes donor cost" % degradation_ms)
    print()
    print("warm_done         %s  (stuck %s)" % (g("warm_done"), g("warm_stuck")))
    print("warm_p50/p99/max  %s / %s / %s ms" %
          (g("warm_p50_ms"), g("warm_p99_ms"), g("warm_max_ms")))
    print("first/last_ready  %s / %s ms" % (g("first_ready_ms"), g("last_ready_ms")))
    print("bytes storage/local/remote  %s / %s / %s MB" %
          (g("storage_mb"), g("local_peer_mb"), g("remote_peer_mb")))
    print("peak_fleet        %d   node_seconds %s" % (peak_fleet, g("node_seconds")))


if __name__ == "__main__":
    main()
