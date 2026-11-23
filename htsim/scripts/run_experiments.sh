#!/bin/bash
#
# Runs every (scheme x workload x load) experiment for Part 3 in parallel.
#   schemes:   ecmp conga
#   workloads: uniform pareto enterprise datamining
#   loads:     10% .. 100%
# => 80 simulations. ECMP and CONGA use the same --rngseed, so each pair of
#    runs sees exactly the same flows (sizes, arrival times, src/dst).
#
# Usage (from the htsim directory):
#   scripts/run_experiments.sh [results_dir]          default: results
#
# Optional environment overrides:
#   JOBS=8          parallel simulations (default: number of CPU cores)
#   DUR=0.02        seconds of flow arrivals (uniform, pareto, enterprise)
#   DRAIN=0.02      extra seconds for in-flight flows to finish
#   DUR_DM=0.2      seconds of flow arrivals for datamining (few, huge flows)
#   DRAIN_DM=0.1
#   SEED=1729
#   LOADS="1.0 0.9 ... 0.1"   WORKLOADS="..."   SCHEMES="..."
#   EXTRA_ARGS="..."   extra htsim options for every run, for example the
#                      2:1 oversubscribed experiment (4 cores, load measured
#                      against the core capacity, as in the CONGA paper):
#     EXTRA_ARGS="--cores=4 --loadbase=core" scripts/run_experiments.sh results_4core
#
# Finished runs are skipped, so the script can be re-run after an interruption.
# Output: <results_dir>/raw/<scheme>_<workload>_<load>.txt, containing
#   F <size_bytes> <start_us> <fct_us>   one line per completed flow
#   FabricStats ...                      load-balance statistics
#   STARTED <n>                          number of flows started
#   DONE <runtime_s>                     marker written at the end
set -eu

cd "$(dirname "$0")/.."    # the htsim directory

export OUT=${1:-results}
export DUR=${DUR:-0.02}
export DRAIN=${DRAIN:-0.02}
export DUR_DM=${DUR_DM:-0.2}
export DRAIN_DM=${DRAIN_DM:-0.1}
export SEED=${SEED:-1729}
export EXTRA_ARGS=${EXTRA_ARGS:-}
JOBS=${JOBS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)}
LOADS=${LOADS:-"1.0 0.9 0.8 0.7 0.6 0.5 0.4 0.3 0.2 0.1"}
WORKLOADS=${WORKLOADS:-"datamining enterprise pareto uniform"}
SCHEMES=${SCHEMES:-"ecmp conga"}

make -s
mkdir -p "$OUT/raw" "$OUT/logs"

# Only one copy of this script may write into a results folder at a time.
if ! mkdir "$OUT/.lock" 2>/dev/null; then
    echo "ERROR: another run_experiments.sh is already writing to $OUT."
    echo "       If no other copy is running, delete the folder $OUT/.lock and try again."
    exit 1
fi
trap 'rmdir "$OUT/.lock" 2>/dev/null' EXIT
rm -f "$OUT"/raw/*.tmp*      # leftovers from an interrupted run

run_one() {
    local lb=$1 wl=$2 load=$3 d dr
    if [ "$wl" = "datamining" ]; then d=$DUR_DM; dr=$DRAIN_DM; else d=$DUR; dr=$DRAIN; fi

    local name="${lb}_${wl}_${load}"
    local out="$OUT/raw/$name.txt"
    if [ -f "$out" ] && grep -q '^STARTED' "$out" && grep -q '^DONE' "$out"; then
        echo "[skip] $name (already done)"
        return 0
    fi

    local t0=$(date +%s)
    local tmp="$out.tmp.$$"      # unique per job, so parallel jobs never share a file
    # Keep only what the analysis needs: stdout is ~100 bytes per flow otherwise.
    # Flow-start lines look like "src<N> <start> ...", RTO lines "src<N> at ...".
    ./htsim --expt=2 --lb="$lb" --flowdist="$wl" --utilization="$load" \
            --duration="$d" --drain="$dr" --rngseed="$SEED" $EXTRA_ARGS \
            --logfile="$OUT/logs/$name" 2> "$OUT/logs/$name.err" \
      | awk '/^src[0-9]+ [0-9]/ { started++ }
             /^Flow /           { print "F", $5, $7, $11 }
             /^FabricStats /    { print }
             END                { print "STARTED", started + 0 }' > "$tmp"
    local status=${PIPESTATUS[0]}
    rm -f "$OUT/logs/$name.trace" "$OUT/logs/$name.id"

    if [ "$status" -ne 0 ] || ! grep -q '^STARTED' "$tmp"; then
        echo "[FAIL] $name (exit $status, see $OUT/logs/$name.err)"
        rm -f "$tmp"
        return 0
    fi
    local t=$(( $(date +%s) - t0 ))
    echo "DONE $t" >> "$tmp"
    mv "$tmp" "$out"
    echo "[done] $name in ${t}s ($(grep -c '^F ' "$out") flows finished)"
}
export -f run_one

# Most expensive jobs (high load, datamining) first, to shorten the total time.
jobs=""
for load in $LOADS; do
    for wl in $WORKLOADS; do
        for lb in $SCHEMES; do
            jobs="$jobs$lb $wl $load"$'\n'
        done
    done
done

n=$(printf "%s" "$jobs" | grep -c .)
echo "Running $n simulations, $JOBS at a time. Results in $OUT/raw/"
echo "  durations: ${DUR}s (+${DRAIN}s drain), datamining ${DUR_DM}s (+${DRAIN_DM}s drain), seed $SEED"
[ -n "$EXTRA_ARGS" ] && echo "  extra htsim options: $EXTRA_ARGS"
start=$(date +%s)

printf "%s" "$jobs" | xargs -P "$JOBS" -L 1 bash -c 'run_one "$@"' _

echo "All done in $(( $(date +%s) - start ))s. Next: python3 scripts/summarize.py $OUT"
