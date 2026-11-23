#!/bin/bash
#
# Experiment driver for the elastic data-centre testbed (--expt=4).
#
# Sweeps are deliberately one-factor-at-a-time against a fixed base config,
# because the result of interest is a transient (how the fleet and latency
# respond to one surge), not a steady-state average.
#
#   ./scripts/run_autoscale.sh scaler    compare none / threshold / target
#   ./scripts/run_autoscale.sh lb        compare server LB policies
#   ./scripts/run_autoscale.sh boot      sweep boot delay (the headline result)
#   ./scripts/run_autoscale.sh control   sweep control interval
#   ./scripts/run_autoscale.sh path      ECMP vs CONGA fabric load balancing
#   ./scripts/run_autoscale.sh all       everything
#
set -u

HTSIM=./htsim
OUT=${OUT:-results_autoscale}
BUCKET=${BUCKET:-2}

# p99 on a heavy-tailed workload is noisy, so every config runs over several
# seeds and is reported as a median with spread. One seed cannot separate
# two policies; see scripts/autoscale_aggregate.py.
SEEDS=${SEEDS:-"1729 2718 3141 4669 5772"}

# Base configuration. Time is compressed (see the header of
# test_autoscale_testbed.cpp): read "boot=10ms" as "one VM boot".
BASE="--expt=4 --leaves=8 --cores=4 --backends=8 --clients=8 \
--duration=0.30 --drain=0.05 --surgestart=0.10 --surgeend=0.20 \
--baseload=24 --peakload=72 --init_servers=4 \
--bootdelay=10 --control=2 --scale_target=0.60 --step_up=4 \
--cooldown_down=20 --flowdist=enterprise --endhost=dctcp"

mkdir -p "$OUT/logs" "$OUT/csv"

run_one() {
    local tag="$1"; shift
    local seed

    for seed in $SEEDS; do
        local log="$OUT/logs/$tag-s$seed.log"

        if [ -s "$log" ] && [ "${FORCE:-0}" != "1" ]; then
            echo "  [skip] $tag seed=$seed"
        else
            echo "  [run ] $tag seed=$seed"
            $HTSIM $BASE --rngseed=$seed "$@" > "$log" 2> "$OUT/logs/$tag-s$seed.err"
            if [ $? -ne 0 ]; then
                echo "  [FAIL] $tag seed=$seed -- see $OUT/logs/$tag-s$seed.err"
                continue
            fi
        fi

        # One machine-readable row per seed, for the aggregator.
        python3 -I scripts/autoscale_analyze.py "$log" \
            --bucket-ms "$BUCKET" --row "$tag" --seed "$seed" >> "$OUT/runs.csv"
    done

    # Keep the full time series of the first seed only: that is what gets
    # plotted, and the rest would just be noise on the same chart.
    local first=$(echo $SEEDS | awk '{print $1}')
    python3 -I scripts/autoscale_analyze.py "$OUT/logs/$tag-s$first.log" \
        --bucket-ms "$BUCKET" --csv-prefix "$OUT/csv/$tag" > /dev/null
}

sweep_scaler() {
    echo "== scaler policy =="
    run_one "scaler-none"      --scaler=none      --lb_server=p2c
    run_one "scaler-threshold" --scaler=threshold --lb_server=p2c --scale_up=0.70 --scale_down=0.30
    run_one "scaler-target"    --scaler=target    --lb_server=p2c
}

sweep_lb() {
    echo "== server load balancer =="
    for lb in rr random least p2c; do
        run_one "lb-$lb" --scaler=target --lb_server=$lb
    done
}

sweep_boot() {
    echo "== boot delay (reaction lag) =="
    for b in 2 5 10 20 40; do
        run_one "boot-${b}ms" --scaler=target --lb_server=p2c --bootdelay=$b
    done
}

sweep_control() {
    echo "== control interval =="
    for c in 1 2 5 10; do
        run_one "control-${c}ms" --scaler=target --lb_server=p2c --control=$c
    done
}

sweep_path() {
    echo "== fabric path load balancing =="
    for p in ecmp conga; do
        run_one "path-$p" --scaler=target --lb_server=p2c --lb_path=$p
    done
}

: > "$OUT/runs.csv"

case "${1:-all}" in
    scaler)  sweep_scaler ;;
    lb)      sweep_lb ;;
    boot)    sweep_boot ;;
    control) sweep_control ;;
    path)    sweep_path ;;
    all)     sweep_scaler; sweep_lb; sweep_boot; sweep_control; sweep_path ;;
    *)       echo "usage: $0 {scaler|lb|boot|control|path|all}"; exit 1 ;;
esac

echo
echo "=============================== RESULTS ==============================="
python3 -I scripts/autoscale_aggregate.py "$OUT/runs.csv" | tee "$OUT/summary.txt"
echo
echo "Per-seed rows : $OUT/runs.csv"
echo "Time series   : $OUT/csv/ (…-latency.csv, …-fleet.csv, first seed)"
