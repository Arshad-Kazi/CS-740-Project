#!/bin/bash
#
# Experiment driver for the GPU warm-up testbed (--expt=5).
#
#   ./scripts/run_gpu_warmup.sh source     storage vs peer vs rack-local peer
#   ./scripts/run_gpu_warmup.sh storage    sweep storage read rate (HDD..NVMe)
#   ./scripts/run_gpu_warmup.sh state      sweep model size
#   ./scripts/run_gpu_warmup.sh batch      sweep scale-up granularity
#   ./scripts/run_gpu_warmup.sh place      packed vs round-robin placement
#   ./scripts/run_gpu_warmup.sh parallel   sweep number of storage nodes
#   ./scripts/run_gpu_warmup.sh all
#
set -u

HTSIM=./htsim
OUT=${OUT:-results_gpu}
BUCKET=${BUCKET:-2}
SEEDS=${SEEDS:-"1729 2718 3141"}

BASE="--expt=5 --racks=8 --cores=4 --gpus=8 --clients=8 \
--duration=0.40 --drain=0.10 --surgestart=0.10 --surgeend=0.25 \
--baseload=15 --peakload=45 --init_nodes=6 \
--compute_ratio=0.4 --statesize=50 --storage_nodes=4 --storage_read=4 \
--nvlink=100 --control=2 --scale_target=0.60 --step_up=4 --cooldown_down=30 \
--flowdist=uniform --flowsize=100000 --endhost=dctcp"

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
            $HTSIM $BASE --rngseed=$seed "$@" > "$log" 2> "$OUT/logs/$tag-s$seed.err" || {
                echo "  [FAIL] $tag seed=$seed"; continue; }
        fi
        python3 -I scripts/gpu_warmup_analyze.py "$log" \
            --bucket-ms "$BUCKET" --row "$tag" --seed "$seed" >> "$OUT/runs.csv"
    done
    local first=$(echo $SEEDS | awk '{print $1}')
    python3 -I scripts/gpu_warmup_analyze.py "$OUT/logs/$tag-s$first.log" \
        --bucket-ms "$BUCKET" --csv-prefix "$OUT/csv/$tag" > /dev/null
}

sweep_source() {
    echo "== warm-up source =="
    run_one "src-storage"   --warmup=storage
    run_one "src-peer"      --warmup=peer
    run_one "src-racklocal" --warmup=peer-racklocal
}

# Device tiers: HDD array / SATA SSD / fast NVMe / beyond the NIC.
sweep_storage() {
    echo "== storage read rate =="
    for r in 1.2 4 10 24; do
        run_one "stor-${r}G" --warmup=storage --storage_read=$r
    done
}

sweep_state() {
    echo "== model size =="
    for s in 10 25 50 100; do
        run_one "state-${s}MB-storage" --warmup=storage --statesize=$s
        run_one "state-${s}MB-peer"    --warmup=peer    --statesize=$s
    done
}

# Trickling should beat batching: fewer concurrent pulls finish sooner.
sweep_batch() {
    echo "== scale-up granularity =="
    for k in 1 2 4 8; do
        run_one "batch-${k}-storage" --warmup=storage --step_up=$k
        run_one "batch-${k}-peer"    --warmup=peer    --step_up=$k
    done
}

sweep_place() {
    echo "== placement =="
    for p in packed roundrobin; do
        run_one "place-$p-storage" --warmup=storage --placement=$p
        run_one "place-$p-peer"    --warmup=peer    --placement=$p
    done
}

sweep_parallel() {
    echo "== storage parallelism =="
    for n in 1 2 4 8; do
        run_one "par-${n}node" --warmup=storage --storage_nodes=$n
    done
}

: > "$OUT/runs.csv"

case "${1:-all}" in
    source)   sweep_source ;;
    storage)  sweep_storage ;;
    state)    sweep_state ;;
    batch)    sweep_batch ;;
    place)    sweep_place ;;
    parallel) sweep_parallel ;;
    all)      sweep_source; sweep_storage; sweep_state; sweep_batch
              sweep_place; sweep_parallel ;;
    *)        echo "usage: $0 {source|storage|state|batch|place|parallel|all}"; exit 1 ;;
esac

echo
echo "=============================== RESULTS ==============================="
python3 -I scripts/gpu_warmup_aggregate.py "$OUT/runs.csv" | tee "$OUT/summary.txt"
echo
echo "Per-seed rows : $OUT/runs.csv"
echo "Time series   : $OUT/csv/"
