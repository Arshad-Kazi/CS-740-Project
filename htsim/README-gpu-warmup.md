# GPU Warm-Up Testbed (htsim `--expt=5`)

Packet-level testbed for **peer-assisted GPU warm-up vs central storage**.

A GPU inference node cannot serve until the model weights are resident.
Getting them there is a network transfer that competes with live serving
traffic, so "time to add capacity" is a *measured outcome*, not a parameter.

---

## 1. What it models

```
               [ core 0 ] ... [ core C-1 ]            40G mesh
                  /  |  \        /  |  \
      [ rack 0 ] ... [ rack R-1 ]   [ storage leaf ]
        /     \                          |
    GPUs    clients                storage nodes
   (elastic) (users)              (read-rate limited)
```

Inside a rack the GPUs additionally share an NVSwitch-class interconnect
(default 100 G, 10x the NIC) that never touches the leaf or the fabric.

### Three ways to warm a node, with different costs

| Source | Path | Cost |
|---|---|---|
| **Central storage** | `[read rate] → [storage NIC] → fabric → node` | Slow; costs the serving fleet **nothing** |
| **Same-rack peer** | `[NVLink out] → [NVLink in] → node` | Fast; bypasses leaf, fabric *and* the donor's NIC — nearly **free** |
| **Cross-rack peer** | `[donor NIC] → fabric → node` | Fast, but shares the **donor's NIC** with its own serving traffic |

Note what is deliberately *absent* from the peer paths: the donor's compute
queue. Reading weights out of HBM is modelled as DMA that bypasses the GPU's
serving pipeline, so a donor loses bandwidth but not compute.

### Capacity is compute, not bandwidth

Each node has a `DreQueue` at `--compute_ratio × 10 Gbps` (default 4 Gbps)
ahead of its NIC. Since compute is below line rate, **compute is what
saturates**, and its DRE is what the autoscaler reads. "Node at 100%
capacity" is literally true about compute.

### ⚠ Timescale is compressed

A 140 GB model is 93 million packets — far beyond packet-level simulation.
Defaults use a 50 MB "model" at 4 Gbps storage, which preserves the ratios
that matter:

| Ratio | Real (H100-class) | This testbed |
|---|---|---|
| storage read ÷ node NIC | 160 ÷ 400 Gbps = 0.4 | 4 ÷ 10 Gbps = 0.4 |
| intra-rack ÷ NIC | ~35x | 10x |
| warm-up ÷ surge duration | ~0.7 | ~0.67 |

---

## 2. Files

| File | Role |
|---|---|
| `test_gpu_warmup_testbed.cpp` | Topology, the three route types, arguments |
| `gpu-fleet.{h,cpp}` | Nodes, racks, weight residency, round-robin dispatch |
| `warmup-policy.h` | **Pluggable source-selection policies** |
| `warmup-manager.{h,cpp}` | Turns PullTasks into real TCP flows; detects completion |
| `elastic-fleet.h` | Interface shared with expt 4, so one autoscaler drives both |
| `scripts/run_gpu_warmup.sh` | Multi-seed sweeps |
| `scripts/gpu_warmup_analyze.py` | Per-run metrics |
| `scripts/gpu_warmup_aggregate.py` | Median across seeds |

---

## 3. Adding a scheduling strategy

This is the point of the testbed. Subclass `WarmupPolicy`; the simulator
never changes:

```cpp
struct PullTask { int source; uint64_t offset, length; };  // source -1 = storage

class MyPolicy : public WarmupPolicy {
    const char *name() const { return "mine"; }

    std::vector<PullTask> plan(uint32_t node, FleetView &view) {
        // view: hasWeights(), rackOf(), loadOf(), donationsInFlight(),
        //       residentNodes(), stateBytes()
    }

    // optional: re-plan as transfers land (chunked / tree strategies)
    std::vector<PullTask> onTaskComplete(uint32_t node, const PullTask &done,
                                         FleetView &view);
};
```

Register it in `makeWarmupPolicy()` and select with `--warmup=mine`.
Returning an empty vector from `plan()` means "wait" — the manager retries.

Shipped: `storage`, `peer`, `peer-racklocal`.
Natural next ones: chunked multi-source, binomial tree, pipelined ring.

---

## 4. Running

```bash
make
./htsim --expt=5 --warmup=peer
./scripts/run_gpu_warmup.sh all       # ~4 min, 3 seeds
./scripts/run_gpu_warmup.sh source    # just storage vs peer
```

### Key options

| Flag | Default | Meaning |
|---|---|---|
| `--warmup` | storage | `storage` / `peer` / `peer-racklocal` |
| `--placement` | packed | `packed` / `roundrobin` |
| `--racks --gpus` | 8, 8 | 64 GPU slots |
| `--init_nodes` | 6 | Nodes already warm at t=0 |
| `--compute_ratio` | 0.4 | Compute ÷ NIC. **Below 1 = compute-bound** |
| `--statesize` | 50 | Model weights, MB |
| `--storage_nodes --storage_read` | 4, 4 | Storage tier |
| `--nvlink` | 100 | Intra-rack Gbps |
| `--step_up` | 4 | Nodes launched per scale-up |
| `--baseload --peakload` | 15, 45 | Gbps offered |

**Do not use `--flowdist=enterprise`.** Its sizes span four orders of
magnitude (p50 2 KB, max 27 MB); one 27 MB flow pins a GPU for ~54 ms — 27
control intervals — so the load signal becomes noise. It also silently
under-delivers: htsim declares `_avgFlowSize=215000` but the CDF's true mean
is ~124 KB, so the arrival rate is set ~42% too low
([workloads.cpp:20](workloads.cpp)).

---

## 5. Results (3 seeds)

| config | degrade_ms | warm_p50 | storage_MB | peer_MB | fleet |
|---|---|---|---|---|---|
| `src-storage` | **54,188** | 100.0 ms | 650 | 0 | 12 |
| `src-peer` | **172** | 10.9 ms | 0 | 1250 | 24 |
| `src-racklocal` | 5,247 | 105.4 ms | 450 | 400 | 17 |
| `stor-1.2G` (HDD) | 134,235 **STUCK** | 333 ms | 150 | 0 | 7 |
| `stor-4G` (SSD) | 54,188 | 100.0 ms | 650 | 0 | 12 |
| `stor-10G` | 10,315 | 41.1 ms | 1100 | 0 | 22 |
| `stor-24G` (NVMe) | 10,652 | 42.3 ms | 1100 | 0 | 22 |
| `batch-1-storage` | 63,414 | 101.4 ms | 650 | 0 | 11 |
| `batch-4-storage` | 54,188 | 100.0 ms | 650 | 0 | 12 |
| `batch-8-storage` | 54,188 | 100.0 ms | 650 | 0 | 12 |
| `par-1node` | 134,107 **STUCK** | 297 ms | 150 | 0 | 7 |
| `par-4node` | 54,188 | 100.0 ms | 650 | 0 | 12 |
| `par-8node` | 52,548 | 100.0 ms | 750 | 0 | 15 |

`degrade_ms` = integral of relative p99 FCT inflation after the surge.
Lower is better; it already contains any cost the donors paid.

### What these support

1. **Peer-assist is worth ~300x here**, and removes storage from the critical
   path entirely (650 MB → 0). Warm-up p50 drops 100 ms → 11 ms.

2. **Storage speed saturates at the NIC.** 10 G and 24 G are identical
   (10,315 vs 10,652). Past roughly link speed, faster storage buys nothing —
   the network becomes the bottleneck. Below it, the cliff is brutal: at
   1.2 G some nodes **never finish warming** inside the run.

3. **Cross-rack peering matters more than it looks.** `peer-racklocal` is 30x
   *worse* than unrestricted `peer`, because every newly-opened rack has no
   local donor and must re-seed from storage (450 MB vs 0). Rack locality is
   a good preference, a bad constraint.

4. **Batching is free up to the storage parallelism, and useless past it.**
   Trickling (`batch-1`) is *worse* than `batch-4`, and `batch-8` is no better
   than `batch-4`. The same number appears from the other side in the
   parallelism sweep: 8 storage nodes barely beat 4 when only 4 pulls are
   ever in flight. **The useful batch size and the useful storage
   parallelism are the same number.**

### What these do NOT support

- **`place-packed` vs `place-roundrobin` is unresolved** (172 vs 104 for peer,
  53,667 vs 54,188 for storage). Spreads overlap; do not rank them.
- **The node-seconds column is not a fair cost comparison.** Storage runs use
  less compute (4.44 vs 8.56) only because they *could not acquire* the nodes
  they wanted, not because they were thriftier.

---

## 6. Known limitations

* **The initial fleet starts warm**, so in `peer` mode storage is never used
  at all. A genuinely cold cluster would have to seed from storage first, and
  the seeding rate would dominate. Worth a `--cold_start` flag.
* **No per-request service cost.** Compute drains by bytes, so a 1-byte
  response is free. Wrong for workloads where small responses are expensive.
* **No tensor parallelism.** A model needing 4 GPUs would make scale-up
  granularity a *group* and add all-reduce traffic between them.
* **Donors pay bandwidth, not compute**, by construction. HBM read bandwidth
  contention with inference is not modelled.
* **Scale-down is abrupt** and discards weights, so re-scaling pays full
  warm-up. A keep-warm pool is a separate experiment.
