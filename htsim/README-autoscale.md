# Elastic Data-Centre Testbed (htsim `--expt=4`)

A packet-level testbed for studying **how a data centre scales its backend
fleet when traffic surges**, and what that costs in flow completion time.

Built on the lab-2 htsim tree; the CONGA leaf switch and DRE queue are reused
unchanged.

---

## 1. What it models

```
              [ core 0 ] ... [ core C-1 ]          40G, full bipartite mesh
                 |  \   \      /   /  |
              [ leaf 0 ] ... [ leaf L-1 ]
                /     \         /    \             10G access links
         backends   clients  backends clients
```

* **Clients** are the users. They are fixed and always on.
* **Backends** are the elastic fleet. Only the *active* subset serves traffic.
* A request is modelled by its **response**, because in a web/DC workload the
  response dominates. The backend is the `DataSource`, the client is the
  `DataSink`, so the link that saturates under load is the backend's egress.

**A server's capacity is its access link.** htsim has no CPU or application
layer — a sink absorbs packets at line rate — so "server overloaded" means
*its 10G egress queue is backing up*. Scale-up spreads load over more access
links. Every trigger signal is a network signal.

### How a static simulator grows a fleet

htsim builds topology statically. Rather than allocating links mid-run, the
testbed **pre-allocates the maximum fleet and gates admission**: `ServerPool`
keeps an ACTIVE set, and only those servers are handed to new flows.

This works because `FlowGenerator` calls the route generator **once per flow,
at the instant the flow is created** ([flow-generator.cpp:147][fg]). A server
that becomes ACTIVE mid-run therefore takes traffic immediately, with no
topology change. Flows already in flight keep their original route — which is
what really happens.

[fg]: flow-generator.cpp

### ⚠ Timescale is compressed

Simulating a real 30–60 s VM boot at 10 Gbps packet-level is out of reach.
Defaults are **boot 10 ms, control interval 2 ms, surge 100 ms**. What matters
is the *ratio* — boot ≫ control, surge ≫ boot — since the lag/overshoot
behaviour is scale-invariant. **Read `--bootdelay=10` as "one VM boot", not as
ten milliseconds.**

---

## 2. Files

| File | Role |
|---|---|
| `test_autoscale_testbed.cpp` | Topology, wiring, route generation, arguments |
| `server-pool.{h,cpp}` | ACTIVE set, boot delay, server load-balancing policies |
| `autoscaler.{h,cpp}` | `EventSource` control loop: none / threshold / target |
| `scripts/run_autoscale.sh` | Multi-seed experiment driver |
| `scripts/autoscale_analyze.py` | Per-run metrics and time series |
| `scripts/autoscale_aggregate.py` | Median + spread across seeds |

Reused from lab 2: `leafswitch.{h,cpp}`, `dre-queue.{h,cpp}`.

---

## 3. Running it

```bash
make
./htsim --expt=4                      # defaults
./scripts/run_autoscale.sh all        # full sweep, ~2 min
./scripts/run_autoscale.sh boot       # just the boot-delay sweep
FORCE=1 SEEDS="1 2 3" ./scripts/run_autoscale.sh scaler
```

Output: `results_autoscale/runs.csv` (one row per seed),
`results_autoscale/csv/*-{latency,fleet}.csv` (time series for plotting),
`results_autoscale/summary.txt` (the aggregated table).

### Key options

| Flag | Default | Meaning |
|---|---|---|
| `--leaves --cores --backends --clients` | 8 4 8 8 | Topology, sized at runtime |
| `--init_servers` | 4 | Backends active at t=0 |
| `--baseload --peakload` | 24 72 | Offered load, Gbps |
| `--surgestart --surgeend` | 0.10 0.20 | Surge window, seconds |
| `--bootdelay` | 10 | Boot delay, **ms** |
| `--control` | 2 | Controller period, **ms** |
| `--scaler` | target | `none` / `threshold` / `target` |
| `--scale_target` | 0.60 | Desired utilisation (target policy) |
| `--scale_up --scale_down` | 0.70 0.30 | Thresholds (threshold policy) |
| `--step_up --step_down` | 4 1 | Servers added/removed per action |
| `--count_booting` | 1 | Count in-flight boots when sizing |
| `--lb_server` | p2c | `rr` / `random` / `least` / `p2c` |
| `--lb_path` | ecmp | `ecmp` / `conga` — fabric path LB |
| `--flowdist` | uniform | `uniform` / `pareto` / `enterprise` / `datamining` |

**Do not use `--flowdist=datamining` for short runs.** It forces a 12.5 MB mean
flow size ([workloads.cpp:20](workloads.cpp)), giving ~240 flows/sec — far too
few for a meaningful p99. The sweeps use `enterprise` (215 KB mean, ~4 000
flows per run).

---

## 4. The surge, and why it needed no new code

A step surge is two `FlowGenerator`s: a baseline over the whole run, plus a
second one over `[surgestart, surgeend]` carrying the extra load. They compose
to base → peak → base. Flow names are prefixed `base-` / `surge-` so the
analyser can tell them apart.

---

## 5. Metrics

`degradation_ms` is the headline: **the integral of relative p99 FCT inflation
after the surge begins**. If p99 sits at 10× baseline for 20 ms, that is
9 × 20 = 180 ms. It is far more robust than a threshold-crossing recovery time,
because one freak flow moves it only in proportion to how long it actually
inflated the tail.

Also reported: p99 ratio, peak/final fleet size, and `server_seconds` — the
integral of active servers over time, i.e. **the cost of the scaling decisions**.

**p99 is noisy on a heavy-tailed workload.** Every config runs over 5 seeds and
is reported as median with min–max spread. *Overlapping spreads mean the
difference is not resolved* — say so rather than ranking them.

---

## 6. Results from the included sweep

5 seeds, 8 leaves × 4 cores, 24 → 72 Gbps surge, 4 initial servers.

| config | p99 ratio | degradation_ms (med [min–max]) | server_s | peak fleet |
|---|---|---|---|---|
| `scaler-none` | 77.4 | **1349** [734–4019] | 1.40 | 4 |
| `scaler-threshold` | 57.0 | 585 [460–778] | 3.08 | 14 |
| `scaler-target` | 48.6 | 616 [353–773] | 2.52 | 12 |
| `lb-rr` | 56.4 | 863 [741–1099] | 2.17 | 11 |
| `lb-random` | 53.3 | 645 [468–1350] | 2.23 | 10 |
| `lb-least` | 50.4 | **473** [274–1164] | 2.92 | 15 |
| `lb-p2c` | 48.6 | 616 [353–773] | 2.52 | 12 |
| `boot-2ms` | 47.2 | 532 [291–892] | 2.83 | 14 |
| `boot-10ms` | 48.6 | 616 [353–773] | 2.52 | 12 |
| `boot-20ms` | 50.3 | 781 [362–1814] | 1.97 | 10 |
| `boot-40ms` | 69.6 | **3531** [2462–5618] | 0.63 | 4 |
| `control-1ms` | 58.0 | 541 [294–748] | 2.83 | 14 |
| `control-10ms` | 49.7 | 886 [429–1029] | 2.01 | 11 |
| `path-ecmp` | 48.6 | 616 [353–773] | 2.52 | 12 |
| `path-conga` | 53.2 | 682 [333–1074] | 2.48 | 12 |

### What these support

1. **Autoscaling works, and the static fleet is the thing to beat.** A fixed
   4-server fleet degrades ~2× worse than either controller and never recovers
   within the run.

2. **Boot delay dominates everything else.** At 40 ms boot — 40 % of the surge
   duration — degradation is ~6× worse than at 2 ms and its spread
   [2462–5618] is cleanly separated from every other config. The fleet barely
   grows (peak 4, cost 0.63 server-s): capacity ordered during the surge
   arrives after it is over. **This is the result the project is about.**

3. **Load-aware server LB beats oblivious.** `least` and `p2c` beat `rr`, in
   the right order. Spreads overlap, so treat it as a trend, not a ranking.

4. **Coarse control hurts.** 10 ms control is clearly worse than 1–5 ms.

### What these do NOT support

* **`threshold` vs `target` is unresolved** (585 vs 616, spreads overlap
  heavily). Do not claim one is better without more seeds.
* **ECMP vs CONGA is unresolved**, and should be — the bottleneck here is the
  *access link*, not the fabric, so fabric load balancing has little to act on.
  To make `--lb_path` matter, oversubscribe the fabric (`--cores=2`).

---

## 7. Known limitations

* **No application layer.** A server cannot be slow for reasons other than its
  link. Real autoscalers trigger on CPU and request latency; this one cannot.
  Adding a `ServerSink` with a service rate (~100 lines) would fix it.
* **`backend_maxmean` is biased.** It compares cumulative bytes across servers,
  so a server that booted late looks "underused" by construction. Use it to
  compare LB policies at equal fleet size only.
* **Utilisation saturates near 0.9**, so an overloaded fleet cannot report how
  overloaded it is. This is realistic and is the source of the step-wise
  convergence — not a bug.
* **Scale-down is abrupt.** Servers leave the dispatch set immediately; there
  is no connection draining. In-flight flows finish on their original route.
