# GPU Warm-Up Testbed (htsim `--expt=5`)

## Build

```bash
make                                    # -> ./htsim
```

## Run

```bash
./htsim --expt=5 --warmup=storage       # single run
./scripts/run_gpu_warmup.sh source      # storage vs peer, 3 seeds
./scripts/run_gpu_warmup.sh all         # every sweep, ~4 min
```

Suites: `source` `storage` `state` `batch` `place` `parallel` `all`.

A single run prints per-flow lines to stdout; the last `WarmStats` line is
its summary. The sweep script aggregates automatically.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `--warmup` | storage | `storage` / `peer` / `peer-racklocal` |
| `--placement` | packed | `packed` / `roundrobin` |
| `--racks --gpus` | 8, 8 | GPU slots |
| `--init_nodes` | 6 | Nodes already warm at t=0 |
| `--compute_ratio` | 0.4 | Compute ÷ NIC |
| `--statesize` | 50 | Model weights, MB |
| `--storage_nodes --storage_read` | 4, 4 | Storage tier |
| `--nvlink` | 100 | Intra-rack Gbps |
| `--step_up` | 4 | Nodes launched per scale-up |
| `--baseload --peakload` | 15, 45 | Gbps offered |
| `--logfile` | — | Write trace to file |

Do not use `--flowdist=enterprise`; its flow sizes make the load signal
unusable. Default is `uniform`.

## Output

Written to `results_gpu/`:

| Path | Contents | In git |
|---|---|---|
| `runs.csv` | one row per run | yes |
| `summary.txt` | aggregated table | yes |
| `csv/` | per-run time series | yes |
| `logs/` | raw packet traces | no (ignored) |

`run_gpu_warmup.sh` truncates `runs.csv` on start, so running one suite
discards the other suites' rows.
