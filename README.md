# Workscripts
Repository of miscellaneous scripts to improve my MongoDB work productivity. Tailored specifically for my workflow, so might not be suitable for other engineers.

# Set of Python tools for manipulating sharded clusters
The CTools package is a set of command-line tools for manipulating sharded clusters. All the tools are built on a common Python framework and use asyncio in order to achieve parallelism.

Click on the links below or run the respective tool with `--help` for more information on how to use it.

### [defragment_sharded_collection](https://github.com/kaloianm/workscripts/blob/master/defragment_sharded_collection.py#L3)
### [reconstruct_cluster_from_config_dump](https://github.com/kaloianm/workscripts/blob/master/reconstruct_cluster_from_config_dump.py#L3)

# Set of Python tools to run Locust performance experiments

### [launch_ec2_replicaset_hosts](https://github.com/kaloianm/workscripts/blob/master/launch_ec2_replicaset_hosts.py#L3)
### [launch_ec2_cluster_hosts](https://github.com/kaloianm/workscripts/blob/master/launch_ec2_cluster_hosts.py#L3)
### [remote_control_cluster](https://github.com/kaloianm/workscripts/blob/master/remote_control_cluster.py#L3)
### [generate_fragmented_sharded_collection](https://github.com/kaloianm/workscripts/blob/master/generate_fragmented_sharded_collection.py#L3)
### [locust_workload](https://github.com/kaloianm/workscripts/blob/master/locust_workload.py#L3)

---

# Locust performance experiments

The [locust_workload](https://github.com/kaloianm/workscripts/blob/master/locust_workload.py#L3) script executes a customizable "OLTP-like" workload against a running cluster, preloaded with data that has schema like the one described in the following section.

## Data generation (mgodatagen)

Use the following commands to load a MongoDB server with data:
```
mgodatagen -f locust_workload_mgodatagen_10GB.json --uri mongodb://localhost
mgodatagen -f locust_workload_mgodatagen_100GB.json --uri mongodb://localhost
mgodatagen -f locust_workload_mgodatagen_500GB.json --uri mongodb://localhost
mgodatagen -f locust_workload_mgodatagen_1TB.json --uri mongodb://localhost
mgodatagen -f locust_workload_mgodatagen_4TB.json --uri mongodb://localhost
```

All configs share the same document structure: a 2,700-byte non-indexed `payload` field, ten 120-byte indexed string fields (`idx0`–`idx9`), a 16-byte random string `shardKey` (non-unique index), and `_id`. This yields approximately **4KB per document** (~4,077 bytes raw). The storage split is roughly **77% data / 23% indexes** (see [Document size breakdown](#document-size-breakdown) below).

- **locust_workload_mgodatagen_10GB.json** — Generates a ~10GB dataset (2M docs). Approximate breakdown: ~8GB data + ~2.4GB indexes. Good for quick local testing.
- **locust_workload_mgodatagen_100GB.json** — Generates a ~100GB dataset (20M docs). Approximate breakdown: ~81GB data + ~24GB indexes. Good for slightly more realistic local testing.
- **locust_workload_mgodatagen_500GB.json** — Generates a ~500GB dataset (100M docs). Approximate breakdown: ~406GB data + ~120GB indexes. Suitable for M50 instances with 1TB data volume.
- **locust_workload_mgodatagen_1TB.json** — Generates a ~1TB dataset (200M docs). Approximate breakdown: ~812GB data + ~240GB indexes. Requires instances with 1.5TB+ data volume.
- **locust_workload_mgodatagen_4TB.json** — Generates a ~4TB dataset (800M docs). Approximate breakdown: ~3.25TB data + ~960GB indexes. Requires instances with 5TB+ data volume.

### Document size breakdown

Every document has the following layout (all string fields contain random ASCII characters):

| Field | Type | Size | Indexed? |
|---|---|---|---|
| `_id` | ObjectId | 12 bytes | Yes (always) |
| `shardKey` | string | **16 bytes** | Yes (non-unique) |
| `idx0`–`idx9` | string × 10 | 120 bytes each → **1,200 bytes total** | Yes (10 secondary indexes) |
| `payload` | string | **2,700 bytes** | No |
| BSON framing / field names | — | ~144 bytes | — |
| **Total** | | **~4,077 bytes ≈ 4KB** | |

**Per-document storage contribution:**
- Non-indexed data (`payload` + overhead): ~2,844 bytes ≈ **70% of the document**
- Indexed fields (`idx0`–`idx9`): 1,200 bytes ≈ **30% of the document**

**Collection vs. index storage ratio:**  
Each of the 10 secondary indexes stores a copy of its 120-byte key per document, so index storage ≈ `10 × 120 × N` bytes. For a collection of N documents:
- Collection (data): `N × 4,077 bytes` ≈ **77%** of total on-disk footprint
- Indexes (all 10 secondary + shardKey): `N × 1,200 bytes` ≈ **23%** of total on-disk footprint

---

## Analyzing experiment results

### [analyze_locust_run](https://github.com/kaloianm/workscripts/blob/master/analyze_locust_run.py)

Reads `<ExperimentName>/experiment_metadata.json` for phase boundaries (`locust_start_unix`, `delete_start_unix`) and `<ExperimentName>/logs/locust/locust_results_stats_history.csv` for per-second latency data collected by Locust.

#### Single-run latency summary

Prints a terminal table and optionally an HTML report with a latency time-series chart. Three phases are analysed: warm-up (from Locust start until 5 minutes before the delete fires), baseline (the last 5 minutes before the delete), and during-delete. The primary signal is the % change in P50 from baseline to during-delete.

> **P99 caveat:** Locust reports *cumulative* percentiles since launch, not rolling. P99 can appear to improve during the delete (growing denominator). P50 is the most reliable degradation signal.

```bash
python3 analyze_locust_run.py DeleteMany1TB-String
python3 analyze_locust_run.py DeleteMany1TB-String --output-html report.html

# Override phase boundaries if experiment_metadata.json is missing
python3 analyze_locust_run.py OldRun --locust-start-unix 1234567890 --delete-start-unix 1234571490

# Point at a live CSV (e.g. while the experiment is still running)
python3 analyze_locust_run.py MyRun --csv path/to/locust_results_stats_history.csv
```

#### Multi-run Locust latency comparison (`--compare-locust-latencies`)

Generates `comparison_locust.png`: a three-panel (P50 / P90 / P99) time-series plot with 15-minute max buckets, log Y scale, and time normalised so all runs start at t = 0. The red dashed vertical line marks t = 1 h (when the delete fires). Runs that have already completed are drawn solid up to the completion marker and dotted after it.

```bash
python3 analyze_locust_run.py DeleteMany1TB-String DeleteMany1TB-String-ReadOnce FastBulkDelete1TB-String \
    --compare-locust-latencies
```

#### Multi-run FTDC cache metrics comparison (`--compare-ftdc`)

Generates `comparison_ftdc.png`: a three-panel plot of the most discriminating server-level WiredTiger cache metrics (dirty bytes, pages read from disk/s, app-thread eviction pressure, etc.) over the first 24 hours of each run. Uses the same time-normalised and completion-marker style as the Locust plot.

Requires `llm-ftdc-analysis` to be installed at `~/workspace/llm-ftdc-analysis/`. Only the FTDC metric files that fall within the requested time window are read, keeping peak memory well below the full-archive size.

```bash
python3 analyze_locust_run.py DeleteMany1TB-String DeleteMany1TB-String-ReadOnce FastBulkDelete1TB-String \
    --compare-ftdc

# Extend or shorten the FTDC window (default 24 h)
python3 analyze_locust_run.py ... --compare-ftdc --ftdc-hours 48

# Both plots in one invocation
python3 analyze_locust_run.py DeleteMany1TB-String DeleteMany1TB-String-ReadOnce FastBulkDelete1TB-String \
    --compare-locust-latencies --compare-ftdc
```

---

## LVM thin-pool volume for COW snapshot experiments

The data volume on each EC2 instance is provisioned as an **LVM thin-pool** on the attached EBS
volume. This lets you take a near-instant copy-on-write (COW) snapshot of the fully-loaded dataset
so that experiments can be run against it and the volume reverted to a known-clean state without
re-loading data.

The thin-pool is sized to occupy 95% of the EBS volume. Because both the origin volume and its
snapshot share the same pool, the EBS volume must be large enough to hold the dataset **plus** the
cumulative divergence (writes) that occur during an experiment. The pool fill level can be
monitored at any time with:

```bash
sudo lvs -o+data_percent,metadata_percent datavg/datapool
```

### Snapshot the clean state (run once, after data load)

```bash
sudo lvcreate -s -kn --name data_clean datavg/data
```

### Roll back to the clean state (near-instant)

Stop MongoDB gracefully before unmounting:

```bash
sudo pkill -SIGTERM mongod mongos
```

Then revert the volume:

```bash
sudo umount /mnt/data
sudo lvremove -f datavg/data
sudo lvcreate -s -kn --name data datavg/data_clean
sudo mount /dev/datavg/data /mnt/data
```

### Permanently drop the snapshot and promote clean state as the primary volume

Stop MongoDB gracefully before unmounting:

```bash
sudo pkill -SIGTERM mongod mongos
```

Then promote the clean snapshot:

```bash
sudo umount /mnt/data
sudo lvremove -f datavg/data
sudo lvrename datavg/data_clean datavg/data
sudo mount /dev/datavg/data /mnt/data
```
