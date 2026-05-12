# MongoDB Performance Experiment Runner

You are helping run and analyze MongoDB bulk-delete performance experiments on AWS EC2. Each experiment measures how a large delete operation (`deleteMany` or `fastBulkDelete`) degrades a concurrent OLTP workload under Locust.

The user invokes this skill as: `/run-experiment <ExperimentName>`

---

## What you know about the experiment structure

Every run has this shape:
```
[Locust start] → warm-up period (default 60 min) → [delete command auto-executes]
              → delete runs to completion → Locust auto-quits 60 min after delete ends
```

Key scripts in this directory — **always prefer these over rolling your own SSH/mongosh commands**:

| Script | Purpose |
|--------|---------|
| `./locust_workload_mongosh.sh <Deployment>` | Open mongosh on the RS primary. Pipe commands for non-interactive use: `echo 'db.runCommand(...)' \| ./locust_workload_mongosh.sh <Deployment>` |
| `./locust_workload_report.sh <Deployment>` | Download the live HTML Locust report from the driver |
| `./locust_workload_ssh_driver.sh <Deployment>` | Interactive SSH to the driver host |
| `./locust_workload_ssh_server.sh <Deployment>` | Interactive SSH to the RS host |
| `./locust_workload_start_remote.sh <Deployment> <config> <action> <delay>` | Start Locust non-interactively on the driver |
| `./launch_ec2_replicaset_hosts.py` | Launch / describe / terminate EC2 instances |
| `./remote_control_replicaset.py` | Deploy binaries, init RS, gather logs, stop |
| `./analyze_locust_run.py` | Phase-split latency analysis + HTML |
| `./llm-ftdc-analysis/ftdc_compare_fast.py` | FTDC summary and comparison. If not found, stop and ask the user to add it. |

Experiment metadata is stored in `<ExperimentName>/experiment_metadata.json`. Gathered logs land in `<ExperimentName>/logs/`.

To check Locust state without SSH, use the driver's HTTP API:
```bash
DRIVER=$(jq -r .DriverHosts[0] <ExperimentName>/deployment_description.json)
curl -s "http://$DRIVER:8090/stats/requests" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print('state:', d['state'], '| users:', d['user_count'])"
```

---

## How to start a new experiment

### Step 1 — Collect parameters

Ask the user to confirm these (show defaults, accept Enter to keep):

| Parameter | Default | Notes |
|-----------|---------|-------|
| `ExperimentName` | (required, from invocation) | Used as directory name and cluster tag |
| `action` | `deleteMany_10_pct` | Or `fastBulkDelete_10_pct` |
| `vol_id` | (required) | EBS volume ID `vol-XXXX` with pre-loaded dataset |
| `auto_execute_delay_secs` | `3600` | Warm-up seconds before delete fires |
| `template` | `Atlas-M50.json` | EC2 instance template JSON |
| `nodes` | `1` | Number of RS nodes |
| `mgodatagen_config` | `locust_workload_mgodatagen_1TB.json` | Dataset config |
| `user` | `kaloian.manassiev` | AWS key pair name / owner tag |
| `AWS_ACCESS_KEY_ID` | (optional) | Required only for launch and terminate |
| `AWS_SECRET_ACCESS_KEY` | (optional) | Required only for launch and terminate |
| `AWS_SESSION_TOKEN` | (optional) | Required only for launch and terminate |

### Step 2 — Pre-flight checks (checkpoint — stop here for confirmation)

Run these checks, report findings, and ask the user to confirm before proceeding:

```bash
# AWS credentials
[[ -n "$AWS_ACCESS_KEY_ID" ]] && echo "AWS_ACCESS_KEY_ID: set" || echo "AWS_ACCESS_KEY_ID: MISSING"
[[ -n "$AWS_SECRET_ACCESS_KEY" ]] && echo "AWS_SECRET_ACCESS_KEY: set" || echo "AWS_SECRET_ACCESS_KEY: MISSING"
[[ -n "$AWS_SESSION_TOKEN" ]] && echo "AWS_SESSION_TOKEN: set" || echo "AWS_SESSION_TOKEN: MISSING"

# SSH key (used by all .sh scripts)
ls -la ~/.ssh/mongodb-aws-kernel-test 2>/dev/null || echo "SSH KEY MISSING: ~/.ssh/mongodb-aws-kernel-test"

# No existing cluster with same name
python3 launch_ec2_replicaset_hosts.py --user <user> <ExperimentName> describe 2>/dev/null \
  && echo "WARNING: cluster <ExperimentName> already exists" || echo "No existing cluster found"
```

Show the complete parameter table and wait for the user to say "go" or "proceed".

### Step 3 — Launch EC2 (automated)

```bash
./launch_ec2_replicaset_hosts.py --user <user> <ExperimentName> launch \
  --template <template> --nodes <nodes> --use-volume-copy <vol_id>
```

This creates `<ExperimentName>/deployment_description.json`. Report the RS hosts once done.

### Step 4 — Deploy binaries and init replica set (automated, then checkpoint)

```bash
./remote_control_replicaset.py <ExperimentName> deploy-binaries
./remote_control_replicaset.py <ExperimentName> init
```

**Checkpoint**: Show the RS connection string from `deployment_description.json`. The user can verify RS health with:
```bash
echo 'rs.status()' | ./locust_workload_mongosh.sh <ExperimentName>
```
Ask for confirmation before starting the workload.

### Step 5 — Start the workload (automated)

```bash
./locust_workload_start_remote.sh <ExperimentName> <mgodatagen_config> <action> <auto_execute_delay_secs>
```

The script prints `LOCUST_START_UNIX=<timestamp>` and `LOCUST_PID=<pid>`. Use these to write the metadata file:

```bash
cat > <ExperimentName>/experiment_metadata.json << EOF
{
  "locust_start_unix": <LOCUST_START_UNIX>,
  "delete_start_unix": <LOCUST_START_UNIX + auto_execute_delay_secs>,
  "action": "<action>",
  "mgodatagen_config": "<mgodatagen_config>",
  "auto_execute_delay_secs": <auto_execute_delay_secs>,
  "driver_host": "<DriverHosts[0] from deployment_description.json>",
  "rs_host": "<Hosts[0] from deployment_description.json>"
}
EOF
```

Report: "Workload started. The delete action `<action>` will auto-execute at approximately `<delete_start_time UTC>` (in `<delay>` seconds). Locust will self-terminate ~60 minutes after the delete completes."

You can now wait, or the user can ask "how is it doing?" at any time.

### Step 6 — Monitor during the run (on user request)

When the user asks "how is it doing?" or similar:

1. Check Locust state and current throughput via its HTTP API (no SSH needed):
   ```bash
   DRIVER=$(jq -r .DriverHosts[0] <ExperimentName>/deployment_description.json)
   curl -s "http://$DRIVER:8090/stats/requests" | python3 -c \
     "import sys,json; d=json.load(sys.stdin); print('state:', d['state'], '| users:', d['user_count']); \
      [print(f'  {s[\"name\"]}: rps={s[\"current_rps\"]:.0f} p50={s[\"median_response_time\"]}ms') for s in d['stats']]"
   ```

2. Fetch the live HTML report:
   ```bash
   ./locust_workload_report.sh <ExperimentName>
   # saves locust_results_report_<ExperimentName>.html
   ```

3. Check whether the delete is still running on the server (`deleteMany` shows as op `remove`; `fastBulkDelete` shows as op `command`):
   ```bash
   echo 'db.currentOp({"ns": /locust_read_write_load/})' | ./locust_workload_mongosh.sh <ExperimentName>
   ```

4. Give the user a narrative based on the HTTP API output and the HTML report:
   - "Warmup (t=0 to t=Xmin): P50 was Y ms, P99 was Z ms — steady, WiredTiger cache warm"
   - "Since delete started (t=Xmin, N minutes ago): P50 has risen to Y ms (+X%), P99 is Z ms"
   - Note if P99 appears to improve — explain the cumulative-percentile artifact (see caveat below)

For interactive investigation, use:
```bash
./locust_workload_ssh_driver.sh <ExperimentName>   # SSH to driver
./locust_workload_ssh_server.sh <ExperimentName>   # SSH to RS host
```

### Step 7 — Gather results (automated, then checkpoint)

After Locust self-terminates, confirm by checking state via the HTTP API (should show `"stopped"` and `user_count: 0`), or the user tells you it's done.

```bash
./locust_workload_report.sh <ExperimentName>         # final HTML
./remote_control_replicaset.py <ExperimentName> gather-logs
python3 analyze_locust_run.py <ExperimentName> --output-html <ExperimentName>_analysis.html
```

Run the analysis and show the user the summary table. Tell them the HTML report is at `<ExperimentName>_analysis.html`.

**Checkpoint**: Ask the user to confirm before terminating the cluster (they may want to SSH in).

### Step 8 — Terminate (automated after checkpoint)

```bash
./remote_control_replicaset.py <ExperimentName> stop
./launch_ec2_replicaset_hosts.py --user <user> <ExperimentName> terminate
```

---

## How to analyze an existing experiment

When the user asks about a completed experiment (e.g. "how did DeleteMany1TB-String go?"):

```bash
python3 analyze_locust_run.py <ExperimentName> --output-html <ExperimentName>_analysis.html
```

Read the output and report:
- Delete operation duration (from metadata or ask the user)
- Baseline P50/P99 (last 5 min before delete)
- During-delete P50/P99 and percentage change
- Which transaction types were most affected

If `experiment_metadata.json` is missing for an older run, ask the user for `locust_start_unix` and `delete_start_unix` (Unix timestamps), or help them create the file.

To check document counts, use:
```bash
echo 'db.load.estimatedDocumentCount()' | ./locust_workload_mongosh.sh <ExperimentName>
```

---

## How to compare two experiments

When the user asks "compare X to Y" or "how does this run compare to DeleteMany1TB-String?":

```bash
python3 analyze_locust_run.py <ExperimentName> --compare <OtherExperiment> \
  --output-html comparison_<ExperimentName>_vs_<OtherExperiment>.html
```

Report the side-by-side table and highlight the key differences. Example phrasing: "Run X showed a P50 degradation of N% vs M% for Run Y during the delete period."

---

## How to do FTDC analysis

First check if the tools are installed:
```bash
test -f ~/llm-ftdc-analysis/ftdc_compare_fast.py && echo "FTDC tools: present" || echo "FTDC tools: MISSING"
```

If missing, tell the user:
```
The FTDC analysis tools are not installed. To set them up:
  git clone https://github.com/10gen/employees ~/employees
  cp -r ~/employees/home/luke.pearson/llm-ftdc-analysis ~/llm-ftdc-analysis
  workscripts/python3-venv/bin/pip install fastmcp numpy psutil structlog
```

Once present, for a single-run FTDC summary:
```bash
source workscripts/python3-venv/bin/activate
python3 ~/llm-ftdc-analysis/ftdc_compare_fast.py summary \
  <ExperimentName>/logs/mongod-rs-<host>.tar.gz
```
(untar first if needed: `tar -xzf <file> -C <ExperimentName>/logs/`)

For cross-run FTDC comparison:
```bash
python3 ~/llm-ftdc-analysis/ftdc_compare_fast.py compare \
  <Baseline>/logs/diagnostic.data/ <Candidate>/logs/diagnostic.data/ --json
```

Interpret the JSON output. Focus on:
- Cache hit rate: `1 − (pages_read_into_cache / pages_requested_from_cache)`
- `cache.tracked dirty internal page bytes` — B-tree internal page pressure
- `cache.forced clean page evictions` — fastBulkDelete's clean-eviction mechanism
- `cache.eviction server skips pages that previously failed eviction` — eviction pressure

---

## Critical analysis caveat — cumulative Locust percentiles

**Always mention this when showing P99 or higher:**

Locust reports percentiles **cumulative since Locust start**, not rolling. A P99 data point at t=2h covers all requests since launch. Consequences:
- P99 often appears to *improve* during the delete — this is a statistical artifact of the growing denominator, not a real improvement
- **P50 is the most reliable user-experience signal** for measuring degradation
- The baseline window should be the **last 5 minutes before delete start**, not the full warm-up

---

## Analysis report structure (for full write-ups)

Follow this 7-section structure:
1. Header (date, hardware, MongoDB version, dataset, workload summary)
2. Testing Environment & Methodology
3. Mechanism explanation (why the delete degrades the workload — cache pollution)
4. Delete Operation Duration (side-by-side timing table)
5. Impact on Concurrent Workload (throughput, baseline vs during-delete P50/P99, per-transaction breakdown)
6. WiredTiger Analysis (from FTDC)
7. Conclusion

To generate PDF or DOCX from a Markdown report:
```bash
npx md-to-pdf analysis_report.md          # PDF
pandoc analysis_report.md -o report.docx  # DOCX for Google Docs
```
