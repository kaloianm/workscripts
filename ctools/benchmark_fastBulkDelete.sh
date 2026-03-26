#!/bin/bash
set -euo pipefail

##############################################################################
# Benchmark: fastBulkDelete
#
# Launches a 1-node replica set on EC2, populates it with a ~500GB collection
# (34M docs, 10 secondary indexes), then times a fastBulkDelete of ~9M
# documents (~100GB) in a contiguous shardKey range.
#
# Designed to be run alongside benchmark_deleteMany.sh on a separate cluster
# for an apples-to-apples comparison.
##############################################################################

##############################################################################
# Configuration — edit these as needed
##############################################################################
CLUSTER_TAG="bench-fastBulkDelete"
MONGO_BIN_PATH="/home/ubuntu/workspace/mongo/bazel-bin/install-devcore/bin"
CTOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-}")" 2>/dev/null && pwd || pwd)"
DEPLOY_DIR="${CTOOLS_DIR}/${CLUSTER_TAG}"
REPLSET_JSON="${DEPLOY_DIR}/deployment_description.json"
MGODATAGEN_CONFIG="${CTOOLS_DIR}/locust_read_write_load_mgodatagen_500GB.json"
RESULTS_DIR="${DEPLOY_DIR}/results"

# Deletion range: shardKey [8500000, 17500000) — ~9M docs out of 34M ≈ 100GB of data
# Starts at ~25% into the keyspace to avoid deleting from the very beginning
DELETE_LOWER=8500000
DELETE_UPPER=17500000

echo "=== Benchmark: fastBulkDelete ==="
echo "Cluster tag:  ${CLUSTER_TAG}"
echo "Results dir:  ${RESULTS_DIR}"
mkdir -p "${RESULTS_DIR}"

##############################################################################
# Step 1: Launch EC2 replica set (1 node)
##############################################################################
echo ""
echo "=== Step 1: Launching EC2 replica set ==="
cd "${CTOOLS_DIR}"
python3 launch_ec2_replicaset_hosts.py "${CLUSTER_TAG}" launch \
    --template Atlas-M50.json --nodes 1 --filesystem xfs

##############################################################################
# Step 2: Set MongoBinPath in replset.json
##############################################################################
echo ""
echo "=== Step 2: Updating MongoBinPath ==="
python3 -c "
import json, sys
with open('${REPLSET_JSON}') as f:
    cfg = json.load(f)
cfg['MongoBinPath'] = '${MONGO_BIN_PATH}'
with open('${REPLSET_JSON}', 'w') as f:
    json.dump(cfg, f, indent=2)
print('MongoBinPath set to:', cfg['MongoBinPath'])
"

##############################################################################
# Step 3: Deploy binaries and create replica set
##############################################################################
echo ""
echo "=== Step 3: Creating replica set ==="
python3 remote_control_replicaset.py "${CLUSTER_TAG}" create

# Extract host addresses
RS_HOST=$(python3 -c "import json; print(json.load(open('${REPLSET_JSON}'))['Hosts'][0])")
echo "RS host: ${RS_HOST}"

##############################################################################
# Step 4: Populate data with mgodatagen (from the EC2 driver/client host)
##############################################################################
echo ""
echo "=== Step 4: Populating data (34M docs, ~500GB — this will take a while) ==="

DRIVER_HOST=$(python3 -c "import json; print(json.load(open('${REPLSET_JSON}'))['DriverHosts'][0])")
echo "Driver host: ${DRIVER_HOST}"

# Copy mgodatagen binary and config to the driver host
MGODATAGEN_BIN=$(which mgodatagen)
echo "Copying mgodatagen binary from ${MGODATAGEN_BIN} to driver host..."
scp -o StrictHostKeyChecking=no "${MGODATAGEN_BIN}" "ubuntu@${DRIVER_HOST}:~/mgodatagen"
scp -o StrictHostKeyChecking=no "${MGODATAGEN_CONFIG}" "ubuntu@${DRIVER_HOST}:~/mgodatagen_config.json"
ssh -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}" "chmod +x ~/mgodatagen"

# Run mgodatagen on the driver host (same VPC, no internet transit)
DATA_LOAD_START=$(date +%s)
ssh -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}" \
    "./mgodatagen -f mgodatagen_config.json --uri 'mongodb://${RS_HOST}:27017'"
DATA_LOAD_END=$(date +%s)
echo "Data population completed in $((DATA_LOAD_END - DATA_LOAD_START)) seconds"

##############################################################################
# Step 5: Start background workload on the client host
##############################################################################
echo ""
echo "=== Step 5: Starting background locust workload on client host ==="

# Clone workscripts repo and install dependencies on the driver host
ssh -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}" bash -s <<'REMOTE_SETUP'
set -euo pipefail
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"
brew install python3
if [ ! -d ~/workscripts ]; then
    git clone https://github.com/kaloianm/workscripts.git ~/workscripts
else
    cd ~/workscripts && git pull
fi
cd ~/workscripts/ctools
rm -rf python3-venv
python3 -m venv python3-venv
source python3-venv/bin/activate
pip install -q -r requirements.txt
REMOTE_SETUP

# Start locust in the background on the driver host
ssh -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}" bash -s <<REMOTE_LOCUST
set -euo pipefail
eval "\$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"
cd ~/workscripts/ctools
git pull
source python3-venv/bin/activate
nohup locust -f locust_read_write_load.py \
    --processes 4 --users 2500 --spawn-rate 100 --autostart \
    --web-port 8090 \
    --mgodatagen-config locust_read_write_load_mgodatagen_500GB.json \
    --host "mongodb://${RS_HOST}:27017" \
    > /tmp/locust_benchmark.log 2>&1 &
echo "Locust started with PID \$!"
REMOTE_LOCUST
echo "Locust workload running on ${DRIVER_HOST} (web UI at http://${DRIVER_HOST}:8090)"

# Let the workload ramp up before starting the benchmark
sleep 30

##############################################################################
# Step 6: Capture pre-deletion metrics
##############################################################################
echo ""
echo "=== Step 6: Capturing pre-deletion metrics ==="

mongosh --quiet "mongodb://${RS_HOST}:27017/locust_read_write_load" --eval '
    var status = db.serverStatus();
    printjson({
        opcounters: status.opcounters,
        wiredTiger_cache: {
            "bytes currently in the cache": status.wiredTiger.cache["bytes currently in the cache"],
            "bytes read into cache": status.wiredTiger.cache["bytes read into cache"],
            "bytes written from cache": status.wiredTiger.cache["bytes written from cache"],
            "pages read into cache": status.wiredTiger.cache["pages read into cache"],
            "pages written from cache": status.wiredTiger.cache["pages written from cache"]
        },
        mem: status.mem
    });
' | tee "${RESULTS_DIR}/serverStatus_before.json"

mongosh --quiet "mongodb://${RS_HOST}:27017/locust_read_write_load" --eval '
    printjson(db.load.stats());
' | tee "${RESULTS_DIR}/collStats_before.json"

echo "Capturing dstat snapshot on RS host..."
ssh -o StrictHostKeyChecking=no "ubuntu@${RS_HOST}" "dstat --nocolor -cdnmgy 1 5" \
    | tee "${RESULTS_DIR}/dstat_before.txt"

##############################################################################
# Step 7: Run fastBulkDelete (TIMED)
##############################################################################
echo ""
echo "=== Step 7: Running fastBulkDelete — shardKey [${DELETE_LOWER}, ${DELETE_UPPER}) ==="
DELETE_START=$(date +%s)

mongosh --quiet "mongodb://${RS_HOST}:27017/locust_read_write_load" --eval "
    var start = new Date();
    var result = db.runCommand({
        fastBulkDelete: 'load',
        filterIndexName: 'shardKey',
        lowerBound: {shardKey: ${DELETE_LOWER}},
        upperBound: {shardKey: ${DELETE_UPPER}}
    });
    var end = new Date();
    printjson({
        ok: result.ok,
        durationMs: end - start,
        durationSec: (end - start) / 1000,
        result: result
    });
" | tee "${RESULTS_DIR}/delete_result.json"

DELETE_END=$(date +%s)
WALL_CLOCK=$((DELETE_END - DELETE_START))
echo ""
echo ">>> fastBulkDelete wall-clock runtime: ${WALL_CLOCK} seconds <<<"
echo "${WALL_CLOCK}" > "${RESULTS_DIR}/wall_clock_seconds.txt"

##############################################################################
# Step 8: Capture post-deletion metrics
##############################################################################
echo ""
echo "=== Step 8: Capturing post-deletion metrics ==="

mongosh --quiet "mongodb://${RS_HOST}:27017/locust_read_write_load" --eval '
    var status = db.serverStatus();
    printjson({
        opcounters: status.opcounters,
        wiredTiger_cache: {
            "bytes currently in the cache": status.wiredTiger.cache["bytes currently in the cache"],
            "bytes read into cache": status.wiredTiger.cache["bytes read into cache"],
            "bytes written from cache": status.wiredTiger.cache["bytes written from cache"],
            "pages read into cache": status.wiredTiger.cache["pages read into cache"],
            "pages written from cache": status.wiredTiger.cache["pages written from cache"]
        },
        mem: status.mem
    });
' | tee "${RESULTS_DIR}/serverStatus_after.json"

mongosh --quiet "mongodb://${RS_HOST}:27017/locust_read_write_load" --eval '
    printjson(db.load.stats());
' | tee "${RESULTS_DIR}/collStats_after.json"

##############################################################################
# Step 9: Stop locust workload and collect FTDC and logs
##############################################################################
echo ""
echo "=== Step 9: Stopping locust and collecting results ==="

# Stop locust on the driver host
ssh -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}" "pkill -f locust 2>/dev/null || true"
scp -o StrictHostKeyChecking=no "ubuntu@${DRIVER_HOST}:/tmp/locust_benchmark.log" "${RESULTS_DIR}/"

# Gather mongod logs + FTDC diagnostic data (includes CPU, I/O, memory metrics)
python3 remote_control_replicaset.py "${CLUSTER_TAG}" gather-logs "${RESULTS_DIR}"

##############################################################################
# Step 10: Summary
##############################################################################
echo ""
echo "================================================================"
echo "  BENCHMARK COMPLETE: fastBulkDelete"
echo "================================================================"
echo "  Cluster tag:       ${CLUSTER_TAG}"
echo "  Deletion range:    shardKey [${DELETE_LOWER}, ${DELETE_UPPER})"
echo "  Wall-clock time:   ${WALL_CLOCK} seconds"
echo "  Results saved to:  ${RESULTS_DIR}/"
echo ""
echo "  To terminate EC2 instances when done:"
echo "    cd ${CTOOLS_DIR} && python3 launch_ec2_replicaset_hosts.py ${CLUSTER_TAG} terminate"
echo "================================================================"
