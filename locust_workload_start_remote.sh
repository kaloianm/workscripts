#!/bin/bash
#
# Start a Locust workload non-interactively on the driver host for an experiment.
#
# Usage: ./locust_workload_start_remote.sh <DeploymentDir> <MgodatagenConfig> <Action> <DelaySecs>
#
# Arguments:
#   DeploymentDir     - Experiment directory containing deployment_description.json
#   MgodatagenConfig  - Filename of the mgodatagen JSON config (e.g. locust_workload_mgodatagen_1TB.json)
#   Action            - Auto-execute action: deleteMany_10_pct or fastBulkDelete_10_pct
#   DelaySecs         - Seconds to wait before the action fires (default: 3600)
#
# On success, prints two lines to stdout:
#   LOCUST_START_UNIX=<unix timestamp on the driver host>
#   LOCUST_PID=<background process PID>

set -e

DEPLOYMENT="$1"
MGODATAGEN_CONFIG="$2"
ACTION="$3"
DELAY_SECS="${4:-3600}"

if [[ -z "$DEPLOYMENT" || -z "$MGODATAGEN_CONFIG" || -z "$ACTION" ]]; then
    echo "Usage: $0 <DeploymentDir> <MgodatagenConfig> <Action> [DelaySecs]" >&2
    exit 1
fi

DEPLOYMENT_JSON="$DEPLOYMENT/deployment_description.json"
if [[ ! -f "$DEPLOYMENT_JSON" ]]; then
    echo "ERROR: $DEPLOYMENT_JSON not found" >&2
    exit 1
fi

DRIVER_HOST=$(jq -r '.DriverHosts[0]' "$DEPLOYMENT_JSON")
RS_HOST=$(jq -r '.Hosts[0]' "$DEPLOYMENT_JSON")

if [[ -z "$DRIVER_HOST" || "$DRIVER_HOST" == "null" ]]; then
    echo "ERROR: DriverHosts[0] not found in $DEPLOYMENT_JSON" >&2
    exit 1
fi

echo "Starting Locust on $DRIVER_HOST against $RS_HOST ..." >&2
echo "  Config: $MGODATAGEN_CONFIG  Action: $ACTION  Delay: ${DELAY_SECS}s" >&2

SSH="ssh -i ~/.ssh/mongodb-aws-kernel-test -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# Run the nohup command on the driver, capturing the start timestamp and PID.
# The heredoc is evaluated locally (variables are expanded here before sending).
OUTPUT=$($SSH ubuntu@"$DRIVER_HOST" bash << EOF
set -e
cd workscripts
source python3-venv/bin/activate
START_UNIX=\$(date +%s)
nohup locust -f locust_workload.py \\
    --csv=locust_results \\
    --csv-full-history \\
    --html=locust_results_report.html \\
    --print-stats \\
    --mgodatagen-config "$MGODATAGEN_CONFIG" \\
    --auto-execute "$ACTION" \\
    --auto-execute-delay "$DELAY_SECS" \\
    --host "mongodb://$RS_HOST" \\
    > locust_results_nohup.log 2>&1 &
LOCUST_PID=\$!
echo "LOCUST_START_UNIX=\$START_UNIX"
echo "LOCUST_PID=\$LOCUST_PID"
EOF
)

echo "$OUTPUT"

START_UNIX=$(echo "$OUTPUT" | grep LOCUST_START_UNIX | cut -d= -f2)
PID=$(echo "$OUTPUT" | grep LOCUST_PID | cut -d= -f2)

echo "Locust started on $DRIVER_HOST with PID $PID (start unix: $START_UNIX)" >&2
