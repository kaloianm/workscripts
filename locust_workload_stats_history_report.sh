#!/bin/bash
DEPLOYMENT=$@
DRIVER_HOST=`jq -r .DriverHosts[0] $DEPLOYMENT/deployment_description.json`

echo "Fetching from $DRIVER_HOST ..."
curl -L -o "locust_results_stats_history_$DEPLOYMENT.csv" "http://$DRIVER_HOST:8090/stats/requests_full_history/csv?download=1"
