#!/bin/bash
DEPLOYMENT=$@
DRIVER_HOST=`jq -r .DriverHosts[0] $DEPLOYMENT/deployment_description.json`

echo "Fetching from $DRIVER_HOST" ...
curl -L -o "locust_results_report_$DEPLOYMENT.html" \
    "http://$DRIVER_HOST:8090/stats/report?download=1&theme=dark"
