#!/bin/bash
DEPLOYMENT=$@
DRIVER_HOST=`jq -r .DriverHosts[0] $DEPLOYMENT/deployment_description.json`

echo "SSHing to $DRIVER_HOST ..."
ssh -i ~/.ssh/mongodb-aws-kernel-test -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@"$DRIVER_HOST"
