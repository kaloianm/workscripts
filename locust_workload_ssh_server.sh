#!/bin/bash
DEPLOYMENT="${1%/}"
HOST=`jq -r .Hosts[0] "$DEPLOYMENT/deployment_description.json"`

echo "SSHing to $HOST ..."
ssh -i ~/.ssh/mongodb-aws-kernel-test -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@"$HOST"
