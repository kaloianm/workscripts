#!/bin/bash
source ~/.bash_scripts

DEPLOYMENT=$@
HOST=`jq -r .Hosts[0] $DEPLOYMENT/deployment_description.json`

echo "SSHing to $HOST" ...
ec2ssh $HOST
