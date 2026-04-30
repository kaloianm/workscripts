#!/bin/bash
source ~/.bash_scripts

DEPLOYMENT=$@
HOST=`jq -r .Hosts[0] $DEPLOYMENT/deployment_description.json`

echo "MongoSHing to $HOST" ...
mongosh $HOST
