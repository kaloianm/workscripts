#!/bin/bash
DEPLOYMENT=$@
HOST=`jq -r .Hosts[0] $DEPLOYMENT/deployment_description.json`

echo "MongoSHing to $HOST ..."
mongosh $HOST
