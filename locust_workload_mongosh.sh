#!/bin/bash
DEPLOYMENT="${1%/}"
HOST=`jq -r .Hosts[0] "$DEPLOYMENT/deployment_description.json"`

echo "MongoSHing to $HOST ..."
mongosh $HOST/locust_read_write_load
