# Dockerfile for MongoDB 8.0 on Ubuntu 24.04 with Replica Set
#
# Usage:
#   docker build -t mongo-ubuntu-instance:8.0 -f MongoInstance.Dockerfile .
#
#   # Run with named volume:
#   docker run -d -p 27017:27017 \
#     --name mongo-8.0 \
#     --network mongo-net \
#     --hostname mongo-8-0 \
#     --network-alias mongo-8-0 \
#     --memory="4g" \
#     --memory-swap="4g" \
#     --volume=mongo-8.0:/mongo-data \
#     mongo-ubuntu-instance:8.0
#
#   # To connect to the running MongoDB instance:
#   docker exec -it --user ubuntu mongo-8.0 /usr/bin/bash
#
# The MongoDB instance will be initialized as a single-node replica set named 'rs0'
# Connect with:
#   mongosh mongodb://mongo-8-0:27017
#   mongosh mongodb://localhost:27017

# Use mongo-ubuntu 1.0 as base image
FROM mongo-ubuntu:1.0

# Import MongoDB GPG key
RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

# Add MongoDB repository
RUN echo "deb [ arch=arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

# Update package list again and install MongoDB 8.0
RUN sudo apt-get update && sudo apt-get install -y \
    mongodb-org=8.0.* \
    mongodb-org-database=8.0.* \
    mongodb-org-server=8.0.* \
    mongodb-org-mongos=8.0.* \
    mongodb-org-tools=8.0.* \
    mongodb-mongosh \
    && sudo rm -rf /var/lib/apt/lists/*

# Create MongoDB directories within a single volume
RUN sudo mkdir -p /mongo-data/db && sudo chown -R mongodb:mongodb /mongo-data/db
RUN sudo mkdir -p /mongo-data/logs && sudo chown -R mongodb:mongodb /mongo-data/logs

# Create single volume for all MongoDB data
VOLUME ["/mongo-data"]

# Expose MongoDB port
EXPOSE 27017

# Set the default user
USER mongodb

# Simply start MongoDB
CMD ["mongod", "--bind_ip", "0.0.0.0", "--port", "27017", "--dbpath", "/mongo-data/db", "--logpath", "/mongo-data/logs/mongod.log", "--replSet", "rs0"]
