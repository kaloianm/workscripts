# Dockerfile for MongoDB 8.0 on Ubuntu 24.04 with Replica Set
#
# Usage:
#   docker build -t mongo-ubuntu-instance:8.0 -f MongoInstance.Dockerfile .
#
#   docker run -d -p 27017:27017 --name mongo-8.0 --network mongo-net --hostname mongo-8-0 --network-alias mongo-8-0 mongo-ubuntu-instance:8.0
#
#   # To connect to the running MongoDB instance:
#   docker exec -it --user ubuntu mongo-8.0 /usr/bin/bash
#
# The MongoDB instance will be initialized as a single-node replica set named 'rs0'
# Connect with: mongosh mongodb://mongo-8-0:27017

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

# Create MongoDB data directory
RUN sudo mkdir -p /data/db && sudo chown -R mongodb:mongodb /data/db

# Create MongoDB log directory
RUN sudo mkdir -p /var/log/mongodb && sudo chown -R mongodb:mongodb /var/log/mongodb

# Expose MongoDB port
EXPOSE 27017

# Set the default user
USER mongodb

# Simply start MongoDB
CMD ["mongod", "--bind_ip", "0.0.0.0", "--port", "27017", "--dbpath", "/data/db", "--logpath", "/var/log/mongodb/mongod.log", "--replSet", "rs0"]
