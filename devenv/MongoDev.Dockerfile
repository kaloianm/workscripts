# Dockerfile for MongoDB development environment on Ubuntu 24.04
#
# Usage:
#   docker build -t mongo-ubuntu-dev:1.0 -f MongoDev.Dockerfile .
#
#   # For interactive development without volume mounting:
#   docker run -it --name mongo-container-dev --hostname mongo-container-dev --network mongo-net mongo-ubuntu-dev:1.0
#
#   # To reconnect to an already started container (or start and connect):
#   docker start -ai mongo-container-dev

# Use mongo-ubuntu 1.0 as base image
FROM mongo-ubuntu:1.0

# Update package list and install required packages
RUN sudo apt-get update && sudo apt-get install -y \
    build-essential \
    git \
    iputils-ping \
    openssh-client \
    procps \
    python3 \
    python3-pip \
    && sudo rm -rf /var/lib/apt/lists/*

# Import MongoDB GPG key
RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

# Add MongoDB repository
RUN echo "deb [ arch=arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

RUN sudo apt-get update && sudo apt-get install -y \
    mongodb-mongosh

# Default command to start bash shell
CMD ["/usr/bin/bash"]
