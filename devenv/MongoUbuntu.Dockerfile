# Dockerfile for a MongoDB Ubuntu 24.04
#
# Usage:
#   docker build -t mongo-ubuntu:1.0 -f MongoUbuntu.Dockerfile .
#   docker network create mongo-net

# Use Ubuntu 24.04 as base image
FROM ubuntu:24.04

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Update package list and install required packages
RUN apt-get update && apt-get upgrade -y

# Install the base packages
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    htop \
    less \
    lsb-release \
    net-tools \
    software-properties-common \
    sudo \
    telnet \
    tmux \
    vim \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create ubuntu user with sudo privileges
RUN echo "ubuntu:ubuntu" | chpasswd && \
    usermod -aG sudo ubuntu && \
    echo "ubuntu ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Copy tmux configuration file
COPY .tmux.conf /home/ubuntu/.tmux.conf
RUN chown ubuntu:ubuntu /home/ubuntu/.tmux.conf

# Add useful commands to bash history
RUN echo "tmux new-session -A -s main" >> /home/ubuntu/.bash_history && \
    chown ubuntu:ubuntu /home/ubuntu/.bash_history

# Set up the working directory
WORKDIR /home/ubuntu

# Switch to ubuntu user
USER ubuntu

# Set up bash as the default shell and configure environment
ENV SHELL=/usr/bin/bash
ENV USER=ubuntu
ENV HOME=/home/ubuntu
