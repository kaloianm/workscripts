#!/bin/bash
set -e

###################################################################################################
echo "Applying OS configuration"
###################################################################################################

sudo bash -c 'echo "# BEGIN USER ULIMITS BLOCK" >> /etc/security/limits.conf'
sudo bash -c 'echo "ubuntu hard nofile 64000" >> /etc/security/limits.conf'
sudo bash -c 'echo "ubuntu hard nproc 64000" >> /etc/security/limits.conf'
sudo bash -c 'echo "ubuntu soft nofile 64000" >> /etc/security/limits.conf'
sudo bash -c 'echo "ubuntu soft nproc 64000" >> /etc/security/limits.conf'
sudo bash -c 'echo "# END USER ULIMITS BLOCK" >> /etc/security/limits.conf'
sudo sysctl -p

###################################################################################################
echo "Configuring required packages"
###################################################################################################

sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update -y
sudo apt install -y libsnmp-dev python3.9
