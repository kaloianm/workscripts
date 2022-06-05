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
sudo bash -c 'echo "vm.max_map_count = 262144" >> /etc/sysctl.conf'
sudo sysctl -p

###################################################################################################
echo "Configuring required packages"
###################################################################################################

sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update -y
sudo apt install -y libsnmp-dev python3.9 python3.9-distutils fio

sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2

curl https://bootstrap.pypa.io/get-pip.py -o $HOME/get-pip.py
python3.9 $HOME/get-pip.py

git clone https://github.com/kaloianm/workscripts.git $HOME/workscripts

###################################################################################################
echo "Configuring volumes"
###################################################################################################

sudo mkdir /mnt/data

echo "Waiting for data volume to be attached ..."
while [ ! -e "/dev/nvme1n1" ]; do sleep 1; done

if [ ! -e "/dev/nvme1n1p1" ]; then
  echo "Partitioning data volume ..."
  sudo parted -s /dev/nvme1n1 mklabel gpt &&
    sudo parted -s -a optimal /dev/nvme1n1 mkpart primary 0% 100%

  echo "Making EXT4 filesystem ..."
  while [ ! -e "/dev/nvme1n1p1" ]; do sleep 1; done
  sudo mkfs -t ext4 /dev/nvme1n1p1
fi

echo "Mounting data volume ..."
sudo mount /dev/nvme1n1p1 /mnt/data
sudo chown ubuntu:ubuntu /mnt/data
