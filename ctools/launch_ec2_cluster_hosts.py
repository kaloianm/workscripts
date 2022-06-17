#!/usr/bin/env python3
#
help_string = '''
This is a tool to launch a set of EC2 hosts for creating a new cluster.

See the help for more commands.
'''

import argparse
import asyncio
import boto3
import json
import logging
import sys

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

#
# Host configuration scripts. Their output will show up in /var/log/cloud-init-output.log.
#

any_host_configuration = '''#!/bin/bash
set -e

export UBUNTU_USER_HOME_DIR=`getent passwd ubuntu | cut -d: -f6`

sudo mkdir /mnt/data
sudo chown ubuntu:ubuntu /mnt/data

###################################################################################################
echo "Applying OS configuration (Home: $UBUNTU_USER_HOME_DIR)"
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

curl https://bootstrap.pypa.io/get-pip.py -o $UBUNTU_USER_HOME_DIR/get-pip.py
python3.9 $UBUNTU_USER_HOME_DIR/get-pip.py
rm $UBUNTU_USER_HOME_DIR/get-pip.py

git clone https://github.com/kaloianm/workscripts.git $UBUNTU_USER_HOME_DIR/workscripts
python3.9 -m pip install -r $UBUNTU_USER_HOME_DIR/workscripts/ctools/requirements.txt
'''


def make_client_driver_host_configuration():
    return any_host_configuration + f'''
###################################################################################################
echo "Configuring driver host workscripts"
###################################################################################################
'''


def make_cluster_host_configuration(filesystem):
    return any_host_configuration + f'''
###################################################################################################
echo "Configuring shard host volumes"
###################################################################################################

echo "Waiting for data volume to be attached ..."
while [ ! -e "/dev/nvme1n1" ]; do sleep 1; done

if [ ! -e "/dev/nvme1n1p1" ]; then
  echo "Partitioning data volume ..."
  sudo parted -s /dev/nvme1n1 mklabel gpt &&
    sudo parted -s -a optimal /dev/nvme1n1 mkpart primary 0% 100%

  echo "Making {filesystem} filesystem ..."
  while [ ! -e "/dev/nvme1n1p1" ]; do sleep 1; done
  sudo mkfs -t {filesystem} /dev/nvme1n1p1
fi

echo "Mounting data volume ..."
sudo mount /dev/nvme1n1p1 /mnt/data
sudo chown ubuntu:ubuntu /mnt/data
'''


def make_instance_tag_specifications(cluster_tag, role):
    return [{
        'ResourceType':
            'instance',
        'Tags': [{
            'Key': 'mongoversion',
            'Value': cluster_tag
        }, {
            'Key': 'mongorole',
            'Value': role
        }, {
            'Key': 'noreap',
            'Value': 'true'
        }]
    }]


def describe_all_instances(ec2, clustertag=None):
    filters = [{
        'Name': 'tag:owner',
        'Values': ['kaloian.manassiev']
    }, {
        'Name': 'instance-state-name',
        'Values': ['running']
    }]

    if clustertag:
        filters.append({'Name': 'tag:mongoversion', 'Values': [clustertag]})

    response = ec2.describe_instances(Filters=filters)

    assert ('NextToken' not in response)
    all_instances = []
    for reservation in response['Reservations']:
        all_instances += reservation['Instances']

    return all_instances


def describe_cluster(ec2, clustertag):
    all_instances = describe_all_instances(ec2, clustertag)

    def filter_by_role(role):
        return list(
            filter(lambda x: {
                'Key': 'mongorole',
                'Value': role
            } in x['Tags'], all_instances))

    cluster_json = {
        "Name":
            clustertag,
        "Hosts":
            list(
                map(lambda x: x["PublicDnsName"],
                    filter_by_role('config') + filter_by_role('shard0') + filter_by_role('shard1'))
            ),
        "DriverHosts":
            list(map(lambda x: x["PublicDnsName"], filter_by_role('driver'))),
        "MongoBinPath":
            "<Substitute with the local binaries path>",
        "RemoteMongoDPath":
            "/mnt/data/mongod",
        "RemoteMongoSPath":
            "/mnt/data/mongos",
        "FeatureFlags": []
    }

    logging.info('\n' + json.dumps(cluster_json, indent=2, separators=(', ', ': ')))


async def main_launch(args, ec2):
    # Client driver instance
    client_driver_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        }, TagSpecifications=make_instance_tag_specifications(args.clustertag, 'driver'),
        UserData=make_client_driver_host_configuration(), MinCount=1, MaxCount=1)['Instances']

    # Config server instances
    config_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        }, BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sdf',
                'Ebs': {
                    'VolumeType': 'gp3',
                    'VolumeSize': 256,
                },
            },
        ], TagSpecifications=make_instance_tag_specifications(args.clustertag, 'config'),
        UserData=make_cluster_host_configuration(args.filesystem), MinCount=3,
        MaxCount=3)['Instances']

    # Shard0 instances
    shard0_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        }, BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sdf',
                'Ebs': {
                    'VolumeType': 'gp3',
                    'VolumeSize': 1500,
                    'Iops': 3000
                },
            },
        ], TagSpecifications=make_instance_tag_specifications(args.clustertag, 'shard0'),
        UserData=make_cluster_host_configuration(args.filesystem),
        MinCount=args.shard_repl_set_nodes, MaxCount=args.shard_repl_set_nodes)['Instances']

    # Shard1 instances
    shard1_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        }, BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sdf',
                'Ebs': {
                    'VolumeType': 'gp3',
                    'VolumeSize': 1500,
                    'Iops': 3000
                },
            },
        ], TagSpecifications=make_instance_tag_specifications(args.clustertag, 'shard1'),
        UserData=make_cluster_host_configuration(args.filesystem),
        MinCount=args.shard_repl_set_nodes, MaxCount=args.shard_repl_set_nodes)['Instances']

    logging.info('Instances launched, now waiting for them to start running ...')

    waiter = ec2.get_waiter('instance_running')
    waiter.wait(
        InstanceIds=list(
            map(lambda x: x['InstanceId'], client_driver_instances + config_instances +
                shard0_instances + shard1_instances)))

    describe_cluster(ec2, args.clustertag)


async def main_describe(args, ec2):
    describe_cluster(ec2, args.clustertag)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    argsParser.add_argument(
        'clustertag', help=
        ('String with which to tag all the instances which will be spawned for this cluster so they '
         'can easily be identified. For example 5.0, 6.0 etc. There must not be any existing '
         'instances with that tag.'), type=str)

    subparsers = argsParser.add_subparsers(title='subcommands')

    # Arguments for the 'launch' command
    parser_launch = subparsers.add_parser(
        'launch', help='Launches the EC2 hosts which will comprise the cluster')
    parser_launch.add_argument('--shard-repl-set-nodes',
                               help='Number of nodes to use for the shard replica sets.', type=int,
                               default=3)
    parser_launch.add_argument('--filesystem', choices=['xfs', 'ext4'],
                               help='Number of nodes to use for the shard replica sets.',
                               default='xfs')
    parser_launch.set_defaults(func=main_launch)

    # Arguments for the 'describe' command
    parser_describe = subparsers.add_parser(
        'describe', help='Describes all the hosts which comprise the cluster')
    parser_describe.set_defaults(func=main_describe)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    args = argsParser.parse_args()
    logging.info(f"Starting with arguments: '{args}'")

    ec2 = boto3.client('ec2')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(args.func(args, ec2))
