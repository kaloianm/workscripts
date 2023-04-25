#!/usr/bin/env python3
#
help_string = '''
Tool to launch a set of clean (in the sense without MongoDB on them) EC2 hosts which can be used for
constructing a MongoDB cluster.

Since it interacts with AWS, a default region_name should be set in $HOME/.aws/config and the AWS
parameters should be specified either in the same config file or as environment variables.

Use --help for more information on the supported commands.
'''

import argparse
import asyncio
import boto3
import json
import logging
import sys

from common.common import yes_no
from common.version import CTOOLS_VERSION

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

#
# Host configuration scripts. Their output will show up in /var/log/cloud-init-output.log.
#

ANY_HOST_CONFIGURATION = '''#!/bin/bash
set -e

export UBUNTU_USER_HOME_DIR=`getent passwd ubuntu | cut -d: -f6`

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


def make_client_driver_host_configuration(clustertag):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring driver host workscripts for {clustertag}"
###################################################################################################
'''


def make_cluster_host_configuration(clustertag, filesystem):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring shard host volumes for {clustertag}"
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
sudo mkdir /mnt/data
sudo mount /dev/nvme1n1p1 /mnt/data
sudo chown -R ubuntu:ubuntu /mnt/data

echo "Data volume mounted, persisting mount point so it survives reboots ..."
if [ -z $(grep "/mnt/data" "/etc/fstab") ]; then echo $(cat "/proc/mounts" | grep "/mnt/data") >> /etc/fstab; fi

echo "Completed configuration for {clustertag} !"
'''


def make_instance_tag_specifications(clustertag, role):
    '''Instantiates an AWS instance tag specification for the specified cluster node'''

    return [{
        'ResourceType':
            'instance',
        'Tags': [{
            'Key': 'mongoversion',
            'Value': clustertag
        }, {
            'Key': 'mongorole',
            'Value': role
        }, {
            'Key': 'noreap',
            'Value': 'true'
        }]
    }]


def describe_all_instances(ec2, clustertag):
    '''Issues a query to describe all running instances with the specified cluster tag'''

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
                'Value': role,
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
        "FeatureFlags": [],
        "MongoDParameters": ["--wiredTigerCacheSizeGB 11", ],
        "MongoSParameters": [],
    }

    return json.dumps(cluster_json, indent=2, separators=(', ', ': '))


async def main_launch(args, ec2):
    '''Implementation of the launch command'''

    ##############################################################################################
    # DRIVER INSTANCES

    client_driver_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        },
        InstanceType='m5.2xlarge',
        TagSpecifications=make_instance_tag_specifications(args.clustertag, 'driver'),
        UserData=make_client_driver_host_configuration(args.clustertag),
        MinCount=1,
        MaxCount=1,
    )['Instances']

    ##############################################################################################
    # SHARD INSTANCES

    cluster_node_block_device_mappings = [
        {
            'DeviceName': '/dev/sdf',
            'Ebs': {
                'VolumeType': 'gp3',
                'VolumeSize': 1500,
                'Iops': 6000
            },
        },
    ]

    # Config instances
    config_instances = ec2.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': 'lt-042b07169886af208',
        },
        InstanceType='m5.2xlarge',
        BlockDeviceMappings=cluster_node_block_device_mappings,
        TagSpecifications=make_instance_tag_specifications(args.clustertag, 'config'),
        UserData=make_cluster_host_configuration(args.clustertag, args.filesystem),
        MinCount=3,
        MaxCount=3,
    )['Instances']

    # Shard(s) instances
    shard_instances = []
    for shard_id in ['shard0', 'shard1']:
        shard_instances += ec2.run_instances(
            LaunchTemplate={
                'LaunchTemplateId': 'lt-042b07169886af208',
            },
            InstanceType='m5.2xlarge',
            BlockDeviceMappings=cluster_node_block_device_mappings,
            TagSpecifications=make_instance_tag_specifications(args.clustertag, shard_id),
            UserData=make_cluster_host_configuration(args.clustertag, args.filesystem),
            MinCount=args.shard_repl_set_nodes,
            MaxCount=args.shard_repl_set_nodes,
        )['Instances']

    logging.info('Instances launched, now waiting for them to start running ...')

    waiter = ec2.get_waiter('instance_running')
    waiter.wait(
        InstanceIds=list(
            map(lambda x: x['InstanceId'], client_driver_instances + config_instances +
                shard_instances)))

    print(describe_cluster(ec2, args.clustertag))

    logging.info(
        f'To deploy binaries to cluster, now run ./remote_control_cluster.py {args.clustertag}.cluster create  ...'
    )


async def main_terminate(args, ec2):
    '''Implementation of the terminate command'''
    cluster_instances = list(
        map(lambda x: x['InstanceId'], describe_all_instances(ec2, args.clustertag)))

    if (len(cluster_instances) == 0):
        raise Exception(f'No hosts found with the cluster tag {args.clustertag}')

    yes_no(f'About to terminate {len(cluster_instances)} instances')
    ec2.terminate_instances(InstanceIds=cluster_instances)


async def main_describe(args, ec2):
    '''Implementation of the describe command'''
    cluster_desc = describe_cluster(ec2, args.clustertag)
    print(cluster_desc)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument(
        'clustertag', help=
        ('String with which to tag all the instances which will be spawned for this cluster so they '
         'can easily be identified. For example 5.0, 6.0 etc. There must not be any existing '
         'instances with that tag.'), type=str)

    subparsers = argsParser.add_subparsers(title='subcommands')

    ###############################################################################################
    # Arguments for the 'launch' command
    parser_launch = subparsers.add_parser(
        'launch', help='Launches the EC2 hosts which will comprise the cluster.')
    parser_launch.add_argument('--shard-repl-set-nodes',
                               help='Number of nodes to use for the shard replica sets.', type=int,
                               default=3)
    parser_launch.add_argument('--filesystem', choices=['xfs', 'ext4'],
                               help='Number of nodes to use for the shard replica sets.',
                               default='xfs')
    parser_launch.set_defaults(func=main_launch)

    ###############################################################################################
    # Arguments for the 'terminate' command
    parser_launch = subparsers.add_parser('terminate',
                                          help='Terminates the EC2 hosts for a cluster.')
    parser_launch.set_defaults(func=main_terminate)

    ###############################################################################################
    # Arguments for the 'describe' command
    parser_describe = subparsers.add_parser(
        'describe', help='Describes all the hosts which comprise the cluster')
    parser_describe.set_defaults(func=main_describe)

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    ec2_instance = boto3.client('ec2')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(args.func(args, ec2_instance))
