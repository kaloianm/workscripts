#!/usr/bin/env python3
#
help_string = '''
Tool to launch a set of clean (in the sense without MongoDB on them) EC2 hosts which can be used for
constructing a sharded MongoDB cluster.

Since it interacts with AWS, a default region_name should be set in ~/.aws/config and the AWS
parameters should be specified either in the same config file or as environment variables.

Use --help for more information on the supported commands.
'''

import argparse
import boto3
import json
import logging
import sys

from common.common import yes_no
from common.ec2_instances import (CLIENT_HOST_TEMPLATE, describe_all_instances,
                                  filter_instances_by_role, launch_instances, load_template,
                                  make_client_driver_host_configuration,
                                  make_cluster_host_configuration, make_instance_tag_specifications,
                                  wait_for_instances)
from common.version import CTOOLS_VERSION

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


def describe_cluster(ec2, clustertag):
    all_instances = describe_all_instances(ec2, clustertag)

    cluster_json = {
        "Name":
            clustertag,
        "Hosts":
            list(
                map(
                    lambda x: x["PublicDnsName"],
                    filter_instances_by_role(all_instances, 'config') +
                    filter_instances_by_role(all_instances, 'shard0') +
                    filter_instances_by_role(all_instances, 'shard1'))),
        "DriverHosts":
            list(
                map(lambda x: x["PublicDnsName"], filter_instances_by_role(all_instances,
                                                                           'driver'))),
        "MongoBinPath":
            "<Substitute with the local binaries path>",
        "RemoteMongoDPath":
            "/mnt/data/mongod",
        "RemoteMongoSPath":
            "/mnt/data/mongos",
        "FeatureFlags": [],
        "MongoDParameters": ["--wiredTigerCacheSizeGB 18", ],
        "MongoSParameters": [],
    }

    return json.dumps(cluster_json, indent=2, separators=(', ', ': '))


def main_launch(args, ec2):
    '''Implementation of the launch command'''

    template = load_template(args.template)
    client_template = load_template(CLIENT_HOST_TEMPLATE)

    ##############################################################################################
    # DRIVER INSTANCES

    client_driver_instances = launch_instances(
        ec2,
        client_template,
        tag_specs=make_instance_tag_specifications(args.clustertag, 'driver'),
        user_data=make_client_driver_host_configuration(args.clustertag),
        count=1,
    )

    ##############################################################################################
    # SHARD INSTANCES

    # Config instances
    config_instances = launch_instances(
        ec2,
        template,
        tag_specs=make_instance_tag_specifications(args.clustertag, 'config'),
        user_data=make_cluster_host_configuration(args.clustertag, args.filesystem),
        count=3,
    )

    # Shard(s) instances
    shard_instances = []
    for shard_id in ['shard0', 'shard1']:
        shard_instances += launch_instances(
            ec2,
            template,
            tag_specs=make_instance_tag_specifications(args.clustertag, shard_id),
            user_data=make_cluster_host_configuration(args.clustertag, args.filesystem),
            count=args.shard_repl_set_nodes,
        )

    wait_for_instances(ec2, client_driver_instances + config_instances + shard_instances)

    cluster_desc = describe_cluster(ec2, args.clustertag)
    with open('cluster.json', 'w') as f:
        f.write(cluster_desc)
    print(cluster_desc)

    logging.info(
        f'To deploy binaries to cluster, now run ./remote_control_cluster.py cluster.json create  ...'
    )


def main_terminate(args, ec2):
    '''Implementation of the terminate command'''
    cluster_instances = list(
        map(lambda x: x['InstanceId'], describe_all_instances(ec2, args.clustertag)))

    if (len(cluster_instances) == 0):
        raise Exception(f'No hosts found with the cluster tag {args.clustertag}')

    yes_no(f'About to terminate {len(cluster_instances)} instances')
    ec2.terminate_instances(InstanceIds=cluster_instances)


def main_describe(args, ec2):
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
    parser_launch.add_argument(
        '--template', required=True,
        help='Path to a JSON file with EC2 instance parameters (e.g. Atlas-M60.json).')
    parser_launch.add_argument('--shard-repl-set-nodes',
                               help='Number of nodes to use for the shard replica sets.', type=int,
                               default=3)
    parser_launch.add_argument('--filesystem', choices=['xfs', 'ext4'],
                               help='Filesystem to use for the data volume.', default='xfs')
    parser_launch.set_defaults(func=main_launch)

    ###############################################################################################
    # Arguments for the 'terminate' command
    parser_terminate = subparsers.add_parser('terminate',
                                             help='Terminates the EC2 hosts for a cluster.')
    parser_terminate.set_defaults(func=main_terminate)

    ###############################################################################################
    # Arguments for the 'describe' command
    parser_describe = subparsers.add_parser(
        'describe', help='Describes all the hosts which comprise the cluster')
    parser_describe.set_defaults(func=main_describe)

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    ec2_instance = boto3.client('ec2', region_name='us-east-1')

    args.func(args, ec2_instance)
