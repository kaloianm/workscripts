#!/usr/bin/env python3
#
help_string = '''
Tool to create and manipulate a MongoDB sharded cluster given SSH and MongoDB port access to a set
of hosts. When launching the EC2 hosts, please ensure that the machine from which they are being
connected to has access in the inbound rules.

The intended usage is:

1. Use the `launch_ec2_cluster_hosts.py Cluster launch` script in order to spawn a set of hosts in
   EC2 on which the MongoDB processes will run.
2. This script will create a directory named after the cluster tag containing a
   deployment_description.json file with the host configuration.
3. Update the 'MongoBinPath' parameter in the deployment_description.json and run the
   `remote_control_cluster.py Tag create` command in order to launch the processes.

Use --help for more information on the supported commands.
'''

import argparse
import asyncio
import json
import logging
import motor.motor_asyncio
import os
import sys

from common.common import yes_no
from common.remote_mongo import (cleanup_mongo_directories, deploy_binaries, gather_logs,
                                 initiate_replica_set, make_remote_mongo_host, rsync_to_hosts,
                                 start_mongod_as_replica_set, stop_mongo_processes)
from common.version import CTOOLS_VERSION
from signal import Signals

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class ClusterBuilder:
    '''
    Wraps information and common management tasks for the whole cluster
    '''

    def __init__(self, cluster_config):
        '''
        Constructs a cluster object from a JSON object with the following fields:
         Name: Name for the cluster (used only for logging purposes)
         Hosts: Array of JSON objects, each of which must follow the format for RemoteMongoHost
          above
        '''

        self.config = cluster_config

        self.name = self.config['Name']
        self.feature_flags = [f'--setParameter {f}=true' for f in self.config["FeatureFlags"]
                              ] if "FeatureFlags" in self.config else []
        self.mongod_parameters = self.config['MongoDParameters'] + self.feature_flags
        self.mongos_parameters = self.config['MongoSParameters'] + self.feature_flags

        # Map host index to role
        role_map = {
            0: 'config',
            1: 'config',
            2: 'config',
            3: 'shard0',
            4: 'shard0',
            5: 'shard0',
            6: 'shard1',
            7: 'shard1',
            8: 'shard1'
        }

        self.available_hosts = []
        for idx, host_info in enumerate(self.config['Hosts']):
            if idx not in role_map:
                raise Exception(f'Too many hosts ({len(self.config["Hosts"])}), expected at most 9')
            self.available_hosts.append(
                make_remote_mongo_host(host_info, self.config, role_map[idx]))

        logging.info(
            f"Cluster will consist of: {list(map(lambda h: (h.host_desc['host'], h.host_desc['shard']), self.available_hosts))}"
        )

        # Split the cluster hosts into 3 replica sets
        self.config_hosts = self.available_hosts[0:3]
        self.shard0_hosts = self.available_hosts[3:6]
        self.shard1_hosts = self.available_hosts[6:9]

    @property
    def connection_string(self):
        '''
        Chooses the connection string for the cluster. It effectively selects the config server
        hosts because they contain no load.
        '''

        return f'mongodb://{self.config_hosts[1].host}'

    async def get_description(self):
        return f'''
Cluster {self.name} started with:
  MongoS: {self.connection_string}
  ConfigServer: {self.config_hosts}
  Shard0: {self.shard0_hosts}
  Shard1: {self.shard1_hosts}
'''


async def start_config_replica_set(cluster):
    logging.info('Starting config processes')

    await start_mongod_as_replica_set(
        cluster.config_hosts,
        27019,
        'config',
        [
            '--configsvr',
        ] + cluster.mongod_parameters,
    )


async def start_shard_replica_set(cluster, shard_hosts, shard_name):
    logging.info(f'Starting shard processes for {shard_name}')

    await start_mongod_as_replica_set(
        shard_hosts,
        27018,
        shard_name,
        [
            '--shardsvr',
        ] + cluster.mongod_parameters,
    )


async def start_mongos_processes(cluster):
    logging.info('Starting mongos processes ...')

    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.create_task(
                host.start_mongos_instance(
                    27017,
                    cluster.config_hosts,
                    cluster.mongos_parameters,
                )))
    await asyncio.gather(*tasks)

    logging.info('Mongos processes started')


##################################################################################################
#
# Main methods implementations for the various sub-commands
#
##################################################################################################


async def main_create(args, cluster):
    '''Implements the create command'''

    yes_no(('Start creating the cluster from scratch.'
            'WARNING: The next steps will erase all existing data on the specified hosts.'))

    await stop_mongo_processes(cluster.available_hosts, Signals.SIGKILL)
    await cleanup_mongo_directories(cluster.available_hosts)
    await deploy_binaries(cluster.available_hosts, cluster.config['MongoBinPath'])
    await start_config_replica_set(cluster)
    await start_shard_replica_set(cluster, cluster.shard0_hosts, 'shard0')
    await start_shard_replica_set(cluster, cluster.shard1_hosts, 'shard1')

    await initiate_replica_set(cluster.config_hosts, 27019, 'config')
    await initiate_replica_set(cluster.shard0_hosts, 27018, 'shard0')
    await initiate_replica_set(cluster.shard1_hosts, 27018, 'shard1')

    # MongoS instances
    await start_mongos_processes(cluster)

    logging.info(f'Connecting to {cluster.connection_string}')

    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(cluster.connection_string)
    logging.info(await mongo_client.admin.command({
        'addShard': f'shard0/{cluster.shard0_hosts[0].host}:27018',
        'name': 'shard0',
    }))
    logging.info(await mongo_client.admin.command({
        'addShard': f'shard1/{cluster.shard1_hosts[0].host}:27018',
        'name': 'shard1',
    }))

    logging.info(await cluster.get_description())


async def main_describe(args, cluster):
    logging.info(await cluster.get_description())


async def main_start(args, cluster):
    '''Implements the start command'''

    if not args.shard or args.shard == 'config':
        await start_config_replica_set(cluster)
    if not args.shard or args.shard == 'shard0':
        await start_shard_replica_set(cluster, cluster.shard0_hosts, 'shard0')
    if not args.shard or args.shard == 'shard1':
        await start_shard_replica_set(cluster, cluster.shard1_hosts, 'shard1')

    await start_mongos_processes(cluster)


async def main_stop(args, cluster):
    '''Implements the stop command'''

    await stop_mongo_processes(cluster.available_hosts, args.signal, args.shard)


async def main_run(args, cluster):
    '''Implements the run command'''

    tasks = []
    for host in cluster.available_hosts:
        if args.shard and host.host_desc['shard'] != args.shard:
            continue

        tasks.append(asyncio.create_task(host.exec_remote_ssh_command(args.command)))
    await asyncio.gather(*tasks)


async def main_rsync(args, cluster):
    '''Implements the rsync command'''

    await rsync_to_hosts(cluster.available_hosts, args.local_pattern, args.remote_path, args.shard)


async def main_deploy_binaries(args, cluster):
    '''Implements the deploy-binaries command'''

    await deploy_binaries(cluster.available_hosts, cluster.config['MongoBinPath'], args.shard)


async def main_gather_logs(args, cluster):
    '''Implements the gather-logs command'''

    await gather_logs(cluster.available_hosts, cluster.name, args.local_path, args.shard)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument('clustertag',
                            help='Cluster tag directory containing deployment_description.json',
                            type=str)
    subparsers = argsParser.add_subparsers(title='subcommands')

    ###############################################################################################
    # Arguments for the 'create' command
    parser_create = subparsers.add_parser('create',
                                          help='Creates (or overwrites) a brand new cluster')
    parser_create.set_defaults(func=main_create)

    ###############################################################################################
    # Arguments for the 'describe' command
    parser_describe = subparsers.add_parser('describe', help='Describes the nodes of a cluster')
    parser_describe.set_defaults(func=main_describe)

    ###############################################################################################
    # Arguments for the 'start' command
    parser_start = subparsers.add_parser(
        'start', help='Starts all the processes of an already created cluster')
    parser_start.add_argument('--shard', nargs='?', type=str,
                              help='Limit the command to just one shard')
    parser_start.set_defaults(func=main_start)

    ###############################################################################################
    # Arguments for the 'stop' command
    parser_stop = subparsers.add_parser(
        'stop',
        help='Stops all the processes of an already created cluster using a specified signal')
    parser_stop.add_argument(
        '--signal', type=lambda x: Signals[x],
        help=f'The signal to use for terminating the processes. One of {[e.name for e in Signals]}',
        default=Signals.SIGTERM)
    parser_stop.add_argument('--shard', nargs='?', type=str,
                             help='Limit the command to just one shard')
    parser_stop.set_defaults(func=main_stop)

    ###############################################################################################
    # Arguments for the 'run' command
    parser_run = subparsers.add_parser('run', help='Runs a command')
    parser_run.add_argument('command', help='The command to run')
    parser_run.add_argument('--shard', nargs='?', type=str,
                            help='Limit the command to just one shard')
    parser_run.set_defaults(func=main_run)

    ###############################################################################################
    # Arguments for the 'rsync' command
    parser_rsync = subparsers.add_parser('rsync',
                                         help='Rsyncs a set of file from a local to remote path')
    parser_rsync.add_argument('local_pattern', help='The local pattern from which to rsync')
    parser_rsync.add_argument('remote_path', help='The remote path to which to rsync')
    parser_rsync.add_argument('--shard', nargs='?', type=str,
                              help='Limit the command to just one shard')
    parser_rsync.set_defaults(func=main_rsync)

    ###############################################################################################
    # Arguments for the 'deploy-binaries' command
    parser_deploy_binaries = subparsers.add_parser(
        'deploy-binaries',
        help='Specialisation of the rsync command which only deploys binaries from MongoBinPath')
    parser_deploy_binaries.add_argument('--shard', nargs='?', type=str,
                                        help='Limit the command to just one shard')
    parser_deploy_binaries.set_defaults(func=main_deploy_binaries)

    ###############################################################################################
    # Arguments for the 'gather-logs' command
    parser_gather_logs = subparsers.add_parser(
        'gather-logs',
        help='Compresses and rsyncs the set of logs from the cluster to a local directory')
    parser_gather_logs.add_argument('local_path', help='The local to which to rsync')
    parser_gather_logs.add_argument('--shard', nargs='?', type=str,
                                    help='Limit the command to just one shard')
    parser_gather_logs.set_defaults(func=main_gather_logs)

    ###############################################################################################

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    config_file = os.path.join(args.clustertag, 'deployment_description.json')
    with open(config_file) as f:
        cluster_config = json.load(f)

    logging.info(f'Configuration: {json.dumps(cluster_config, indent=2)}')
    cluster = ClusterBuilder(cluster_config)

    asyncio.run(args.func(args, cluster))
