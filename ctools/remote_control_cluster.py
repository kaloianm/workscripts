#!/usr/bin/env python3
#
help_string = '''
Tool to create and manipulate a MongoDB sharded cluster given SSH access to a set of hosts. The
intended usage is:

  1. Create 9 instances in the AWS console
  2. Run the following command in order to obtain the list of instances:
       aws ec2 describe-instances \
         --filters "Name=tag:owner,Values=kaloian.manassiev" \
         --query "Reservations[].Instances[].PublicDnsName[]"
  3. Create a JSON file with the following format:
        {
                "Name": "Test Cluster",
                "Hosts": [
                    # The output from step (2)
                ],
                "MongoBinPath": "<Path where the MongoDB binaries are stored>",
                "FeatureFlags": [ "<List of strings with feature flag names to enable>" ]
        }
  4. ./remote_control_cluster.py create <File from step (3)>

See the help for more supported commands.
'''

import argparse
import asyncio
import json
import logging
import sys

from pymongo import MongoClient
from remote_common import RemoteSSHHost
from signal import Signals

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class RemoteMongoHost(RemoteSSHHost):
    '''
    Specialisation of 'RemoteSSHHost' which will be running MongoDB services (mongod/mongos, etc)
    '''

    def __init__(self, host_desc):
        '''
        Constructs a cluster host object from host description, which is a superset of the
        description required by 'RemoteSSHHost' above. The additional fields are:
          mongod_data_path: Path on the host to serve as a root for the MongoD service's data and
            logs. Defaulted to $HOME/mongod_data.
          mongos_data_path: Path on the host to serve as a root for the MongoS service's data and
            logs. Defaulted to $HOME/mongos_data.
        '''

        RemoteSSHHost.__init__(self, host_desc)

        default_mongod_data_path = '$HOME/mongod_data'
        default_mongos_data_path = '$HOME/mongos_data'

        # Populate parameter defaults
        if 'mongod_data_path' not in self.host_desc:
            self.host_desc['mongod_data_path'] = default_mongod_data_path
        if 'mongos_data_path' not in self.host_desc:
            self.host_desc['mongos_data_path'] = default_mongos_data_path

    async def start_mongod_instance(self, port, repl_set_name, extra_args=[]):
        '''
        Starts a single MongoD instance on this host, as part of a replica set called 'repl_set_name',
        listening on 'port'. The 'extra_args' is a list of additional command line arguments to
        specify to the mongod command line.
        '''

        await self.exec_remote_ssh_command(
            (f'mkdir -p {self.host_desc["mongod_data_path"]} && '
             f'$HOME/binaries/mongod --replSet {repl_set_name} '
             f'--dbpath {self.host_desc["mongod_data_path"]} '
             f'--logpath {self.host_desc["mongod_data_path"]}/mongod.log '
             f'--port {port} --bind_ip_all '
             f'--fork '
             f'{" ".join(extra_args)}'))

    async def start_mongos_instance(self, port, config_server, extra_args=[]):
        '''
        Starts a single MongoS instance on this host, listening on 'port', pointing to
        'config_server'. The 'extra_args' is a list of additional command line arguments to specify
        to the mongos command line.
        '''

        await self.exec_remote_ssh_command(
            (f'mkdir -p {self.host_desc["mongos_data_path"]} && '
             f'$HOME/binaries/mongos --configdb config/{config_server.host}:27019 '
             f'--logpath {self.host_desc["mongos_data_path"]}/mongos.log '
             f'--port {port} --bind_ip_all '
             f'--fork '
             f'{" ".join(extra_args)}'))


class Cluster:
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

        self.available_hosts = list(
            map(lambda host_info: RemoteMongoHost(host_info), self.config['Hosts']))

        # Split the cluster hosts into 3 replica sets
        self.shard0_hosts = self.available_hosts[0:3]
        self.shard1_hosts = self.available_hosts[3:6]
        self.config_server_hosts = self.available_hosts[6:9]

    async def start_mongod_as_replica_set(self, hosts, port, repl_set_name, extra_args):
        '''
        Starts the mongod processes on the specified 'hosts', where each host will be listening on
        'port' and will be initiated as part of 'repl_set_name'. The 'extra_args' is a list of
        additional command line arguments to specify to the mongod command line.
        '''

        # Start the Replica Set hosts
        tasks = []
        for host in hosts:
            tasks.append(
                asyncio.ensure_future(host.start_mongod_instance(port, repl_set_name, extra_args)))
        await asyncio.gather(*tasks)

    async def rsync(self, local_pattern, remote_path):
        '''
        Rsyncs all the files that match 'local_pattern' to the 'remote_path' on all nodes
        '''

        tasks = []
        for host in self.available_hosts:
            tasks.append(
                asyncio.ensure_future(host.rsync_files_to_remote(local_pattern, remote_path)))
        await asyncio.gather(*tasks)


async def stop_all_mongo_processes(cluster, signal):
    '''
    Stops processes that might have been left over from a previous run, on all nodes
    '''

    logging.info('Stopping mongo processes')
    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.exec_remote_ssh_command(
                    (f'killall --wait -s {signal.name} mongo mongod mongos || true'))))
    await asyncio.gather(*tasks)


async def cleanup_mongo_directories(cluster):
    '''
    Cleanup directories that might have been left over from a previous run, on all nodes
    '''

    logging.info('Cleaning leftover directories')
    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.exec_remote_ssh_command((f'rm -rf {host.host_desc["mongod_data_path"]} ;'
                                              f'rm -rf {host.host_desc["mongos_data_path"]}'))))
    await asyncio.gather(*tasks)


async def install_prerequisite_packages(cluster):
    '''
    Install (using apt) all the prerequisite libraries that the mongodb binaries require, on all nodes
    '''

    logging.info('Installing prerequisite packages')
    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.exec_remote_ssh_command('sudo apt update && sudo apt -y install libsnmp-dev')))
    await asyncio.gather(*tasks)


async def start_mongod_processes(cluster):
    tasks = []

    # Shard server-only parameters
    shard_extra_parameters = [
        '--setParameter rangeDeleterBatchSize=100000',
        '--setParameter orphanCleanupDelaySecs=0',
    ] + cluster.feature_flags

    # Shard(s)
    tasks.append(
        asyncio.ensure_future(
            cluster.start_mongod_as_replica_set(cluster.shard0_hosts, 27018, 'shard0', [
                '--shardsvr',
            ] + shard_extra_parameters)))

    tasks.append(
        asyncio.ensure_future(
            cluster.start_mongod_as_replica_set(cluster.shard1_hosts, 27018, 'shard1', [
                '--shardsvr',
            ] + shard_extra_parameters)))

    # Config server-only parameters
    config_extra_parameters = [] + cluster.feature_flags

    # Config Server
    tasks.append(
        asyncio.ensure_future(
            cluster.start_mongod_as_replica_set(cluster.config_server_hosts, 27019, 'config', [
                '--configsvr',
            ] + config_extra_parameters)))

    await asyncio.gather(*tasks)


async def start_mongos_processes(cluster):
    # MongoS-only parameters
    mongos_extra_parameters = [] + cluster.feature_flags

    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.start_mongos_instance(27017, cluster.config_server_hosts[0],
                                           mongos_extra_parameters)))
    await asyncio.gather(*tasks)


async def main_create(args, cluster):
    '''Implements the create command'''

    await stop_all_mongo_processes(cluster, Signals.SIGKILL)
    await cleanup_mongo_directories(cluster)
    await install_prerequisite_packages(cluster)
    await cluster.rsync(f'{cluster.config["MongoBinPath"]}/*', '$HOME/binaries')
    await start_mongod_processes(cluster)

    async def initiate_replica_set(hosts, port, repl_set_name):
        '''
        Initiates 'hosts' as a replica set with a name of 'repl_set_name' and 'hosts':'port' as
        members
        '''

        # Initiate the Replica Set
        connection_string = f'mongodb://{hosts[0].host}:{port}'
        logging.info(f'Connecting to {connection_string} in order to initiate it as a replica set')

        with MongoClient(connection_string) as mongo_client:
            replica_set_members_list = list(
                map(
                    lambda id_and_host: {
                        '_id': id_and_host[0],
                        'host': f'{id_and_host[1].host}:{port}'
                    }, zip(range(0, len(hosts)), hosts)))

            logging.info(
                mongo_client.admin.command({
                    'replSetInitiate': {
                        '_id': repl_set_name,
                        'members': replica_set_members_list,
                    }
                }))

    await initiate_replica_set(cluster.shard0_hosts, 27018, 'shard0')
    await initiate_replica_set(cluster.shard1_hosts, 27018, 'shard1')
    await initiate_replica_set(cluster.config_server_hosts, 27019, 'config')

    # MongoS instances
    await start_mongos_processes(cluster)

    mongos_connection_string = f'mongodb://{cluster.available_hosts[0].host}'
    logging.info(f'Connecting to {mongos_connection_string}')

    mongo_client = MongoClient(mongos_connection_string)
    logging.info(
        mongo_client.admin.command({
            'addShard': f'shard0/{cluster.shard0_hosts[0].host}:27018',
            'name': 'shard0'
        }))
    logging.info(
        mongo_client.admin.command({
            'addShard': f'shard1/{cluster.shard1_hosts[0].host}:27018',
            'name': 'shard1'
        }))

    logging.info(f'''
Cluster {cluster.name} started with:
  MongoS: {mongos_connection_string}
  ConfigServer: {cluster.config_server_hosts}
  Shard0: {cluster.shard0_hosts}
  Shard1: {cluster.shard1_hosts}
''')


async def main_stop(args, cluster):
    '''Implements the stop command'''

    await stop_all_mongo_processes(cluster, args.signal)


async def main_start(args, cluster):
    '''Implements the start command'''

    await start_mongod_processes(cluster)
    await start_mongos_processes(cluster)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    argsParser.add_argument(
        'clusterconfigfile',
        help='JSON-formatted text file which contains the configuration of the cluster', type=str)
    subparsers = argsParser.add_subparsers(title='subcommands')

    # Arguments for the 'create' command
    parser_create = subparsers.add_parser('create',
                                          help='Creates (or overwrites) a brand new cluster')
    parser_create.set_defaults(func=main_create)

    # Arguments for the 'stop' command
    parser_stop = subparsers.add_parser(
        'stop',
        help='Stops all the processes of an already created cluster using a specified signal')
    parser_stop.add_argument(
        'signal', type=lambda x: Signals[x],
        help=f'The signal to use for terminating the processes. One of {[e.name for e in Signals]}')
    parser_stop.set_defaults(func=main_stop)

    # Arguments for the 'start' command
    parser_start = subparsers.add_parser(
        'start', help='Starts all the processes of an already created cluster')
    parser_start.set_defaults(func=main_start)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    args = argsParser.parse_args()

    with open(args.clusterconfigfile) as f:
        cluster_config = json.load(f)

    logging.info(f"Starting with configuration: '{cluster_config}'")
    cluster = Cluster(cluster_config)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(args.func(args, cluster))
