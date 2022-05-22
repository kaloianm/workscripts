#!/usr/bin/env python3
#

#
# 1. Create 9 instances in the AWS console
# 2. Run the following command in order to obtain the list of instances
#      aws ec2 describe-instances --filters "Name=tag:owner,Values=kaloian.manassiev" --query "Reservations[].Instances[].PublicDnsName[]"
#    and paste the output under the 'available_hosts' variable below
#

import argparse
import asyncio
import json
import logging
import sys

from pymongo import MongoClient

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class RemoteHost:
    '''
    Wraps the common management tasks and the description of a remote host which will be part of the cluster.
    '''

    def __init__(self, host_desc):
        '''
        '''

        default_ssh_user_name = 'ubuntu'

        if (isinstance(host_desc, str)):
            self.host_desc = {
                'host': host_desc,
                'ssh_username': default_ssh_user_name,
                'ssh_args': '-o StrictHostKeyChecking=no',
                'mongod_data_path': '$HOME/mongod_data',
                'mongos_data_path': '$HOME/mongos_data',
            }
        else:
            self.host_desc = host_desc

        self.host = self.host_desc['host']

    def __repr__(self):
        return self.host

    async def exec_remote_ssh_command(self, command):
        '''
        Runs the specified command on the remote host under the SSH credentials configuration above
        '''

        ssh_command = f'ssh {self.host_desc["ssh_args"]} {self.host_desc["ssh_username"]}@{self.host_desc["host"]} "{command}"'
        logging.info(f'Executing ({self.host_desc["host"]}): {ssh_command}')
        ssh_process = await asyncio.create_subprocess_shell(ssh_command)
        await ssh_process.wait()

        if ssh_process.returncode != 0:
            raise Exception(
                f'SSH command on host {self.host_desc["host"]} failed with code {ssh_process.returncode}'
            )

    async def rsync_files_to_remote(self, source_pattern, destination_path):
        '''
        Uses rsync to copy files matching 'source_pattern' to 'destination_path'
        '''

        rsync_command = f'rsync -e "ssh {self.host_desc["ssh_args"]}" --progress -r -t {source_pattern} {self.host_desc["ssh_username"]}@{self.host_desc["host"]}:{destination_path}'
        logging.info(f'Executing ({self.host_desc["host"]}): {rsync_command}')
        rsync_process = await asyncio.create_subprocess_shell(rsync_command)
        await rsync_process.wait()

        if rsync_process.returncode != 0:
            raise Exception(
                f'RSYNC command on host {self.host_desc["host"]} failed with code {rsync_process.returncode}'
            )

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
    Wraps the common information for the cluster
    '''

    def __init__(self, cluster_config):
        '''
        '''

        self.config = cluster_config

        self.name = cluster_config['Name']
        self.available_hosts = list(
            map(lambda host_info: RemoteHost(host_info), cluster_config['Hosts']))

        self.shard0_hosts = self.available_hosts[0:3]
        self.shard1_hosts = self.available_hosts[3:6]
        self.config_server_hosts = self.available_hosts[6:9]


async def cleanup_leftover_processes(cluster):
    '''
    Cleanup processes that might have been left over from a previous run, on all nodes.
    '''

    logging.info('Killing leftover processes')
    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.exec_remote_ssh_command((f'killall -9 mongo mongod mongos ;'
                                              f'rm -rf {host.host_desc["mongod_data_path"]} ;'
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


async def copy_binaries(cluster, mongo_binaries_path):
    '''
    Copy the mongodb binaries to all nodes
    '''

    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.rsync_files_to_remote(f'{mongo_binaries_path}/*', '$HOME/binaries')))
    await asyncio.gather(*tasks)


async def start_shards_and_config_server(cluster):
    '''
    Start the mongod service on each of the hosts and initiate them as replica sets
    '''

    async def make_replica_set(hosts, port, repl_set_name, extra_args):
        '''
        Makes a replica set out of the specified 'hosts', where each host will be listening on
        'port' and will be initiated as part of 'repl_set_name'. The 'extra_args' is a list of
        additional command line arguments to specify to the mongod command line.
        '''

        # Start the Replica Set hosts
        tasks = []
        for host in hosts:
            tasks.append(
                asyncio.ensure_future(host.start_mongod_instance(port, repl_set_name, extra_args)))
        await asyncio.gather(*tasks)

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

    # Shard(s)
    await make_replica_set(cluster.shard0_hosts, 27018, 'shard0', [
        '--shardsvr',
        '--setParameter rangeDeleterBatchSize=100000',
        '--setParameter orphanCleanupDelaySecs=0',
    ])
    await make_replica_set(cluster.shard1_hosts, 27018, 'shard1', [
        '--shardsvr',
        '--setParameter rangeDeleterBatchSize=100000',
        '--setParameter orphanCleanupDelaySecs=0',
    ])

    # Config Server
    await make_replica_set(cluster.config_server_hosts, 27019, 'config', [
        '--configsvr',
    ])


async def make_cluster(cluster):
    '''
    Start the mongos service on each of the hosts and add the newly created shards to the cluster
    '''

    tasks = []
    for host in cluster.available_hosts:
        tasks.append(
            asyncio.ensure_future(
                host.start_mongos_instance(27017, cluster.config_server_hosts[0])))
    await asyncio.gather(*tasks)

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

    logging.info(f"""
Cluster {cluster.name} started with:
  MongoS: {mongos_connection_string}
  ConfigServer: {cluster.config_server_hosts}
  Shard0: {cluster.shard0_hosts}
  Shard1: {cluster.shard1_hosts}
""")


async def main(args):
    '''Main entrypoint of the application'''

    with open(args.clusterconfigfile) as f:
        cluster_config = json.load(f)

    logging.info(f"Starting cluster generation with configuration: '{cluster_config}'")
    cluster = Cluster(cluster_config)

    await cleanup_leftover_processes(cluster)
    await install_prerequisite_packages(cluster)
    await copy_binaries(cluster, cluster_config['MongoBinPath'])
    await start_shards_and_config_server(cluster)
    await make_cluster(cluster)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description='Tool to create a fully working cluster given SSH access to a set of hosts')
    argsParser.add_argument(
        'clusterconfigfile',
        help='JSON-formatted text file which contains the desired configuration of the cluster',
        type=str)

    args_list = " ".join(sys.argv[1:])

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    args = argsParser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
