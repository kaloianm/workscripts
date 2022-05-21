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
import logging
import sys

from pymongo import MongoClient

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

# Set of hosts on which to place the cluster
available_hosts = [
    # TODO: Execute the following command in order to obtain the set of instances to use and paste
    # it in this array:
    #   aws ec2 describe-instances --filters "Name=tag:owner,Values=kaloian.manassiev" --query "Reservations[].Instances[].PublicDnsName[]"
]

shard0_hosts = available_hosts[0:3]
shard1_hosts = available_hosts[3:6]
config_server_hosts = available_hosts[6:9]

# SSH credentials
ssh_user_name = 'ubuntu'

# Directories to use on each cluster node
mongod_data_path = '$HOME/mongod_data'
mongos_data_path = '$HOME/mongos_data'


async def exec_remote_ssh_command(host, command):
    '''
    Runs the specified command on the remote host under the SSH credentials configuration above
    '''

    ssh_command = f'ssh -o StrictHostKeyChecking=no {ssh_user_name}@{host} "{command}"'
    logging.info(f'Executing ({host}): {ssh_command}')
    ssh_process = await asyncio.create_subprocess_shell(ssh_command)

    await ssh_process.wait()

    if ssh_process.returncode != 0:
        raise Exception(f'SSH command on host {host} failed with code {ssh_process.returncode}')


async def cleanup_leftover_processes():
    '''
    Cleanup processes that might have been left over from a previous run, on all nodes.
    '''

    logging.info('Killing leftover processes')
    tasks = []
    for host in available_hosts:
        tasks.append(
            asyncio.ensure_future(
                exec_remote_ssh_command(
                    host,
                    f'killall -9 mongo mongod mongos ; rm -rf {mongod_data_path} ; rm -rf {mongos_data_path}'
                )))
    await asyncio.gather(*tasks)


async def install_prerequisite_packages():
    '''
    Install (using apt) all the prerequisite libraries that the mongodb binaries require, on all nodes
    '''

    logging.info('Installing prerequisite packages')
    tasks = []
    for host in available_hosts:
        tasks.append(
            asyncio.ensure_future(
                exec_remote_ssh_command(host,
                                        'sudo apt update && sudo apt -y install libsnmp-dev')))
    await asyncio.gather(*tasks)


async def copy_binaries(mongo_binaries_path):
    '''
    Copy the mongodb binaries to all nodes
    '''

    async def run_copy_command(host):
        copy_command = f'rsync -e "ssh -o StrictHostKeyChecking=no" --progress -r -t {mongo_binaries_path}/* {ssh_user_name}@{host}:~/binaries'
        logging.info(f'Executing: {copy_command}')
        copy_process = await asyncio.create_subprocess_shell(copy_command)

        await copy_process.wait()

        if copy_process.returncode != 0:
            raise Exception(
                f'Copy command on host {host} failed with code {copy_process.returncode}')

    tasks = []
    for host in available_hosts:
        tasks.append(asyncio.ensure_future(run_copy_command(host)))
    await asyncio.gather(*tasks)


async def start_shards_and_config_server():
    '''
    Start the mongod service on each of the hosts and initiate them as replica sets
    '''

    async def start_mongod_instance(host, port, repl_set_name, extra_args):
        '''
        Starts a single MongoD instance on 'host' as part of a replica set called 'repl_set_name',
        listening on 'port'. The 'extra_args' is a list of additional command line arguments to
        specify to the mongod command line.
        '''

        await exec_remote_ssh_command(
            host, (f'mkdir -p {mongod_data_path} && '
                   f'~/binaries/mongod --replSet {repl_set_name} '
                   f'--dbpath {mongod_data_path} --logpath {mongod_data_path}/mongod.log '
                   f'--port {port} --fork --bind_ip_all '
                   f'--setParameter rangeDeleterBatchSize=100000 '
                   f'--setParameter orphanCleanupDelaySecs=0 '
                   f'{" ".join(extra_args)}'))

    async def make_replica_set(hosts, port, repl_set_name, extra_parameters):
        '''
        Makes a replica set out of the specified 'hosts', where each host will be listening on
        'port' and will be initiated as part of 'repl_set_name'. The 'extra_args' is a list of
        additional command line arguments to specify to the mongod command line.
        '''

        # Start the Replica Set hosts
        tasks = []
        for host in hosts:
            tasks.append(
                asyncio.ensure_future(
                    start_mongod_instance(host, port, repl_set_name, extra_parameters)))
        await asyncio.gather(*tasks)

        # Initiate the Replica Set
        connection_string = f'mongodb://{hosts[0]}:{port}'
        logging.info(f'Connecting to {connection_string} in order to initiate it as a replica set')

        with MongoClient(connection_string) as mongo_client:
            replica_set_members_list = list(
                map(lambda id_and_host: {
                    '_id': id_and_host[0],
                    'host': f'{id_and_host[1]}:{port}'
                }, zip(range(0, len(hosts)), hosts)))

            logging.info(
                mongo_client.admin.command({
                    'replSetInitiate': {
                        '_id': f'{repl_set_name}',
                        'members': replica_set_members_list,
                    }
                }))

    # Shard(s)
    await make_replica_set(shard0_hosts, 27018, 'shard0', ['--shardsvr'])
    await make_replica_set(shard1_hosts, 27018, 'shard1', ['--shardsvr'])

    # Config Server
    await make_replica_set(config_server_hosts, 27019, 'config', ['--configsvr'])


async def make_cluster():
    '''
    Start the mongos service on each of the hosts and add the newly created shards to the cluster
    '''

    tasks = []
    for mongos in available_hosts:
        tasks.append(
            asyncio.ensure_future(
                exec_remote_ssh_command(
                    mongos, (f'mkdir -p {mongos_data_path} && '
                             f'~/binaries/mongos --configdb config/{config_server_hosts[0]}:27019 '
                             f'--logpath {mongos_data_path}/mongos.log '
                             f'--fork --bind_ip_all'))))
    await asyncio.gather(*tasks)

    mongos_connection_string = f'mongodb://{available_hosts[0]}'
    logging.info(f'Connecting to {mongos_connection_string}')

    mongo_client = MongoClient(mongos_connection_string)
    logging.info(
        mongo_client.admin.command({
            'addShard': f'shard0/{shard0_hosts[0]}:27018',
            'name': 'shard0'
        }))
    logging.info(
        mongo_client.admin.command({
            'addShard': f'shard1/{shard1_hosts[0]}:27018',
            'name': 'shard1'
        }))

    logging.info(f"""
Cluster started with:
  MongoS: {mongos_connection_string}
  ConfigServer: {config_server_hosts}
  Shard0: {shard0_hosts}
  Shard1: {shard1_hosts}
""")


async def main(args):
    '''Main entrypoint of the application'''

    await cleanup_leftover_processes()
    await install_prerequisite_packages()
    await copy_binaries(args.mongobinpath)
    await start_shards_and_config_server()
    await make_cluster()


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description='Tool to create a fully working cluster given SSH access to a set of hosts')
    argsParser.add_argument(
        'mongobinpath',
        help='Location of the mongodb core server binaries to upload to the destination nodes',
        type=str)

    args_list = " ".join(sys.argv[1:])

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    logging.info(f"Starting cluster generation with parameters: '{args_list}'")

    args = argsParser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
