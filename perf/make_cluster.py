#!/usr/bin/env python3
#

#
# 1. Create 8 instances in the AWS console
# 2. Run the following command in order to obtain the list of instances
#      aws ec2 describe-instances --filters "Name=tag:owner,Values=kaloian.manassiev" --query "Reservations[].Instances[].PublicDnsName[]"
#    and paste the output under the 'available_hosts' variable below
#

import logging
import subprocess
import sys
from pymongo import MongoClient

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

available_hosts = [
    # TODO: Execute the following command in order to obtain the set of instances to use and paste
    # it in this array:
    #   aws ec2 describe-instances --filters "Name=tag:owner,Values=kaloian.manassiev" --query "Reservations[].Instances[].PublicDnsName[]"
]

mongos_hosts = available_hosts[0:1]
config_server_hosts = available_hosts[1:2]
shard0_hosts = available_hosts[2:5]
shard1_hosts = available_hosts[5:8]

# SSH credentials
ssh_user_name = 'ubuntu'

mongo_binaries_path = '/home/ubuntu/workspace/mongo/build/install/bin'
mongo_data_path = '$HOME/data'


# Runs the specified command on the remote host under the SSH credentials configuration above
def exec_remote_ssh_command(host, command):
    ssh_command = f'ssh -o StrictHostKeyChecking=no {ssh_user_name}@{host} "{command}"'
    logging.info(f'Executing: {ssh_command}')
    subprocess.run(ssh_command, shell=True)


# Copy binaries to all nodes and install the prerequisite packages
def copy_binaries():
    for host in available_hosts:
        command = f'rsync -e "ssh -o StrictHostKeyChecking=no" --progress -r -t {mongo_binaries_path}/* {ssh_user_name}@{host}:~/binaries'
        logging.info(f'Executing: {command}')
        subprocess.run(command, shell=True)

        exec_remote_ssh_command(host, 'sudo apt update && sudo apt -y install libsnmp-dev')


copy_binaries()


# Cleanup processes on all nodes
def cleanup_processes():
    for host in available_hosts:
        exec_remote_ssh_command(host, f'killall -9 mongo mongod mongos ; rm -rf {mongo_data_path}')


cleanup_processes()


# Start MongoD on each of the instance and convert them to replica sets
def start_config_server_and_shards():
    # Starts a single MongoD instance on the specified 'host' as part of a replica set called 'repl_set_name'
    def start_mongod_instance(host, repl_set_name, extra_parameters):
        exec_remote_ssh_command(
            host,
            f'mkdir -p {mongo_data_path} && ~/binaries/mongod --replSet {repl_set_name} --dbpath {mongo_data_path} --logpath {mongo_data_path}/mongod.log --fork --bind_ip_all --setParameter rangeDeleterBatchSize=100000 --setParameter orphanCleanupDelaySecs=0 {" ".join(extra_parameters)}'
        )

    # Makes a replica set out of the specified nodes
    def make_replica_set(hosts, port, repl_set_name, extra_parameters):
        # Start the RS hosts
        for host in hosts:
            start_mongod_instance(host, repl_set_name, extra_parameters)

        # Initiate the replica set
        connection_string = f'mongodb://{host}:{port}'
        logging.info(f'Connecting to {connection_string}')

        mongo_client = MongoClient(connection_string)
        logging.info(
            mongo_client.admin.command({
                'replSetInitiate': {
                    '_id':
                        f'{repl_set_name}',
                    'members':
                        list(
                            map(
                                lambda id_and_host: {
                                    '_id': id_and_host[0],
                                    'host': f'{id_and_host[1]}:{port}'
                                }, zip(range(0, len(hosts)), hosts))),
                }
            }))

    # Config Server
    make_replica_set(config_server_hosts, 27019, 'config', ['--configsvr'])

    # Shard(s)
    make_replica_set(shard0_hosts, 27018, 'shard0', ['--shardsvr'])
    make_replica_set(shard1_hosts, 27018, 'shard1', ['--shardsvr'])


start_config_server_and_shards()


# Start MongoS and add the added shards
def make_cluster():
    for mongos in mongos_hosts:
        command = f'mkdir -p {mongo_data_path} && ~/binaries/mongos --configdb config/{config_server_hosts[0]}:27019 --logpath {mongo_data_path}/mongos.log --fork --bind_ip_all'
        exec_remote_ssh_command(mongos, command)

    connection_string = f'mongodb://{mongos_hosts[0]}:27017'
    logging.info(f'Connecting to {connection_string}')
    mongo_client = MongoClient(connection_string)
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


make_cluster()
