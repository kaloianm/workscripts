#!/usr/bin/env python3
#
help_string = '''
Tool to create and manipulate a MongoDB replica set given SSH and MongoDB port access to a set
of hosts. When launching the EC2 hosts, please ensure that the machine from which they are being
connected to has access in the inbound rules.

The intended usage is:

1. Use the `launch_ec2_replicaset_hosts.py Tag launch` script in order to spawn a set of hosts in
   EC2 on which the MongoDB processes will run.
2. This script will produce a replica set description replset.json file which contains the specific
   hosts and will serve as an input for the `remote_control_replicaset.py` utilities.
3. Update the 'MongoBinPath' parameter in replset.json and run the
   `remote_control_replicaset.py create replset.json` command in order to launch the processes.

Use --help for more information on the supported commands.
'''

import argparse
import asyncio
import json
import logging
import sys

from common.common import yes_no
from common.remote_mongo import (cleanup_mongo_directories, deploy_binaries, gather_logs,
                                 install_prerequisite_packages, initiate_replica_set,
                                 make_remote_mongo_host, rsync_to_hosts,
                                 start_mongod_as_replica_set, stop_mongo_processes)
from common.version import CTOOLS_VERSION
from signal import Signals

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class ReplicaSetBuilder:
    '''
    Wraps information and common management tasks for a replica set
    '''

    def __init__(self, config):
        self.config = config

        self.name = self.config['Name']
        self.feature_flags = [f'--setParameter {f}=true' for f in self.config["FeatureFlags"]
                              ] if "FeatureFlags" in self.config else []
        self.mongod_parameters = self.config.get('MongoDParameters', []) + self.feature_flags

        self.hosts = []
        for host_info in self.config['Hosts']:
            self.hosts.append(make_remote_mongo_host(host_info, self.config, 'rs'))

        logging.info(
            f"Replica set will consist of: {list(map(lambda h: h.host_desc['host'], self.hosts))}")

    @property
    def connection_string(self):
        return f'mongodb://{self.hosts[0].host}'

    async def get_description(self):
        return f'''
Replica set {self.name} started with:
  Hosts: {self.hosts}
  Connection: {self.connection_string}
'''


##################################################################################################
#
# Main methods implementations for the various sub-commands
#
##################################################################################################


async def main_create(args, rs):
    '''Implements the create command'''

    yes_no(('Start creating the replica set from scratch. '
            'WARNING: The next steps will erase all existing data on the specified hosts.'))

    await stop_mongo_processes(rs.hosts, Signals.SIGKILL)
    await cleanup_mongo_directories(rs.hosts)
    # await install_prerequisite_packages(rs.hosts)
    await deploy_binaries(rs.hosts, rs.config['MongoBinPath'])
    await start_mongod_as_replica_set(rs.hosts, 27017, rs.name, rs.mongod_parameters)
    await initiate_replica_set(rs.hosts, 27017, rs.name)

    logging.info(await rs.get_description())


async def main_describe(args, rs):
    logging.info(await rs.get_description())


async def main_start(args, rs):
    '''Implements the start command'''

    await start_mongod_as_replica_set(rs.hosts, 27017, rs.name, rs.mongod_parameters)


async def main_stop(args, rs):
    '''Implements the stop command'''

    await stop_mongo_processes(rs.hosts, args.signal)


async def main_run(args, rs):
    '''Implements the run command'''

    tasks = []
    for host in rs.hosts:
        tasks.append(asyncio.create_task(host.exec_remote_ssh_command(args.command)))
    await asyncio.gather(*tasks)


async def main_rsync(args, rs):
    '''Implements the rsync command'''

    await rsync_to_hosts(rs.hosts, args.local_pattern, args.remote_path)


async def main_deploy_binaries(args, rs):
    '''Implements the deploy-binaries command'''

    await deploy_binaries(rs.hosts, rs.config['MongoBinPath'])


async def main_gather_logs(args, rs):
    '''Implements the gather-logs command'''

    await gather_logs(rs.hosts, rs.name, args.local_path)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument(
        'replsetconfigfile',
        help='JSON-formatted text file which contains the configuration of the replica set',
        type=str)
    subparsers = argsParser.add_subparsers(title='subcommands')

    ###############################################################################################
    # Arguments for the 'create' command
    parser_create = subparsers.add_parser('create',
                                          help='Creates (or overwrites) a brand new replica set')
    parser_create.set_defaults(func=main_create)

    ###############################################################################################
    # Arguments for the 'describe' command
    parser_describe = subparsers.add_parser('describe', help='Describes the nodes of a replica set')
    parser_describe.set_defaults(func=main_describe)

    ###############################################################################################
    # Arguments for the 'start' command
    parser_start = subparsers.add_parser(
        'start', help='Starts all the processes of an already created replica set')
    parser_start.set_defaults(func=main_start)

    # Arguments for the 'stop' command
    parser_stop = subparsers.add_parser(
        'stop',
        help='Stops all the processes of an already created replica set using a specified signal')
    parser_stop.add_argument(
        '--signal', type=lambda x: Signals[x],
        help=f'The signal to use for terminating the processes. One of {[e.name for e in Signals]}',
        default=Signals.SIGTERM)
    parser_stop.set_defaults(func=main_stop)

    ###############################################################################################
    # Arguments for the 'run' command
    parser_run = subparsers.add_parser('run', help='Runs a command')
    parser_run.add_argument('command', help='The command to run')
    parser_run.set_defaults(func=main_run)

    ###############################################################################################
    # Arguments for the 'rsync' command
    parser_rsync = subparsers.add_parser('rsync',
                                         help='Rsyncs a set of file from a local to remote path')
    parser_rsync.add_argument('local_pattern', help='The local pattern from which to rsync')
    parser_rsync.add_argument('remote_path', help='The remote path to which to rsync')
    parser_rsync.set_defaults(func=main_rsync)

    ###############################################################################################
    # Arguments for the 'deploy-binaries' command
    parser_deploy_binaries = subparsers.add_parser(
        'deploy-binaries',
        help='Specialisation of the rsync command which only deploys binaries from MongoBinPath')
    parser_deploy_binaries.set_defaults(func=main_deploy_binaries)

    ###############################################################################################
    # Arguments for the 'gather-logs' command
    parser_gather_logs = subparsers.add_parser(
        'gather-logs',
        help='Compresses and rsyncs the set of logs from the replica set to a local directory')
    parser_gather_logs.add_argument('local_path', help='The local to which to rsync')
    parser_gather_logs.set_defaults(func=main_gather_logs)

    ###############################################################################################

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    with open(args.replsetconfigfile) as f:
        rs_config = json.load(f)

    logging.info(f'Configuration: {json.dumps(rs_config, indent=2)}')
    rs = ReplicaSetBuilder(rs_config)

    asyncio.run(args.func(args, rs))
