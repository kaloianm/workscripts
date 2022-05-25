#!/usr/bin/env python3
#
'''
'''

import argparse
import asyncio
import json
import logging
import sys

from remote_common import RemoteSSHHost

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main_run(args, available_hosts):
    '''Implements the run command'''

    tasks = []
    for host in available_hosts:
        tasks.append(asyncio.ensure_future(host.exec_remote_ssh_command(args.command)))
    await asyncio.gather(*tasks)


async def main_rsync(args, available_hosts):
    '''Implements the rsync command'''

    tasks = []
    for host in available_hosts:
        tasks.append(
            asyncio.ensure_future(host.rsync_files_to_remote(args.local_pattern, args.remote_path)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description='Tool to execute an SSH command across a set of hosts')
    argsParser.add_argument('clusterconfigfile',
                            help='JSON-formatted text file which contains the set of hosts',
                            type=str)
    subparsers = argsParser.add_subparsers(title='subcommands')

    # Arguments for the 'run' command
    parser_run = subparsers.add_parser('run', help='Runs a command')
    parser_run.add_argument('command', help='The command to run')
    parser_run.set_defaults(func=main_run)

    # Arguments for the 'rsync' command
    parser_rsync = subparsers.add_parser('rsync',
                                         help='Rsyncs a set of file from a local to remote path')
    parser_rsync.add_argument('local_pattern', help='The local pattern from which to rsync')
    parser_rsync.add_argument('remote_path', help='The remote path to which to rsync')
    parser_rsync.set_defaults(func=main_rsync)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    args = argsParser.parse_args()

    with open(args.clusterconfigfile) as f:
        cluster_config = json.load(f)

    logging.info(f"Starting with configuration: '{cluster_config}'")
    available_hosts = list(map(lambda host_info: RemoteSSHHost(host_info), cluster_config['Hosts']))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(args.func(args, available_hosts))
