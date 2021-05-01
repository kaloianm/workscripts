#!/usr/bin/env python3
#

import argparse
import asyncio
import motor.motor_asyncio
import sys

from common import Cluster

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri)
    if not (await cluster.adminDb.command('ismaster'))['msg'] == 'isdbgrid':
        raise Exception("Not connected to mongos")


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description='Tool to defragment a sharded cluster')
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str, nargs=1)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(args))
    except Exception as e:
        print('Command failed due to:', str(e))
        sys.exit(-1)
