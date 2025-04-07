#!/usr/bin/env python3
#
help_string = '''
This is a tool to unshard a collection which is already on a single shard, without downtime.

Use --help for more information on the supported commands.
'''

import argparse
import asyncio
import bson
import logging
import sys

from common.common import Cluster
from common.version import CTOOLS_VERSION

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def clear_collection_sharding(cluster, args):
    collection = await cluster.configDb.collections.find_one({'_id': args.namespace})
    if not collection:
        logging.info(f'Collection {args.namespace} is NOT sharded')
        return

    logging.info(f'Stopping balancer')
    await cluster.adminDb.command({'balancerStop': 1})

    logging.info(f'Merging all chunks into one')
    await cluster.adminDb.command({
        'mergeChunks':
            args.namespace,
        'bounds': [{
            k: bson.min_key.MinKey
            for k in collection['key'].keys()
        }, {
            k: bson.max_key.MaxKey
            for k in collection['key'].keys()
        }]
    })

    if await cluster.configDb.chunks.count_documents({'uuid': collection['uuid']}) != 1:
        raise Exception(f'Collection {args.namespace} must contain exactly one chunk')

    await cluster.configDb.collections.delete_one({'_id': args.namespace})
    await cluster.configDb.chunks.delete_many({'uuid': collection['uuid']})
    await cluster.configDb.tags.delete_many({'ns': args.namespace})

    async def check_cache_collections(shard_id, shard_conn):
        logging.info(f'Clearing shard {shard_id} from leftover routing/filtering information')

        shard_admin_db = shard_conn.get_database('admin')
        await shard_admin_db.command({
            '_flushRoutingTableCacheUpdates': args.namespace,
            'syncFromConfig': True
        })

    await cluster.on_each_shard(check_cache_collections)


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos()

    ns = {'db': args.namespace.split('.', 1)[0], 'coll': args.namespace.split('.', 1)[1]}

    await clear_collection_sharding(cluster, args)

    database = await cluster.configDb.databases.find_one({'_id': ns['db']})

    async def check_orphaned_shards(shard_id, shard_conn):
        if (shard_id == database['primary']):
            return
        logging.info(f'Clearing shard {shard_id} from leftover orphaned information')
        shard_db = shard_conn.get_database(ns['db'])
        await shard_db[ns['coll']].drop()

    await cluster.on_each_shard(check_orphaned_shards)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument('namespace', help='The namespace to unshard, in the form of db.coll',
                            metavar='namespace', type=str)

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(args))
