#!/usr/bin/env python3
#

import argparse
import asyncio
import bson
import datetime
import json
import logging
import sys
import uuid

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from common import Cluster, ShardCollectionUtil, yes_no
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos()
    await cluster.check_balancer_is_disabled()

    ns = {'db': args.namespace.split('.', 1)[0], 'coll': args.namespace.split('.', 1)[1]}

    # Run sanity checks for the collection
    db = await cluster.configDb.databases.find_one({'_id': ns['db']})
    if not db or not db['partitioned']:
        raise Exception(f"""Sharding is not enabled for database {ns['db']}.
                                Please run sh.enableSharding('{ns['db']}')""")

    if await cluster.configDb.collections.count_documents({'_id': args.namespace}) > 0:
        raise Exception(f'Collection {args.namespace} is already sharded')

    # Ensure that shards do not have any leftover information
    async def check_cache_collections(shard_id, shard_conn):
        logging.info(f'Checking shard {shard_id} for leftover sharding information')

        configDb = shard_conn.get_database('config', codec_options=cluster.system_codec_options)
        if await configDb.get_collection('system.cache.collections').count_documents(
            {'_id': args.namespace}):
            raise Exception(f'Leftover collection entry found on shard {shard_id}')
        if await configDb.get_collection(f'system.cache.chunks.{args.namespace}').count_documents(
            {}):
            raise Exception(f'Leftover chunk entries found on shard {shard_id}')

    await cluster.on_each_shard(check_cache_collections)

    # Check the shard key index and obtain the collection's UUID
    logging.info(
        f"Checking for shard key index presence and the collection's UUID on shard {db['primary']}")
    shard_conn = await cluster.make_direct_shard_connection(db['primary'])
    db_on_shard = shard_conn.get_database(ns['db'])

    # TODO: Check indexes

    # Obtain the collection's UUID
    coll_description_iter = await db_on_shard.list_collections(filter={'name': ns['coll']})
    coll_description = coll_description_iter.next()

    # Choose sharding information for the collection
    shard_collection = ShardCollectionUtil(args.namespace, coll_description['info']['uuid'],
                                           json.loads(args.shard_key), args.shard_key_unique, await
                                           cluster.FCV)

    sem = asyncio.Semaphore(10)

    # TODO: Write chunks on the config server

    # TODO: Write chunks on the DB primary shard

    # TODO: Write collection entry on the config server

    # TODO: Flush router config on the shard


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description='Tool to shard a large collection with minimum downtime')
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument('namespace', help='The namespace to shard, in the form of db.coll',
                            metavar='namespace', type=str)
    argsParser.add_argument('shard_key', help='The shard key for the collection',
                            metavar='shard_key', type=str)
    argsParser.add_argument('shard_key_unique',
                            help='Whether the shard key for the collection should be unique',
                            metavar='shard_key_unique', type=bool)
    argsParser.add_argument(
        'split_points_file',
        help='BSON-formatted file containing the split points to use for creating chunks',
        metavar='split_points_file', type=str)

    args = argsParser.parse_args()

    list = " ".join(sys.argv[1:])

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    logging.info(f"Starting with parameters: '{list}'")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
