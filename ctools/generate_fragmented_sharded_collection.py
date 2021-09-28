#!/usr/bin/env python3
#

import argparse
import asyncio
import bson
import datetime
import math
import random
import sys
import uuid

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from common import Cluster
from pymongo import InsertOne
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=False)

    fcv = await cluster.FCV
    shard_key_as_string = fcv <= '4.2'

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    epoch = bson.objectid.ObjectId()
    collection_uuid = uuid.uuid4()
    shardIds = await cluster.shardIds

    print(f"Enabling sharding for database {ns['db']}")
    await cluster.adminDb.command({'enableSharding': ns['db']})

    print(
        f'Placing {args.num_chunks} chunks over {shardIds} for collection {args.ns} with a shard key of {args.shard_key_type}'
    )

    uuid_shard_key_byte_order = None
    if args.shard_key_type == 'uuid':
        uuid_shard_key_byte_order = 'little' if cluster.uuid_representation == UuidRepresentation.JAVA_LEGACY else 'big'
        print(f'Will use {uuid_shard_key_byte_order} byte order for generating UUIDs')

    print(f'Cleaning up old entries for {args.ns} ...')
    await cluster.configDb.collections.delete_many({'_id': args.ns})
    await cluster.configDb.chunks.delete_many({'ns': args.ns})
    print(f'Cleaned up old entries for {args.ns}')

    sem = asyncio.Semaphore(10)

    ###############################################################################################
    # Create the collection on each shard
    ###############################################################################################
    shard_connections = {}

    async def safe_create_shard_indexes(shard):
        async with sem:
            print('Creating shard key indexes on shard ' + shard['_id'])
            client = shard_connections[shard['_id']] = await cluster.make_direct_shard_connection(
                shard)
            db = client[ns['db']]

            await db.command({
                'applyOps': [{
                    'op': 'c',
                    'ns': ns['db'] + '.$cmd',
                    'ui': collection_uuid,
                    'o': {
                        'create': ns['coll'],
                    },
                }]
            }, codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD))

            await db.command({
                'createIndexes': ns['coll'],
                'indexes': [{
                    'key': {
                        'shardKey': 1
                    },
                    'name': 'Shard key index'
                }]
            })

    tasks = []
    async for shard in cluster.configDb.shards.find({}):
        tasks.append(asyncio.ensure_future(safe_create_shard_indexes(shard)))
    await asyncio.gather(*tasks)

    ###############################################################################################
    # Create collection and chunk entries on the config server
    ###############################################################################################

    def make_chunk_id(i):
        if shard_key_as_string:
            return 'shard-key-' + str(i).zfill(8)
        else:
            return ObjectId()

    def make_shard_key(i):
        if uuid_shard_key_byte_order:
            return uuid.UUID(bytes=i.to_bytes(16, byteorder=uuid_shard_key_byte_order))
        else:
            return i

    def gen_chunk(i):
        sortedShardIdx = math.floor(i / (args.num_chunks / len(shardIds)))
        shardId = random.choice(
            shardIds[:sortedShardIdx] + shardIds[sortedShardIdx + 1:]
        ) if random.random() < args.fragmentation else shardIds[sortedShardIdx]

        obj = {
            '_id': make_chunk_id(i),
            'ns': args.ns,
            'lastmodEpoch': epoch,
            'lastmod': bson.timestamp.Timestamp(i + 1, 0),
            'shard': shardId
        }

        if i == 0:
            obj = {
                **obj,
                **{
                    'min': {
                        'shardKey': bson.min_key.MinKey
                    },
                    'max': {
                        'shardKey': make_shard_key(i * 10000)
                    },
                }
            }
        elif i == args.num_chunks - 1:
            obj = {
                **obj,
                **{
                    'min': {
                        'shardKey': make_shard_key((i - 1) * 10000)
                    },
                    'max': {
                        'shardKey': bson.max_key.MaxKey
                    },
                }
            }
        else:
            obj = {
                **obj,
                **{
                    'min': {
                        'shardKey': make_shard_key((i - 1) * 10000)
                    },
                    'max': {
                        'shardKey': make_shard_key(i * 10000)
                    }
                }
            }

        return obj

    chunk_objs = list(map(gen_chunk, range(args.num_chunks)))

    async def safe_write_chunks(shard, chunks_subset, progress):
        async with sem:
            config_and_shard_insert = await asyncio.gather(*[
                asyncio.ensure_future(
                    cluster.configDb.chunks.bulk_write(
                        list(map(lambda x: InsertOne(x), chunks_subset)), ordered=False)),
                asyncio.ensure_future(shard_connections[shard][ns['db']][ns['coll']].bulk_write(
                    list(
                        map(lambda x: InsertOne(dict(x['min'], **{'originalChunk': x})),
                            chunks_subset)), ordered=False))
            ])

            progress.update(config_and_shard_insert[0].inserted_count)

    with tqdm(total=args.num_chunks, unit=' chunks') as progress:
        progress.write('Writing chunks entries ...')
        batch_size = 5000
        shard_to_chunks = {}
        tasks = []
        for c in chunk_objs:
            shard = c['shard']
            if not shard in shard_to_chunks:
                shard_to_chunks[shard] = [c]
            else:
                shard_to_chunks[shard].append(c)

            if len(shard_to_chunks[shard]) == batch_size:
                tasks.append(
                    asyncio.ensure_future(
                        safe_write_chunks(shard, shard_to_chunks[shard], progress)))
                del shard_to_chunks[shard]

        for s in shard_to_chunks:
            tasks.append(asyncio.ensure_future(safe_write_chunks(s, shard_to_chunks[s], progress)))

        await asyncio.gather(*tasks)
        progress.write('Chunks write completed')

    print('Writing collection entry')
    await cluster.configDb.collections.with_options(
        codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD)).insert_one({
            '_id': args.ns,
            'lastmodEpoch': epoch,
            'lastmod': datetime.datetime.now(),
            'dropped': False,
            'key': {
                'shardKey': 1
            },
            'unique': True,
            'uuid': collection_uuid
        })


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description='Tool to generated a sharded collection with various degree of fragmentation')
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument('--ns', help='The namespace to create', metavar='ns', type=str,
                            required=True)
    argsParser.add_argument('--num_chunks', help='The number of chunks to create',
                            metavar='num_chunks', type=int, required=True)
    argsParser.add_argument('--shard_key_type', help='The type to use for a shard key',
                            metavar='shard_key_type', type=str, default='uuid',
                            choices=['integer', 'uuid'])
    argsParser.add_argument(
        '--fragmentation',
        help="""A number between 0 and 1 indicating the level of fragmentation of the chunks. The
           fragmentation is a measure of how likely it is that a chunk, which needs to sequentially
           follow the previous one, on the same shard, is actually not on the same shard.""",
        metavar='fragmentation', type=float, default=0.10)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
