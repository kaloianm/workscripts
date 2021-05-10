#!/usr/bin/env python3
#

import argparse
import asyncio
import bson
import datetime
import math
import motor.motor_asyncio
import random
import sys
import uuid

from bson.codec_options import CodecOptions
from common import Cluster
from pymongo import InsertOne
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.checkIsMongos()

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    epoch = bson.objectid.ObjectId()
    collection_uuid = uuid.uuid4()
    shardIds = await cluster.shardIds

    print(f"Enabling sharding for database {ns['db']}")
    await cluster.adminDb.command({'enableSharding': ns['db']})

    print(f'Placing {args.numchunks} chunks over {shardIds} for collection {args.ns}')

    print(f'Cleaning up old entries for {args.ns} ...')
    await cluster.configDb.collections.delete_many({'_id': args.ns})
    await cluster.configDb.chunks.delete_many({'ns': args.ns})
    print(f'Cleaned up old entries for {args.ns}')

    sem = asyncio.Semaphore(8)

    ###############################################################################################
    # Create the collection on each shard
    ###############################################################################################
    async def safe_create_shard_indexes(shard):
        async with sem:
            print('Creating shard key indexes on shard ' + shard['_id'])
            conn_parts = shard['host'].split('/', 1)
            conn = motor.motor_asyncio.AsyncIOMotorClient(conn_parts[1], replicaset=conn_parts[0])
            db = conn[ns['db']]

            applyOpsCommand = {
                'applyOps': [{
                    'op': 'c',
                    'ns': ns['db'] + '.$cmd',
                    'ui': collection_uuid,
                    'o': {
                        'create': ns['coll'],
                    },
                }]
            }
            await db.command(applyOpsCommand, codec_options=CodecOptions(uuid_representation=4))

            createIndexesCommand = {
                'createIndexes': ns['coll'],
                'indexes': [{
                    'key': {
                        'shardKey': 1
                    },
                    'name': 'Shard key index'
                }]
            }
            await db.command(createIndexesCommand)

    tasks = []
    async for shard in cluster.configDb.shards.find({}):
        tasks.append(asyncio.ensure_future(safe_create_shard_indexes(shard)))
    await asyncio.gather(*tasks)

    ###############################################################################################
    # Create collection and chunk entries on the config server
    ###############################################################################################
    def gen_chunk(i):
        sortedShardIdx = math.floor(i / (args.numchunks / len(shardIds)))
        shardId = random.choice(
            shardIds[:sortedShardIdx] + shardIds[sortedShardIdx + 1:]
        ) if random.random() < args.fragmentation else shardIds[sortedShardIdx]

        obj = {
            '_id': f'shardKey-{args.ns}-{str(i)}',
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
                        'shardKey': i * 10000
                    },
                }
            }
        elif i == args.numchunks - 1:
            obj = {
                **obj,
                **{
                    'min': {
                        'shardKey': (i - 1) * 10000
                    },
                    'max': {
                        'shardKey': bson.max_key.MaxKey
                    },
                }
            }
        else:
            obj = {**obj, **{'min': {'shardKey': (i - 1) * 10000}, 'max': {'shardKey': i * 10000}}}

        return InsertOne(obj)

    chunks = list(map(gen_chunk, range(args.numchunks)))

    async def safe_write_chunks(chunks_subset, i, progress):
        async with sem:
            result = await cluster.configDb.chunks.bulk_write(chunks_subset, ordered=False)
            global num_chunks_written
            progress.update(result.inserted_count)

    with tqdm(total=args.numchunks, unit=' chunks') as progress:
        progress.write('Writing chunks entries ...')
        batch_size = 5000
        tasks = [
            asyncio.ensure_future(safe_write_chunks(chunks[i:i + batch_size], i, progress))
            for i in range(0, len(chunks), batch_size)
        ]
        await asyncio.gather(*tasks)
        progress.write('Chunks write completed')

    print('Writing collection entry')
    await cluster.configDb.collections.insert_one({
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
        metavar='uri', type=str, nargs=1)
    argsParser.add_argument('--ns', help='The namespace to create', metavar='ns', type=str,
                            required=True)
    argsParser.add_argument('--numchunks', help='The number of chunks to create',
                            metavar='numchunks', type=int, required=True)
    argsParser.add_argument(
        '--fragmentation',
        help="""A number between 0 and 1 indicating the level of fragmentation of the chunks. The
           fragmentation is a measure of how likely it is that a chunk, which needs to sequentially
           follow the previous one, on the same shard, is actually not on the same shard.""",
        metavar='fragmentation', type=float, default=0.10)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
