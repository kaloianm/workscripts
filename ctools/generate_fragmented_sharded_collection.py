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

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri)
    if not (await cluster.adminDb.command('ismaster'))['msg'] == 'isdbgrid':
        raise Exception("Not connected to mongos")

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    epoch = bson.objectid.ObjectId()
    collectionUUID = uuid.uuid4()
    shardIds = await cluster.shardIds

    print(f'Placing {args.numchunks} chunks over {shardIds} for collection {args.ns}')

    print(f'Cleaning up old entries for {args.ns} ...')
    await cluster.configDb.collections.delete_many({'_id': args.ns})
    await cluster.configDb.chunks.delete_many({'ns': args.ns})
    print(f'Cleaned up old entries for {args.ns}')

    # Create the collection on each shard
    async for shard in cluster.configDb.shards.find({}):
        print('Creating shard key indexes on shard ' + shard['_id'])
        connParts = shard['host'].split('/', 1)
        conn = motor.motor_asyncio.AsyncIOMotorClient(connParts[1], replicaset=connParts[0])
        db = conn[ns['db']]

        applyOpsCommand = {
            'applyOps': [{
                'op': 'c',
                'ns': ns['db'] + '.$cmd',
                'ui': collectionUUID,
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

    # Create collection and chunk entries on the config server
    def gen_chunk(i):
        sortedShardIdx = math.floor(i / (args.numchunks / len(shardIds)))
        shardId = random.choice(shardIds[:sortedShardIdx] +
                                shardIds[sortedShardIdx + 1:]) if random.random(
                                ) < args.fragmentation else shardIds[sortedShardIdx]

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

    print(f'Writing chunks entries')
    chunks = list(map(gen_chunk, range(args.numchunks)))

    sem = asyncio.Semaphore(8)
    global num_chunks_written
    num_chunks_written = 0

    async def safe_write_chunks(chunks_subset, i):
        async with sem:
            result = await cluster.configDb.chunks.bulk_write(chunks_subset, ordered=False)
            global num_chunks_written
            num_chunks_written += result.inserted_count
            print(
                f'Written {round((num_chunks_written * 100)/args.numchunks, 1)}% ({num_chunks_written} entries) so far',
                end='\r')

    batch_size = 5000
    tasks = [
        asyncio.ensure_future(safe_write_chunks(chunks[i:i + batch_size], i))
        for i in range(0, len(chunks), batch_size)
    ]
    await asyncio.gather(*tasks)
    print('Chunks write completed')

    print(f'Writing collection entry')
    await cluster.configDb.collections.insert_one({
        '_id': args.ns,
        'lastmodEpoch': epoch,
        'lastmod': datetime.datetime.now(),
        'dropped': False,
        'key': {
            'shardKey': 1
        },
        'unique': True,
        'uuid': collectionUUID
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
