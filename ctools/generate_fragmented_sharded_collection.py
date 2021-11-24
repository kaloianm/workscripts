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
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

maxInteger = sys.maxsize
minInteger = -sys.maxsize - 1


def fmt_bytes(num):
    suffix = "B"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def chunk_size_desc():
    if args.chunk_size_min == args.chunk_size_max:
        return fmt_bytes(args.chunk_size_min)
    else:
        return f'[min: {fmt_bytes(args.chunk_size_min)}, max: {fmt_bytes(args.chunk_size_max)}]'


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=False)

    fcv = await cluster.FCV
    shard_key_as_string = fcv <= '4.2'

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    epoch = bson.objectid.ObjectId()
    collection_creation_time = datetime.datetime.now() 
    collection_timestamp = bson.timestamp.Timestamp(collection_creation_time, 1) 
    collection_uuid = uuid.uuid4()
    shardIds = await cluster.shardIds

    print(f"Enabling sharding for database {ns['db']}")
    await cluster.adminDb.command({'enableSharding': ns['db']})

    print(
        f'Placing {args.num_chunks} chunks over {shardIds} for collection {args.ns} with a shard key of {args.shard_key_type}'
    )

    print(f'Chunk size: {chunk_size_desc()}, document size: {fmt_bytes(args.doc_size)}')

    uuid_shard_key_byte_order = None
    if args.shard_key_type == 'uuid':
        uuid_shard_key_byte_order = 'little' if cluster.uuid_representation == UuidRepresentation.JAVA_LEGACY else 'big'
        print(f'Will use {uuid_shard_key_byte_order} byte order for generating UUIDs')

    print(f'Cleaning up old entries for {args.ns} ...')
    dbName, collName = args.ns.split('.', 1)
    await cluster.client[dbName][collName].drop()
    assert await cluster.configDb.collections.count_documents({'_id': args.ns}) == 0
    assert await cluster.configDb.chunks.count_documents({'ns': args.ns}) == 0
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

    def gen_chunks(num_chunks):
        for i in range(num_chunks):
            if len(shardIds) == 1:
                shardId = shardIds[0]
            else:
                sortedShardIdx = math.floor(i / (num_chunks / len(shardIds)))
                shardId = random.choice(
                    shardIds[:sortedShardIdx] + shardIds[sortedShardIdx + 1:]
                ) if random.random() < args.fragmentation else shardIds[sortedShardIdx]

            obj = {
                '_id': make_chunk_id(i),
                'lastmodEpoch': epoch,
                'lastmod': bson.timestamp.Timestamp(i + 1, 0),
                'shard': shardId
            }

            if fcv >= '5.0':
                obj.update({'uuid': collection_uuid})
            else:
                obj.update({'ns': args.ns})

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
            elif i == num_chunks - 1:
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

            yield obj

    def generate_inserts(chunks_subset):
        chunk_size = random.randint(args.chunk_size_min, args.chunk_size_max)
        doc_size = args.doc_size
        num_of_docs_per_chunk = chunk_size // doc_size
        long_string = 'X' * math.ceil(doc_size / 2)

        for c in chunks_subset:
            minKey = c['min'][
                'shardKey'] if c['min']['shardKey'] is not bson.min_key.MinKey else minInteger
            maxKey = c['max'][
                'shardKey'] if c['max']['shardKey'] is not bson.max_key.MaxKey else maxInteger
            gap = ((maxKey - minKey) // (num_of_docs_per_chunk + 1))
            key = minKey
            for i in range(num_of_docs_per_chunk):
                yield {'shardKey': key, long_string: long_string}
                key += gap
                assert key < maxKey, f'key: {key}, maxKey: {maxKey}'

    async def safe_write_chunks(shard, chunks_subset, progress):
        async with sem:
            write_chunks_entries = asyncio.ensure_future(
                cluster.configDb.chunks.with_options(
                    codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD)).insert_many(chunks_subset, ordered=False))
            write_data = asyncio.ensure_future(
                shard_connections[shard][ns['db']][ns['coll']].insert_many(
                    generate_inserts(chunks_subset), ordered=False))

            await asyncio.gather(write_chunks_entries, write_data)
            progress.update(len(chunks_subset))

    with tqdm(total=args.num_chunks, unit=' chunks') as progress:
        progress.write('Writing chunks entries ...')
        batch_size = 1
        shard_to_chunks = {}
        tasks = []
        for c in gen_chunks(args.num_chunks):
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
    coll_obj = {
            '_id': args.ns,
            'lastmod': collection_creation_time,
            'key': {
                'shardKey': 1
                },
            'unique': True,
            'uuid': collection_uuid
            }

    if fcv >= '5.0':
        coll_obj.update({
            'timestamp': collection_timestamp
            })
    else:
        coll_obj.update({
            'lastmodEpoch': epoch,
            'dropped': False
            })

    await cluster.configDb.collections.with_options(
        codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD)).insert_one(coll_obj)


if __name__ == "__main__":
    def kb_to_bytes(kilo):
        return int(kilo) * 1024

    argsParser = argparse.ArgumentParser(
        description='Tool to generated a sharded collection with various degree of fragmentation')
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument('--ns', help='The namespace to create', metavar='neamspace', type=str,
                            required=True)
    argsParser.add_argument('--num-chunks', help='The number of chunks to create', metavar='num',
                            type=int, required=True)
    argsParser.add_argument('--chunk-size-kb', help='Final chunk size (in KiB)', metavar='num',
                            dest='chunk_size', type=lambda x: kb_to_bytes(x), nargs='+',
                            default=kb_to_bytes(1024))
    argsParser.add_argument('--doc-size-kb', help='Size of the generated documents (in KiB)',
                            metavar='num', dest='doc_size', type=lambda x: kb_to_bytes(x),
                            default=kb_to_bytes(8))
    argsParser.add_argument('--shard-key-type', help='The type to use for a shard key',
                            metavar='type', type=str, default='uuid', choices=['integer', 'uuid'])
    argsParser.add_argument(
        '--fragmentation',
        help="""A number between 0 and 1 indicating the level of fragmentation of the chunks. The
           fragmentation is a measure of how likely it is that a chunk, which needs to sequentially
           follow the previous one, on the same shard, is actually not on the same shard.""",
        metavar='fragmentation', type=float, default=0.10)

    args = argsParser.parse_args()

    if len(args.chunk_size) == 1:
        args.chunk_size_min = args.chunk_size_max = args.chunk_size[0]
    elif len(args.chunk_size) == 2:
        args.chunk_size_min = args.chunk_size[0]
        args.chunk_size_max = args.chunk_size[1]
    else:
        raise Exception(f'Too many chunk sizes values provided, maximum 2 allowed')

    del args.chunk_size

    if args.doc_size > min(args.chunk_size_min, args.chunk_size_max):
        raise Exception(
            f'''Specified document size is too big. It needs to be smaller than the chunk size: '''
            f'''Doc size : {fmt_bytes(args.doc_size)}, Chunk size: {chunk_size_desc()}''')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
