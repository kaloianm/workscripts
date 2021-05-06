#!/usr/bin/env python3
#

import argparse
import asyncio
import motor.motor_asyncio
import pymongo
import sys

from common import Cluster, yes_no

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())

    if args.dryrun:
        print(f'Performing a dry run ...')
        try:
            await cluster.checkIsMongos()
        except Cluster.NotMongosException:
            print('Not connected to a mongos')
    else:
        await cluster.checkIsMongos()
        if not yes_no('The next step will perform durable changes to the cluster.\n' +
                      'Proceed (yes/no)? '):
            raise KeyboardInterrupt('User canceled')

    ###############################################################################################
    # Sanity checks (Read-Only): Ensure that the balancer and auto-splitter are stopped and that the
    # MaxChunkSize has been configured appropriately
    #
    balancer_doc = await cluster.configDb.settings.find_one({'_id': 'balancer'})
    if balancer_doc is None or balancer_doc['mode'] != 'off':
        raise Exception('Balancer must be stopped before running this script')

    auto_splitter_doc = await cluster.configDb.settings.find_one({'_id': 'autosplit'})
    if auto_splitter_doc is None or auto_splitter_doc['enabled']:
        raise Exception('Auto-splitter must be disabled before running this script')

    chunksize_doc = await cluster.configDb.settings.find_one({'_id': 'chunksize'})
    if chunksize_doc is None or chunksize_doc['value'] <= 128:
        raise Exception(
            'The MaxChunkSize must be configured to at least 128 MB before running this script')

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    #
    num_chunks = await cluster.configDb.chunks.count_documents({'ns': args.ns})
    global num_chunks_processed
    num_chunks_processed = 0
    shardToChunks = {}
    collectionVersion = None
    async for c in cluster.configDb.chunks.find({'ns': args.ns}, sort=[('min', pymongo.ASCENDING)]):
        shardId = c['shard']
        if collectionVersion is None:
            collectionVersion = c['lastmod']
        if c['lastmod'] > collectionVersion:
            collectionVersion = c['lastmod']
        if shardId not in shardToChunks:
            shardToChunks[shardId] = {'chunks': []}
        shard = shardToChunks[shardId]
        shard['chunks'].append(c)
        num_chunks_processed += 1
        if num_chunks_processed % 10 == 0:
            print(
                f'Initialisation: {round((num_chunks_processed * 100)/num_chunks, 1)}% ({num_chunks_processed} chunks) done',
                end='\r')

    print(
        f'Initialisation completed and found {num_chunks_processed} chunks spread over {len(shardToChunks)} shards'
    )
    print(f'Collection version is {collectionVersion}')

    ###############################################################################################
    #
    # WRITE PHASES START FROM HERE ONWARDS
    #

    max_merges_on_shards_at_less_than_collection_version = 1
    max_merges_on_shards_at_collection_version = 1

    #
    ###############################################################################################

    # PHASE 1: Bring all shards to be at the same major chunk version
    async def merge_chunks_on_shard(shard, semaphore_for_first_merge, semaphore_for_next_merges):
        shardChunks = shardToChunks[shard]['chunks']
        if len(shardChunks) < 2:
            return

        num_merges_performed = 0
        consecutive_chunks = []
        for c in shardChunks:
            if len(consecutive_chunks) == 0 or consecutive_chunks[-1]['max'] != c['min']:
                consecutive_chunks = [c]
                continue
            else:
                consecutive_chunks.append(c)

            estimated_size_of_run_mb = len(consecutive_chunks) * args.estimated_chunk_size_mb
            if estimated_size_of_run_mb > 240:
                mergeCommand = {
                    'mergeChunks': args.ns,
                    'bounds': [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]
                }

                async with (semaphore_for_first_merge
                            if num_merges_performed == 0 else semaphore_for_next_merges):
                    if args.dryrun:
                        print(
                            f'Merging {len(consecutive_chunks)} consecutive chunks on {shard}: {mergeCommand}'
                        )
                    else:
                        await cluster.adminDb.command(mergeCommand)
                    consecutive_chunks = []

                num_merges_performed += 1

    sem_at_collection_version = asyncio.Semaphore(max_merges_on_shards_at_collection_version)
    sem_less_than_collection_version = asyncio.Semaphore(
        max_merges_on_shards_at_less_than_collection_version)
    tasks = []
    for s in shardToChunks:
        maxShardVersionChunk = max(shardToChunks[s]['chunks'], key=lambda c: c['lastmod'])
        shardVersion = maxShardVersionChunk['lastmod']
        print(f"{s}: {maxShardVersionChunk['lastmod']}: ", end='')
        if shardVersion.time == collectionVersion.time:
            print(' Merging without major version bump ...')
            tasks.append(
                asyncio.ensure_future(
                    merge_chunks_on_shard(s, sem_at_collection_version, sem_at_collection_version)))
        else:
            print(' Merging and performing major version bump ...')
            tasks.append(
                asyncio.ensure_future(
                    merge_chunks_on_shard(s, sem_less_than_collection_version,
                                          sem_at_collection_version)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description=
        """Tool to defragment a sharded cluster in a way which minimises the rate at which the major
           shard version gets bumped in order to minimise the amount of stalls due to refresh.""")
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str, nargs=1)
    argsParser.add_argument(
        '--dryrun', help=
        """Indicates whether the script should perform actual durable changes to the cluster or just
           print the commands which will be executed. Since some phases of the script depend on
           certain state of the cluster to have been reached by previous phases, if this mode is
           selected, the script will stop early.""", action='store_true')
    argsParser.add_argument('--ns', help='The namespace to defragment', metavar='ns', type=str,
                            required=True)
    argsParser.add_argument(
        '--estimated_chunk_size_mb', help=
        """The amount of data to estimate per chunk (in MB). This value is used as an optimisation
           in order to gather as many consecutive chunks as possible before invoking splitVector.""",
        metavar='estimated_chunk_size_mb', type=int, default=64)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
