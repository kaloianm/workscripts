#!/usr/bin/env python3
#

import argparse
import asyncio
import motor.motor_asyncio
import pymongo
import sys

from common import Cluster, yes_no
from pymongo import errors as pymongo_errors

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
        if not yes_no('The next steps will perform durable changes to the cluster.\n' +
                      'Proceed (yes/no)? '):
            raise KeyboardInterrupt('User canceled')

    ###############################################################################################
    # Sanity checks (Read-Only): Ensure that the balancer and auto-splitter are stopped and that the
    # MaxChunkSize has been configured appropriately
    #
    balancer_doc = await cluster.configDb.settings.find_one({'_id': 'balancer'})
    if not args.dryrun and (balancer_doc is None or balancer_doc['mode'] != 'off'):
        raise Exception('Balancer must be stopped before running this script')

    auto_splitter_doc = await cluster.configDb.settings.find_one({'_id': 'autosplit'})
    if not args.dryrun and (auto_splitter_doc is None or auto_splitter_doc['enabled']):
        raise Exception('Auto-splitter must be disabled before running this script')

    chunk_size_doc = await cluster.configDb.settings.find_one({'_id': 'chunksize'})
    if not args.dryrun and (chunk_size_doc is None or chunk_size_doc['value'] <= 128):
        raise Exception(
            'The MaxChunkSize must be configured to at least 128 MB before running this script')
    chunk_size = chunk_size_doc['value']

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    # in preparation for the subsequent write phase.
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

    ###############################################################################################
    # PHASE 1 (Merge-only): Run up to `max_merges_on_shards_at_collection_version` concurrent
    # mergeChunks across all shards which are already on the collection major version and up to
    # `max_merges_on_shards_at_less_than_collection_version`. Every merge on a shard which is not
    # at the collection version will result in a StaleShardVersion, which in turn triggers a
    # refresh and a stall on the critical CRUD path.
    #
    # At the end of this phase, all chunks which are already contigious on the same shard will be
    # merged without having to perform any moves.
    async def merge_chunks_on_shard(shard, semaphore_for_first_merge, semaphore_for_next_merges):
        shardChunks = shardToChunks[shard]['chunks']
        if len(shardChunks) < 2:
            return

        num_merges_performed = 0
        num_lock_busy_errors_encountered = 0
        consecutive_chunks = []
        for c in shardChunks:
            if len(consecutive_chunks) == 0 or consecutive_chunks[-1]['max'] != c['min']:
                consecutive_chunks = [c]
                continue
            else:
                consecutive_chunks.append(c)

            estimated_size_of_run_mb = len(consecutive_chunks) * args.estimated_chunk_size_mb

            # After we have collected a run of chunks whose estimated size is 90% of the maximum
            # chunk size, invoke `splitVector` in order to determine whether we can merge them or
            # if we should continue adding more chunks to be merged
            if estimated_size_of_run_mb > chunk_size * 0.90:
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
                        try:
                            await cluster.adminDb.command(mergeCommand)
                        except pymongo_errors.OperationFailure as ex:
                            if ex.details['code'] == 46:  # The code for LockBusy
                                num_lock_busy_errors_encountered += 1
                                if num_lock_busy_errors_encountered == 1:
                                    print(
                                        f"""WARNING: Lock error occurred while trying to merge chunk range {mergeCommand['bounds']}.
                                            This indicates the presence of an older MongoDB version."""
                                    )
                            else:
                                raise

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

    ###############################################################################################
    # PHASE 2:


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
