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

    ns = {'db': args.ns.split('.', 1)[0], 'coll': args.ns.split('.', 1)[1]}
    shardKeyPattern = (await cluster.configDb.collections.find_one({'_id': args.ns}))['key']
    num_chunks = await cluster.configDb.chunks.count_documents({'ns': args.ns})
    print(
        f'Collection {args.ns} has a shardKeyPattern of {shardKeyPattern} and {num_chunks} chunks')

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
        raise Exception("""The balancer must be stopped before running this script. Please run:
                           db.getSiblingDB('config').settings.update({_id:'balancer'}, {$set: {mode: 'off', enabled: false}}, {upsert: true})"""
                        )

    auto_splitter_doc = await cluster.configDb.settings.find_one({'_id': 'autosplit'})
    if not args.dryrun and (auto_splitter_doc is None or auto_splitter_doc['enabled']):
        raise Exception(
            """The auto-splitter must be disabled before running this script. Please run:
               db.getSiblingDB('config').settings.update({_id:'autosplit'}, {$set: {enabled: false}}, {upsert: true})"""
        )

    chunk_size_doc = await cluster.configDb.settings.find_one({'_id': 'chunksize'})
    if not args.dryrun and (chunk_size_doc is None or chunk_size_doc['value'] < 128):
        raise Exception(
            """The MaxChunkSize must be configured to at least 128 MB before running this script. Please run:
               db.getSiblingDB('config').settings.update({_id:'chunksize'}, {$set: {value: 128}}, {upsert: true})"""
        )
    target_chunk_size = chunk_size_doc['value']

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    # in preparation for the subsequent write phase.
    #
    num_chunks_processed = 0
    shard_to_chunks = {}
    collectionVersion = None
    async for c in cluster.configDb.chunks.find({'ns': args.ns}, sort=[('min', pymongo.ASCENDING)]):
        shardId = c['shard']
        if collectionVersion is None:
            collectionVersion = c['lastmod']
        if c['lastmod'] > collectionVersion:
            collectionVersion = c['lastmod']
        if shardId not in shard_to_chunks:
            shard_to_chunks[shardId] = {'chunks': [], 'num_merges_performed': 0}
        shard = shard_to_chunks[shardId]
        shard['chunks'].append(c)
        num_chunks_processed += 1
        if num_chunks_processed % 10 == 0:
            print(
                f'Initialisation: {round((num_chunks_processed * 100)/num_chunks, 1)}% ({num_chunks_processed} chunks) done',
                end='\r')

    print(
        f'Initialisation completed and found {num_chunks_processed} chunks spread over {len(shard_to_chunks)} shards'
    )
    print(f'Collection version is {collectionVersion}')

    ###############################################################################################
    #
    # WRITE PHASES START FROM HERE ONWARDS
    #

    max_merges_on_shards_at_less_than_collection_version = 1
    max_merges_on_shards_at_collection_version = 8

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
    sem_at_collection_version = asyncio.Semaphore(max_merges_on_shards_at_collection_version)
    sem_at_less_than_collection_version = asyncio.Semaphore(
        max_merges_on_shards_at_less_than_collection_version)

    async def merge_chunks_on_shard(shard, collection_version):
        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if len(shard_chunks) < 2:
            return

        chunk_at_shard_version = max(shard_chunks, key=lambda c: c['lastmod'])
        shard_version = chunk_at_shard_version['lastmod']
        shard_is_at_collection_version = shard_version.time == collection_version.time
        print(f'{shard}: {shard_version}: ', end='')
        if shard_is_at_collection_version:
            print('Merge will start without major version bump ...')
        else:
            print('Merge will start with a major version bump ...')

        num_lock_busy_errors_encountered = 0

        consecutive_chunks = []
        estimated_size_of_consecutive_chunks = 0

        for c in shard_chunks:
            if len(consecutive_chunks) == 0:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.estimated_chunk_size_mb
                continue

            merge_consecutive_chunks_without_size_check = False

            if consecutive_chunks[-1]['max'] == c['min']:
                consecutive_chunks.append(c)
                estimated_size_of_consecutive_chunks += args.estimated_chunk_size_mb
            elif len(consecutive_chunks) == 1:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.estimated_chunk_size_mb
                continue
            else:
                merge_consecutive_chunks_without_size_check = True

            # After we have collected a run of chunks whose estimated size is 90% of the maximum
            # chunk size, invoke `splitVector` in order to determine whether we can merge them or
            # if we should continue adding more chunks to be merged
            if (estimated_size_of_consecutive_chunks <=
                    target_chunk_size * 0.90) and not merge_consecutive_chunks_without_size_check:
                continue

            mergeCommand = {
                'mergeChunks': args.ns,
                'bounds': [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]
            }

            # Determine the "exact" (not 100% exact because we use the 'estimate' option) size of
            # the currently accumulated bounds via the `dataSize` command in order to decide
            # whether this run should be merged or if we should continue adding chunks to it.
            actual_size_of_consecutive_chunks = 0
            if args.dryrun:
                actual_size_of_consecutive_chunks = estimated_size_of_consecutive_chunks
            else:
                data_size_response = await cluster.client[ns['db']].command({
                    'dataSize': args.ns,
                    'keyPattern': shardKeyPattern,
                    'min': mergeCommand['bounds'][0],
                    'max': mergeCommand['bounds'][1],
                    'estimate': True
                })
                actual_size_of_consecutive_chunks = max(data_size_response['size'],
                                                        1024 * 1024) / 1024

            if merge_consecutive_chunks_without_size_check:
                pass
            elif actual_size_of_consecutive_chunks < target_chunk_size * 0.75:
                # If the actual range size is sill 25% less than the target size, continue adding
                # consecutive chunks
                estimated_size_of_consecutive_chunks = actual_size_of_consecutive_chunks
                continue
            elif actual_size_of_consecutive_chunks > target_chunk_size * 1.10:
                # TODO: If the actual range size is 10% more than the target size, use `splitVector`
                # to determine a better merge/split sequence so as not to generate huge chunks which
                # will have to be split later on
                pass

            # Perform the actual merge, obeying the configured concurrency
            async with (sem_at_collection_version
                        if shard_is_at_collection_version else sem_at_less_than_collection_version):
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
                                        This indicates the presence of an older MongoDB version.""")
                        else:
                            raise

            if merge_consecutive_chunks_without_size_check:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.estimated_chunk_size_mb
            else:
                consecutive_chunks = []
                estimated_size_of_consecutive_chunks = 0

            shard_entry['num_merges_performed'] += 1
            shard_is_at_collection_version = True

    tasks = []
    for s in shard_to_chunks:
        tasks.append(asyncio.ensure_future(merge_chunks_on_shard(s, collectionVersion)))
    await asyncio.gather(*tasks)

    ###############################################################################################
    # PHASE 2:
    #
    # TODO:


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
           in order to gather as many consecutive chunks as possible before invoking dataSize.""",
        metavar='estimated_chunk_size_mb', type=int, default=64)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
