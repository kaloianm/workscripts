#!/usr/bin/env python3
#

import argparse
import asyncio
import logging
import math
import motor.motor_asyncio
import pymongo
import sys

from common import Cluster, yes_no
from pymongo import errors as pymongo_errors
from tqdm import tqdm

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class ShardedCollection:
    def __init__(self, cluster, ns):
        self.cluster = cluster
        self.name = ns
        self.ns = {'db': self.name.split('.', 1)[0], 'coll': self.name.split('.', 1)[1]}

    async def init(self):
        self.shard_key_pattern = (await self.cluster.configDb.collections.find_one(
            {'_id': self.name}))['key']

    async def data_size_kb(self, range):
        data_size_response = await self.cluster.client[self.ns['db']].command({
            'dataSize': self.name,
            'keyPattern': self.shard_key_pattern,
            'min': range[0],
            'max': range[1],
            'estimate': True
        })

        # Round up the data size of the chunk to the nearest kilobyte
        return math.ceil(float(data_size_response['size']) / 1024.0)

    async def merge_chunks(self, range):
        await self.cluster.adminDb.command({'mergeChunks': self.name, 'bounds': range})

    async def try_write_chunk_size(self, range, expected_owning_shard, size_to_write):
        try:
            update_result = await self.cluster.configDb.chunks.update_one({
                'ns': self.name,
                'min': range[0],
                'max': range[1],
                'shard': expected_owning_shard
            }, {'$set': {
                'defrag_collection_est_size': size_to_write
            }})

            if update_result.modified_count != 1:
                raise Exception(f"Chunk [{range[0]}, {range[1]}] shard: {expected_owning_shard}, size_to_write:{size_to_write} wasn't updated: {update_result.raw_result}")

            return True
        except Exception as ex:
            logging.warning(f'Error {ex} occurred while writing the chunk size')
            return False


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    coll = ShardedCollection(cluster, args.ns)
    await coll.init()

    num_chunks = await cluster.configDb.chunks.count_documents({'ns': coll.name})
    print(
        f"""Collection {coll.name} has a shardKeyPattern of {coll.shard_key_pattern} and {num_chunks} chunks.
            For optimisation and for dry runs, will assume a chunk size of {args.phase_1_estimated_chunk_size_kb}KB."""
    )

    if not args.dryrun:
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
                           sh.stopBalancer()""")

    auto_splitter_doc = await cluster.configDb.settings.find_one({'_id': 'autosplit'})
    if not args.dryrun and (auto_splitter_doc is None or auto_splitter_doc['enabled']):
        raise Exception(
            """The auto-splitter must be disabled before running this script. Please run:
               db.getSiblingDB('config').settings.update({_id:'autosplit'}, {$set: {enabled: false}}, {upsert: true})"""
        )

    chunk_size_doc = await cluster.configDb.settings.find_one({'_id': 'chunksize'})
    if chunk_size_doc is None or chunk_size_doc['value'] < 128:
        if not args.dryrun:
            raise Exception(
                """The MaxChunkSize must be configured to at least 128 MB before running this script. Please run:
                   db.getSiblingDB('config').settings.update({_id:'chunksize'}, {$set: {value: 128}}, {upsert: true})"""
            )
        else:
            target_chunk_size_kb = args.dryrun
    else:
        target_chunk_size_kb = chunk_size_doc['value'] * 1024

    if args.dryrun:
        print(f"""Performing a dry run with target chunk size of {target_chunk_size_kb}.
                  No actual modifications to the cluster will occur.""")
        try:
            await cluster.checkIsMongos()
        except Cluster.NotMongosException:
            print('Not connected to a mongos')

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    # in preparation for the subsequent write phase.
    ###############################################################################################

    shard_to_chunks = {}
    collectionVersion = None

    with tqdm(total=num_chunks, unit=' chunks') as progress:
        async for c in cluster.configDb.chunks.find({'ns': coll.name}, sort=[('min',
                                                                              pymongo.ASCENDING)]):
            shardId = c['shard']
            if collectionVersion is None:
                collectionVersion = c['lastmod']
            if c['lastmod'] > collectionVersion:
                collectionVersion = c['lastmod']
            if shardId not in shard_to_chunks:
                shard_to_chunks[shardId] = {'chunks': [], 'num_merges_performed': 0}
            shard = shard_to_chunks[shardId]
            shard['chunks'].append(c)
            progress.update()

    print(
        f'Collection version is {collectionVersion} and chunks are spread over {len(shard_to_chunks)} shards'
    )

    ###############################################################################################
    #
    # WRITE PHASES START FROM HERE ONWARDS
    #
    ###############################################################################################

    ###############################################################################################
    # PHASE 1 (Merge-only): The purpose of this phase is to merge as many chunks as possible without
    # actually moving any data. It is intended to achieve the maximum number of merged chunks with
    # the minimum possible intrusion to the ongoing CRUD workload due to refresh stalls.
    #
    # The stage is also resumable, because for every chunk/chunk range that it processes, it will
    # persist a field called 'defrag_collection_est_size' on the chunk, which estimates its size as
    # of the time the script ran. Resuming Phase 1 will skip over any chunks which already contain
    # this field, because it indicates that previous execution already ran and performed all the
    # possible merges.
    #
    # These are the parameters that control the operation of this phase and their purpose is
    # explaned below:

    max_merges_on_shards_at_less_than_collection_version = 1
    max_merges_on_shards_at_collection_version = 10

    # The way Phase 1 (merge-only) operates is by running:
    #
    #   (1) Up to `max_merges_on_shards_at_less_than_collection_version` concurrent mergeChunks
    #       across all shards which are below the collection major version
    #           AND
    #   (2) Up to `max_merges_on_shards_at_collection_version` concurrent mergeChunks across all
    #       shards which are already on the collection major version
    #
    # Merges due to (1) will bring the respective shard's major version to that of the collection,
    # which unfortunately is interpreted by the routers as "something routing-related changed" and
    # will result in refresh and a stall on the critical CRUD path. Because of this, the script only
    # runs one at a time of these by default. On the other hand, merges due to (2) only increment
    # the minor version and will not cause stalls on the CRUD path, so these can run with higher
    # concurrency.
    #
    # The expectation is that at the end of this phase, not all possible defragmentation would have
    # been achieved, but the number of chunks on the cluster would have been significantly reduced
    # in a way that would make Phase 2 much less invasive due to refreshes after moveChunk.
    #
    # For example in a collection with 1 million chunks, a refresh due to moveChunk could be
    # expected to take up to a second. However with the number of chunks reduced to 500,000 due to
    # Phase 1, the refresh time would be on the order of ~100-200msec.
    ###############################################################################################

    sem_at_less_than_collection_version = asyncio.Semaphore(
        max_merges_on_shards_at_less_than_collection_version)
    sem_at_collection_version = asyncio.Semaphore(max_merges_on_shards_at_collection_version)

    async def merge_chunks_on_shard(shard, collection_version, progress):
        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if len(shard_chunks) == 0:
            return

        chunk_at_shard_version = max(shard_chunks, key=lambda c: c['lastmod'])
        shard_version = chunk_at_shard_version['lastmod']
        shard_is_at_collection_version = shard_version.time == collection_version.time
        progress.write(f'{shard}: {shard_version}: ', end='')
        if shard_is_at_collection_version:
            progress.write('Merge will start without major version bump')
        else:
            progress.write('Merge will start with a major version bump')

        consecutive_chunks = []
        estimated_size_of_consecutive_chunks = 0

        num_lock_busy_errors_encountered = 0

        for c in shard_chunks:
            progress.update()

            if len(consecutive_chunks) == 0:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb
                continue

            merge_consecutive_chunks_without_size_check = False

            if consecutive_chunks[-1]['max'] == c['min']:
                consecutive_chunks.append(c)
                estimated_size_of_consecutive_chunks += args.phase_1_estimated_chunk_size_kb
            elif len(consecutive_chunks) == 1:
                if not args.dryrun and not 'defrag_collection_est_size' in consecutive_chunks[-1]:
                    chunk_range = [consecutive_chunks[-1]['min'], consecutive_chunks[-1]['max']]
                    data_size = await coll.data_size_kb(chunk_range)
                    await coll.try_write_chunk_size(chunk_range, shard, data_size)

                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb
                continue
            else:
                merge_consecutive_chunks_without_size_check = True

            # After we have collected a run of chunks whose estimated size is 90% of the maximum
            # chunk size, invoke `splitVector` in order to determine whether we can merge them or
            # if we should continue adding more chunks to be merged
            if (estimated_size_of_consecutive_chunks <= target_chunk_size_kb * 0.90
                ) and not merge_consecutive_chunks_without_size_check:
                continue

            merge_bounds = [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]

            # Determine the "exact" (not 100% exact because we use the 'estimate' option) size of
            # the currently accumulated bounds via the `dataSize` command in order to decide
            # whether this run should be merged or if we should continue adding chunks to it.
            if not args.dryrun:
                actual_size_of_consecutive_chunks = await coll.data_size_kb(
                    merge_bounds) if not args.dryrun else estimated_size_of_consecutive_chunks
            else:
                actual_size_of_consecutive_chunks = estimated_size_of_consecutive_chunks

            if merge_consecutive_chunks_without_size_check:
                pass
            elif actual_size_of_consecutive_chunks < target_chunk_size_kb * 0.75:
                # If the actual range size is sill 25% less than the target size, continue adding
                # consecutive chunks
                estimated_size_of_consecutive_chunks = actual_size_of_consecutive_chunks
                continue
            elif actual_size_of_consecutive_chunks > target_chunk_size_kb * 1.10:
                # TODO: If the actual range size is 10% more than the target size, use `splitVector`
                # to determine a better merge/split sequence so as not to generate huge chunks which
                # will have to be split later on
                pass

            # Perform the actual merge, obeying the configured concurrency
            async with (sem_at_collection_version
                        if shard_is_at_collection_version else sem_at_less_than_collection_version):
                if not args.dryrun:
                    try:
                        await coll.merge_chunks(merge_bounds)
                        await coll.try_write_chunk_size(merge_bounds, shard,
                                                        actual_size_of_consecutive_chunks)
                    except pymongo_errors.OperationFailure as ex:
                        if ex.details['code'] == 46:  # The code for LockBusy
                            num_lock_busy_errors_encountered += 1
                            if num_lock_busy_errors_encountered == 1:
                                logging.warning(
                                    f"""Lock error occurred while trying to merge chunk range {merge_bounds}.
                                        This indicates the presence of an older MongoDB version.""")
                        else:
                            raise
                else:
                    progress.write(
                        f'Merging {len(consecutive_chunks)} consecutive chunks on {shard}: {merge_bounds}'
                    )

            if merge_consecutive_chunks_without_size_check:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb
            else:
                consecutive_chunks = []
                estimated_size_of_consecutive_chunks = 0

            shard_entry['num_merges_performed'] += 1
            shard_is_at_collection_version = True

    with tqdm(total=num_chunks, unit=' chunks') as progress:
        tasks = []
        for s in shard_to_chunks:
            tasks.append(
                asyncio.ensure_future(merge_chunks_on_shard(s, collectionVersion, progress)))
        await asyncio.gather(*tasks)

    ###############################################################################################
    # PHASE 2 (Move-and-merge): The purpose of this phase is to move chunks, which are not
    # contiguous on a shard (and couldn't be merged by Phase 1) to a shard where they could be
    # further merged to adjacent chunks.
    #
    # This stage relies on the 'defrag_collection_est_size' fields written to every chunk from
    # Phase 1 in order to calculate the most optimal move strategy.
    #
    # TODO: Implement


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
           print the commands which will be executed. If specified, it needs to be passed a value
           (in MB) which indicates the target chunk size to be used for the simulation in case the
           cluster doesn't have the chunkSize setting enabled. Since some phases of the script
           depend on certain state of the cluster to have been reached by previous phases, if this
           mode is selected, the script will stop early.""", metavar='target_chunk_size',
        type=lambda x: int(x) * 1024, required=False)
    argsParser.add_argument('--ns', help='The namespace to defragment', metavar='ns', type=str,
                            required=True)
    argsParser.add_argument(
        '--phase_1_estimated_chunk_size_mb',
        help="""Applies only to Phase 1 and specifies the amount of data to estimate per chunk
           (in MB) before invoking dataSize in order to obtain the exact size. This value is just an
           optimisation under Phase 1 order to collect as large of a candidate range to merge as
           possible before invoking dataSize on the entire candidate range. Otherwise, the script
           would be invoking dataSize for every single chunk and blocking for the results, which
           would reduce its parallelism.

           The default is chosen as 40%% of 64MB, which states that we project that under the
           current 64MB chunkSize default and the way the auto-splitter operates, the collection's
           chunks are only about 40%% full.

           For dry-runs, because dataSize is not invoked, this parameter is also used to simulate
           the exact chunk size (i.e., instead of actually calling dataSize, the script pretends
           that it returned phase_1_estimated_chunk_size_mb).
           """, metavar='phase_1_estimated_chunk_size_mb', dest='phase_1_estimated_chunk_size_kb',
        type=lambda x: int(x) * 1024, default=64 * 0.40)

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
