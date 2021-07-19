#!/usr/bin/env python3
#

import argparse
import asyncio
import logging
import math
from motor.frameworks.asyncio import is_event_loop
import pymongo
import sys

from common import Cluster, yes_no
from copy import deepcopy
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
        self._direct_config_connection = None

    async def init(self):
        collection_entry = await self.cluster.configDb.collections.find_one({'_id': self.name})

        self.uuid = collection_entry['uuid']
        self.shard_key_pattern = collection_entry['key']

    async def data_size_kb(self):
        data_size_response = await self.cluster.client[self.ns['db']].command({
            'collStats': self.ns['coll'],
        }, codec_options=self.cluster.client.codec_options)
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

    # TODO this method does not work as is now
    async def data_size_kb_entire_shard(self, shard):
        pipeline = [{"$collStats": {"storageStats": {}}},
                    {"$match": {"shard": shard}}]
        list = await self.cluster.client[self.ns['db']][self.ns['coll']].aggregate(pipeline).to_list(1)
        print(list)
        size = list[0]['storageStats']['size']
        return math.ceil(max(float(size), 1024.0) / 1024.0)

    async def data_size_kb_from_shard(self, range):
        data_size_response = await self.cluster.client[self.ns['db']].command({
            'dataSize': self.name,
            'keyPattern': self.shard_key_pattern,
            'min': range[0],
            'max': range[1],
            'estimate': True
        }, codec_options=self.cluster.client.codec_options)

        # Round up the data size of the chunk to the nearest kilobyte
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

    async def split_chunk_middle(self, chunk):
        await self.cluster.adminDb.command({
                'splitChunk': self.name,
                'bounds': [chunk['min'], chunk['max']]
            }, codec_options=self.cluster.client.codec_options)


    async def move_chunk(self, chunk, to):
        await self.cluster.adminDb.command({
                'moveChunk': self.name,
                'bounds': [chunk['min'], chunk['max']],
                'to': to
            }, codec_options=self.cluster.client.codec_options)

    async def merge_chunks(self, consecutive_chunks, unsafe_mode):
        assert (len(consecutive_chunks) > 1)

        if unsafe_mode == 'no':
            await self.cluster.adminDb.command({
                'mergeChunks': self.name,
                'bounds': [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]
            }, codec_options=self.cluster.client.codec_options)
        elif unsafe_mode == 'unsafe_direct_commit_against_configsvr':
            if not self._direct_config_connection:
                self._direct_config_connection = await self.cluster.make_direct_config_server_connection(
                )

            # TODO: Implement the unsafe_direct_commit_against_configsvr option
            raise NotImplementedError(
                'The unsafe_direct_commit_against_configsvr option is not yet implemented')
        elif unsafe_mode == 'super_unsafe_direct_apply_ops_aginst_configsvr':
            first_chunk = deepcopy(consecutive_chunks[0])
            first_chunk['max'] = consecutive_chunks[-1]['max']
            # TODO: Bump first_chunk['version'] to the collection version
            first_chunk.pop('history', None)

            first_chunk_update = [{
                'op': 'u',
                'b': False,  # No upsert
                'ns': 'config.chunks',
                'o': first_chunk,
                'o2': {
                    '_id': first_chunk['_id']
                },
            }]
            remaining_chunks_delete = list(
                map(lambda x: {
                    'op': 'd',
                    'ns': 'config.chunks',
                    'o': {
                        '_id': x['_id']
                    },
                }, consecutive_chunks[1:]))
            precondition = [
                # TODO: Include the precondition
            ]
            apply_ops_cmd = {
                'applyOps': first_chunk_update + remaining_chunks_delete,
                'preCondition': precondition,
            }

            if not self._direct_config_connection:
                self._direct_config_connection = await self.cluster.make_direct_config_server_connection(
                )

            await self._direct_config_connection.admin.command(
                apply_ops_cmd, codec_options=self.cluster.client.codec_options)

    async def try_write_chunk_size(self, range, expected_owning_shard, size_to_write_kb):
        try:
            update_result = await self.cluster.configDb.chunks.update_one({
                'ns': self.name,
                'min': range[0],
                'max': range[1],
                'shard': expected_owning_shard
            }, {'$set': {
                'defrag_collection_est_size': size_to_write_kb
            }})

            if update_result.matched_count != 1:
                raise Exception(
                    f"Chunk [{range[0]}, {range[1]}] wasn't updated: {update_result.raw_result}")
        except Exception as ex:
            logging.warning(f'Error {ex} occurred while writing the chunk size')

    async def clear_chunk_size_estimations(self):
        update_result = await self.cluster.configDb.chunks.update_many(
            {'ns': self.name}, {'$unset': {
                'defrag_collection_est_size': ''
            }})
        return update_result.modified_count


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=args.dryrun)

    coll = ShardedCollection(cluster, args.ns)
    await coll.init()

    num_chunks = await cluster.configDb.chunks.count_documents({'ns': coll.name})
    print(
        f"""Collection {coll.name} has a shardKeyPattern of {coll.shard_key_pattern} and {num_chunks} chunks.
            For optimisation and for dry runs will assume a chunk size of {args.phase_1_estimated_chunk_size_kb} KB."""
    )

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

    if args.small_chunk_frac <= 0 or args.small_chunk_frac > 0.5:
        raise Exception("The value for --small-chunk-threshold must be between 0 and 0.5")

    if args.shard_imbalance_frac <= 1.0 or args.shard_imbalance_frac > 1.5:
        raise Exception("The value for --shard-imbalance-threshold must be between 1.0 and 1.5")

    if args.dryrun:
        print(f"""Performing a dry run with target chunk size of {target_chunk_size_kb} KB.
                  No actual modifications to the cluster will occur.""")
    else:
        yes_no(
            f'The next steps will perform an actual merge with target chunk size of {target_chunk_size_kb} KB.'
        )
        if args.phase_1_reset_progress:
            yes_no(f'Previous defragmentation progress will be reset.')
            num_cleared = await coll.clear_chunk_size_estimations()
            print(f'Cleared {num_cleared} already processed chunks.')

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    # in preparation for the subsequent write phase.
    ###############################################################################################

    shard_to_chunks = {}
    collectionVersion = None

    with tqdm(total=num_chunks, unit=' chunks') as progress:
        async for c in cluster.configDb.chunks.find({'ns': coll.name}, sort=[('min',
                                                                              pymongo.ASCENDING)]):
            shard_id = c['shard']
            if collectionVersion is None:
                collectionVersion = c['lastmod']
            if c['lastmod'] > collectionVersion:
                collectionVersion = c['lastmod']
            if shard_id not in shard_to_chunks:
                shard_to_chunks[shard_id] = {'chunks': [], 'num_merges_performed': 0, 'num_moves_performed': 0}
            shard = shard_to_chunks[shard_id]
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
        num_lock_busy_errors_encountered = 0

        def lookahead(iterable):
            """Pass through all values from the given iterable, augmented by the
            information if there are more values to come after the current one
            (True), or if it is the last value (False).
            """
            # Get an iterator and pull the first value.
            it = iter(iterable)
            last = next(it)
            # Run the iterator to exhaustion (starting from the second value).
            for val in it:
                # Report the *previous* value (more to come).
                yield last, True
                last = val
            # Report the last value.
            yield last, False

        remain_chunks = []
        for c, has_more in lookahead(shard_chunks):
            progress.update()

            if len(consecutive_chunks) == 0:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb

                if not has_more:
                    remain_chunks.append(c)
                    if 'defrag_collection_est_size' not in c:
                        if not args.dryrun:
                            c['defrag_collection_est_size'] = args.phase_1_estimated_chunk_size_kb
                        else:
                            chunk_range = [c['min'], c['max']]
                            c['defrag_collection_est_size'] = await coll.data_size_kb_from_shard(chunk_range)
                            await coll.try_write_chunk_size(chunk_range, shard, c['defrag_collection_est_size'])

                continue

            merge_consecutive_chunks_without_size_check = False

            if consecutive_chunks[-1]['max'] == c['min']:
                consecutive_chunks.append(c)
                estimated_size_of_consecutive_chunks += args.phase_1_estimated_chunk_size_kb
            elif len(consecutive_chunks) == 1:
                if 'defrag_collection_est_size' not in consecutive_chunks[0]:
                    if args.dryrun:
                        consecutive_chunks[0]['defrag_collection_est_size'] = args.phase_1_estimated_chunk_size_kb
                    else:
                        chunk_range = [consecutive_chunks[0]['min'], consecutive_chunks[0]['max']]
                        data_size_kb = await coll.data_size_kb_from_shard(chunk_range)
                        await coll.try_write_chunk_size(chunk_range, shard, data_size_kb)
                        consecutive_chunks[0]['defrag_collection_est_size'] = data_size_kb

                remain_chunks.append(consecutive_chunks[0])

                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb

                if not has_more:
                    remain_chunks.append(c)
                    if 'defrag_collection_est_size' not in consecutive_chunks[0]:
                        if args.dryrun:
                            consecutive_chunks[0]['defrag_collection_est_size'] = args.phase_1_estimated_chunk_size_kb
                        else:
                            chunk_range = [consecutive_chunks[0]['min'], consecutive_chunks[0]['max']]
                            data_size_kb = await coll.data_size_kb_from_shard(chunk_range)
                            await coll.try_write_chunk_size(chunk_range, shard, data_size_kb)
                            c['defrag_collection_est_size'] = data_size_kb

                continue
            else:
                merge_consecutive_chunks_without_size_check = True

            # To proceed to this stage we must have at least 2 consecutive chunks as candidates to
            # be merged
            assert (len(consecutive_chunks) > 1)

            # After we have collected a run of chunks whose estimated size is 90% of the maximum
            # chunk size, invoke `dataSize` in order to determine whether we can merge them or if
            # we should continue adding more chunks to be merged
            if (estimated_size_of_consecutive_chunks < target_chunk_size_kb * 0.90
                ) and not merge_consecutive_chunks_without_size_check and has_more:
                continue

            merge_bounds = [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]

            # Determine the "exact" (not 100% exact because we use the 'estimate' option) size of
            # the currently accumulated bounds via the `dataSize` command in order to decide
            # whether this run should be merged or if we should continue adding chunks to it.
            actual_size_of_consecutive_chunks = estimated_size_of_consecutive_chunks
            if not args.dryrun:
                actual_size_of_consecutive_chunks = await coll.data_size_kb_from_shard(merge_bounds)

            if merge_consecutive_chunks_without_size_check or not has_more:
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
            sem = (sem_at_collection_version
                        if shard_is_at_collection_version else sem_at_less_than_collection_version)
            async with sem:
                new_chunk = consecutive_chunks[0].copy()
                new_chunk['max'] = consecutive_chunks[-1]['max']
                new_chunk['defrag_collection_est_size'] = actual_size_of_consecutive_chunks
                remain_chunks.append(new_chunk)
                        
                if not args.dryrun:
                    try:
                        await coll.merge_chunks(consecutive_chunks,
                                                args.phase_1_perform_unsafe_merge)
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

            # Reset the accumulator so far. If we are merging due to
            # merge_consecutive_chunks_without_size_check, need to make sure that we don't forget
            # the current entry since it is not part of the run
            if merge_consecutive_chunks_without_size_check:
                consecutive_chunks = [c]
                estimated_size_of_consecutive_chunks = args.phase_1_estimated_chunk_size_kb
                if not has_more:
                    remain_chunks.append(c)
            else:
                consecutive_chunks = []
                estimated_size_of_consecutive_chunks = 0

            shard_entry['num_merges_performed'] += 1
            shard_is_at_collection_version = True

        # replace list of chunks for phase 2
        shard_entry['chunks'] = remain_chunks

    
    print('Phase 1: Merging consecutive chunks on shards')
    
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

    total_shard_size = {}

    # Mirror the config.chunks indexes in memory
    chunks_id_index = {}
    chunks_min_index = {}
    chunks_max_index = {}
    for s in shard_to_chunks:
        for c in shard_to_chunks[s]['chunks']:
            assert(chunks_id_index.get(c['_id']) == None)
            chunks_id_index[c['_id']] = c
            chunks_min_index[frozenset(c['min'].items())] = c
            chunks_max_index[frozenset(c['max'].items())] = c

    # might be called with a chunk document without size estimation
    async def get_chunk_size(ch):
        if 'defrag_collection_est_size' in ch:
            return ch['defrag_collection_est_size']

        local = chunks_id_index[ch['_id']]
        if 'defrag_collection_est_size' in local:
            return local['defrag_collection_est_size']

        print("need to perform a chunk size estimation")
        chunk_range = [ch['min'], ch['max']]
        data_size_kb = await coll.data_size_kb_from_shard(chunk_range)
        chunks_id_index[ch['_id']]['defrag_collection_est_size'] = data_size_kb

        return data_size_kb

    async def move_merge_chunks_by_size(shard, idealNumChunks, progress):
        total_moved_data_kb = 0

        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if len(shard_chunks) == 0:
            return 0

        async def get_chunk_imbalance_or_0(target_chunk):
            if (target_chunk is None) or target_chunk['shard'] == shard:
                return 0

            size = await get_chunk_size(target_chunk)
            size_remain = (size % target_chunk_size_kb)
            if size_remain == 0:
                return 0
            else:
                return abs(size_remain - target_chunk_size_kb)

        num_chunks = len(shard_chunks)

        progress.write(f'Moving small chunks off shard {shard}')

        def get_chunk_size_or_0(ch):
            if 'defrag_collection_est_size' in ch:
                return ch['defrag_collection_est_size']
            else:
                 return 0
        sorted_chunks = shard_chunks.copy()
        sorted_chunks.sort(key = get_chunk_size_or_0)

        for c in sorted_chunks:
            progress.update()

            # Abort if we have too few chunks already
            if num_chunks <= idealNumChunks + 1:
                progress.write(f"too few chunks already on shard {shard}: {num_chunks} < {idealNumChunks} + 1")
                break

            # this chunk might no longer exist due to a move
            if c['_id'] not in chunks_id_index:
                continue

            # avoid moving larger chunks
            center_size_kb = await get_chunk_size(c)
            # Use < 0.6 so that we do not move chunks which were split before
            if center_size_kb > target_chunk_size_kb * args.small_chunk_frac:
                break

            # chunks should be on other shards, but if this script was executed multiple times or 
            # due to parallelism the chunks might now be on the same shard            

            left_chunk = chunks_max_index.get(frozenset(c['min'].items())) # await cluster.configDb.chunks.find_one({'ns':coll.name, 'max': c['min']})
            right_chunk = chunks_min_index.get(frozenset(c['max'].items())) # await cluster.configDb.chunks.find_one({'ns':coll.name, 'min': c['max']})
#                if not args.dryrun:
#                    assert(left_chunk is None or (await cluster.configDb.chunks.find_one({'ns':coll.name, 'max': c['min']}))['shard'] == left_chunk['shard'])

            # skip chunks on same shard
            if left_chunk is not None and left_chunk['shard'] == shard:
                left_chunk = None
            if right_chunk is not None and right_chunk['shard'] == shard:
                right_chunk = None
            
            # Exclude overweight target shards
            if left_chunk is not None and right_chunk is not None:
                if total_shard_size[left_chunk['shard']] > total_shard_size[right_chunk['shard']] * args.shard_imbalance_frac:
                    left_chunk = None
                elif total_shard_size[right_chunk['shard']] > total_shard_size[left_chunk['shard']] * args.shard_imbalance_frac:
                    right_chunk = None
                else:
                    pass

            if left_chunk is not None:
                target_shard = left_chunk['shard']
                left_size = await get_chunk_size(left_chunk)
                new_size = left_size + center_size_kb
                is_overweight = total_shard_size[shard] > total_shard_size[target_shard] * args.shard_imbalance_frac
                # only move a smaller chunk unless shard is bigger
                if (center_size_kb <= left_size or is_overweight) and (
                    await get_chunk_imbalance_or_0(left_chunk)) >= (await get_chunk_imbalance_or_0(right_chunk)):

                    if not args.dryrun:
                        await coll.move_chunk(c, target_shard)
                        await coll.merge_chunks([left_chunk, c], args.phase_1_perform_unsafe_merge)
                    else:
                        bounds = [left_chunk['min'], c['max']]
                        progress.write(f'Moving chunk left from {shard} to {target_shard}, merging {bounds}, new size: {new_size}')

                    # update local map, 
                    chunks_id_index.pop(c['_id']) # only first chunk is kept
                    chunks_min_index.pop(frozenset(c['min'].items()))
                    chunks_max_index.pop(frozenset(left_chunk['max'].items()))
                    chunks_max_index[frozenset(c['max'].items())] = left_chunk
                    left_chunk['max'] = c['max']
                    left_chunk['defrag_collection_est_size'] = new_size

                    total_shard_size[shard] -= center_size_kb
                    total_shard_size[target_shard] += center_size_kb
                    total_moved_data_kb += center_size_kb
                    num_chunks -= 1
                    continue
            
            if right_chunk is not None:
                target_shard = right_chunk['shard']
                right_size = await get_chunk_size(right_chunk)
                new_size = right_size + center_size_kb
                is_overweight = total_shard_size[shard] > total_shard_size[target_shard] * 1.2
                if center_size_kb <= right_size or is_overweight:
                    # TODO abort if target shard has too much data already

                    if not args.dryrun:
                        await coll.move_chunk(c, target_shard)
                        await coll.merge_chunks([c, right_chunk], args.phase_1_perform_unsafe_merge)
                    else:
                        bounds = [c['min'], right_chunk['max']]
                        progress.write(f'Moving chunk right from {c["shard"]} to {right_chunk["shard"]}, merging {bounds}, new size: {new_size}')

                    # update local map
                    chunks_id_index.pop(right_chunk['_id']) # only first chunk is kept
                    chunks_min_index.pop(frozenset(right_chunk['min'].items()))
                    chunks_max_index.pop(frozenset(c['max'].items()))
                    chunks_max_index[frozenset(right_chunk['max'].items())] = c
                    c['shard'] = target_shard
                    c['max'] = right_chunk['max']
                    c['defrag_collection_est_size'] = new_size

                    total_shard_size[shard] -= center_size_kb
                    total_shard_size[target_shard] += center_size_kb
                    total_moved_data_kb += center_size_kb
                    num_chunks -= 1
                    continue
        # </for c in sorted_chunks:>
        return total_moved_data_kb

    async def split_oversized_chunks(shard, progress):
        if args.dryrun:
            return

        async for c in cluster.configDb.chunks.find({'ns': coll.name, 'shard': shard}):
            progress.update()

            local_c = chunks_id_index[c['_id']]
            if local_c['defrag_collection_est_size'] > target_chunk_size_kb * 1.2:
                await coll.split_chunk_middle(local_c)

    num_shards = await cluster.configDb.shards.count_documents({})
    coll_size_kb = await coll.data_size_kb()

    sum_coll_size = 0
    for s in shard_to_chunks:
        data_size = 0
        for c in shard_to_chunks[s]['chunks']:
            if 'defrag_collection_est_size' in c:
                data_size += c['defrag_collection_est_size']
            else:
                data_size += args.phase_1_estimated_chunk_size_kb
        total_shard_size[s] = data_size
        sum_coll_size += data_size
    
    # If we run on a dummy cluster assume collection size
    if args.dryrun and coll_size_kb == 1:
        coll_size_kb = sum_coll_size

    avg_chunk_size_phase_1 = sum_coll_size / len(chunks_id_index)
    ideal_num_chunks = max(math.ceil(coll_size_kb / target_chunk_size_kb), num_shards)
    ideal_num_chunks_per_shard = min(math.ceil(ideal_num_chunks / num_shards), 1)

    print('Phase 2: Moving and merging small chunks')
    print(f'Collection size {coll_size_kb} kb. Avg chunk size Phase I {avg_chunk_size_phase_1} kb')

    orig_shard_sizes = total_shard_size.copy()
    for s in shard_to_chunks:
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks'])
        data_size = total_shard_size[s]
        print(f"Number chunks on shard {s}: {num_chunks_per_shard}  Data-Size: {data_size} kb")

    # Move and merge small chunks. The way this is written it might need to run multiple times
    max_iterations = 25
    num_chunks = len(chunks_id_index)
    total_moved_data_kb = 0
    while max_iterations > 0:
        max_iterations -= 1
        print(f"""Number of chunks is {num_chunks} the ideal number of chunks based on collection size is {ideal_num_chunks}, per shard {ideal_num_chunks_per_shard}""")

        moved_data_kb = 0
        with tqdm(total=num_chunks, unit=' chunks') as progress:
            tasks = []
            # TODO balancer logic prevents us from donating / receiving more than once per shard
            for s in shard_to_chunks:
                moved_data_kb += await move_merge_chunks_by_size(s, ideal_num_chunks_per_shard, progress)
#                tasks.append(
#                    asyncio.ensure_future(move_merge_chunks_by_size(s, ideal_num_chunks_per_shard, progress)))
#            await asyncio.gather(*tasks)

        total_moved_data_kb += moved_data_kb
        # update shard_to_chunks
        for s in shard_to_chunks:
            shard_to_chunks[s]['chunks'] = []
        
        for cid in chunks_id_index:
            c = chunks_id_index[cid]
            shard_to_chunks[c['shard']]['chunks'].append(c)
        
        num_chunks = len(chunks_id_index)
        if not args.dryrun:
            num_chunks_actual = await cluster.configDb.chunks.count_documents({'ns': coll.name})
            assert(num_chunks_actual == num_chunks)

        if num_chunks < math.ceil(ideal_num_chunks * 1.25) or moved_data_kb == 0:
            break

        print('Phase 2.2: Splitting oversized chunks, moved {moved_data_kb} kb of data')

        num_chunks = len(chunks_id_index)
        with tqdm(total=num_chunks, unit=' chunks') as progress:
            tasks = []
            for s in shard_to_chunks:
                tasks.append(
                    asyncio.ensure_future(split_oversized_chunks(s, progress)))
            await asyncio.gather(*tasks)

    num_chunks = len(chunks_id_index)

    print("\nReached convergence: \n")
    avg_chunk_size_phase_2 = 0
    for s in shard_to_chunks:
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks'])
        data_size = total_shard_size[s]
        avg_chunk_size_phase_2 += data_size
        print(f"Number chunks on {s}: {num_chunks_per_shard}  Data-Size: {data_size} kb "
              f" ({data_size - orig_shard_sizes[s]} kb)  Avg chunk size {round(data_size / num_chunks_per_shard, 2)} kb")
    
    avg_chunk_size_phase_2 /= len(chunks_id_index)

    print("\n")
    print(f"""Number of chunks is {num_chunks} the ideal number of chunks would be {ideal_num_chunks} for a collection size of {coll_size_kb} kb""")
    print(f'Average chunk size Phase I {round(avg_chunk_size_phase_1, 2)} kb  average chunk size Phase II {round(avg_chunk_size_phase_2, 2)} kb')
    print(f"Total moved data: {total_moved_data_kb} kb i.e. {round(100 * total_moved_data_kb / coll_size_kb, 2)} %")

if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description=
        """Tool to defragment a sharded cluster in a way which minimises the rate at which the major
           shard version gets bumped in order to minimise the amount of stalls due to refresh.""")
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument(
        '--dryrun', help=
        """Indicates whether the script should perform actual durable changes to the cluster or just
           print the commands which will be executed. If specified, it needs to be passed a value
           (in MB) which indicates the target chunk size to be used for the simulation in case the
           cluster doesn't have the chunkSize setting enabled. Since some phases of the script
           depend on certain state of the cluster to have been reached by previous phases, if this
           mode is selected, the script will stop early.""", metavar='target_chunk_size',
        type=lambda x: int(x) * 1024, required=False)
    argsParser.add_argument('--ns', help="""The namespace on which to perform defragmentation""",
                            metavar='ns', type=str, required=True)
    argsParser.add_argument('--small-chunk-threshold', help="""Threshold for the size of chunks 
        eligable to be moved in Phase II. Fractional value between 0 and 0.5""",
        metavar='fraction', dest='small_chunk_frac', type=float, default=0.25)
    argsParser.add_argument('--shard-imbalance-threshold', help="""Threshold for the size difference 
        between two shards where chunks can be moved to. Fractional value between 1.0 and 1.5""",
        metavar='fraction', dest="shard_imbalance_frac", type=float, default=1.2)
    argsParser.add_argument(
        '--phase_1_reset_progress',
        help="""Applies only to Phase 1 and instructs the script to clear the chunk size estimation
        and merge progress which may have been made by an earlier invocation""",
        action='store_true')
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
        type=lambda x: int(x) * 1024, default=64 * 1024 * 0.40)
    argsParser.add_argument(
        '--phase_1_perform_unsafe_merge',
        help="""Applies only to Phase 1 and instructs the script to directly write the merged chunks
           to the config.chunks collection rather than going through the `mergeChunks` command.""",
        metavar='phase_1_perform_unsafe_merge', type=str, default='no', choices=[
            'no', 'unsafe_direct_commit_against_configsvr',
            'super_unsafe_direct_apply_ops_aginst_configsvr'
        ])

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
