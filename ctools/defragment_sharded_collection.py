#!/usr/bin/env python3
#

import argparse
import asyncio
import logging
import math
from motor.frameworks.asyncio import is_event_loop
import pymongo
import sys
import time

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
        if (collection_entry is None) or collection_entry.get('dropped', False):
            raise Exception(f"""Collection '{self.name}' does not exist""")

        self.uuid = collection_entry['uuid']
        self.shard_key_pattern = collection_entry['key']

    async def data_size_kb(self):
        data_size_response = await self.cluster.client[self.ns['db']].command({
            'collStats': self.ns['coll'],
        }, codec_options=self.cluster.client.codec_options)
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

    async def data_size_kb_per_shard(self):
        """Returns an dict:
        {<shard_id>: <size>}
        with collection size in KiB for each shard
        """
        pipeline = [{'$collStats': {'storageStats': {}}},
                    {'$project': {'shard': True, 'storageStats': {'size': True}}}]
        storage_stats = await self.cluster.client[self.ns['db']][self.ns['coll']].aggregate(pipeline).to_list(300)
        def bytes_to_kb(size):
            return max(float(size), 1024.0) / 1024.0
        sizes = {}
        for s in storage_stats:
            shard_id = s['shard']
            sizes[shard_id] = bytes_to_kb(s['storageStats']['size'])
        return sizes

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
                'split': self.name,
                'bounds': [chunk['min'], chunk['max']]
            }, codec_options=self.cluster.client.codec_options)

    async def split_chunk(self, chunk, maxChunkSize_kb):
        shard_entry = await self.cluster.configDb.shards.find_one({'_id': chunk['shard']})
        if shard_entry is None:
            raise Exception(f"cannot resolve shard {chunk['shard']}")
           
        chunk_size_kb = chunk['defrag_collection_est_size']
        if chunk_size_kb <= maxChunkSize_kb:
            return

        num_split_points = chunk_size_kb // maxChunkSize_kb
        surplus = chunk_size_kb - num_split_points * maxChunkSize_kb

        new_maxChunkSize_kb = maxChunkSize_kb - (maxChunkSize_kb - surplus) / (num_split_points + 1);

        logging.info(f"chunk size: {fmt_kb(chunk_size_kb)}, num_split_points: {num_split_points}, surplus: {fmt_kb(surplus)}")
        if surplus >= maxChunkSize_kb - new_maxChunkSize_kb and surplus < maxChunkSize_kb * 0.8:
            # add 5% more to avoid creating a last chunk with few documents
            maxChunkSize_kb = new_maxChunkSize_kb + new_maxChunkSize_kb * 0.05
            logging.info(f"New chunk size: {fmt_kb(maxChunkSize_kb)}")

        conn = await self.cluster.make_direct_shard_connection(shard_entry)
        res = await conn.admin.command({
                'splitVector': self.name,
                'keyPattern': self.shard_key_pattern,
                'maxChunkSizeBytes': maxChunkSize_kb * 1024,
                'min': chunk['min'], 
                'max': chunk['max']
            }, codec_options=self.cluster.client.codec_options)

        print(f"Num split points: {len(res['splitKeys'])}")
        if len(res['splitKeys']) > 0:
            for key in res['splitKeys']:
                res = await self.cluster.adminDb.command({
                    'split': self.name,
                    'middle': key
                }, codec_options=self.cluster.client.codec_options)

        conn.close()

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

def fmt_bytes(num):
    suffix = "B"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def fmt_kb(num):
    return fmt_bytes(num*1024)


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=args.dryrun)

    coll = ShardedCollection(cluster, args.ns)
    await coll.init()

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
    if chunk_size_doc is None:
        if not args.dryrun:
            raise Exception(
                """The MaxChunkSize must be configured before running this script. Please run:
                   db.getSiblingDB('config').settings.update({_id:'chunksize'}, {$set: {value: <maxChunkSize>}}, {upsert: true})"""
            )
        else:
            target_chunk_size_kb = args.dryrun
    elif chunk_size_doc['value'] <= 0:
        raise Exception(
                f"""Found an invalid chunk size in config.settings: '{chunk_size_doc['value']}'""")
    else:
        target_chunk_size_kb = chunk_size_doc['value'] * 1024

    if args.small_chunk_frac <= 0 or args.small_chunk_frac > 0.5:
        raise Exception("The value for --small-chunk-threshold must be between 0 and 0.5")
    small_chunk_size_kb = target_chunk_size_kb * args.small_chunk_frac

    if args.shard_imbalance_frac <= 1.0 or args.shard_imbalance_frac > 1.5:
        raise Exception("The value for --shard-imbalance-threshold must be between 1.0 and 1.5")

    if args.threshold_for_size_calculation < 0 or args.threshold_for_size_calculation > 1:
        raise Exception("The value for --phase_1_calc_size_threshold must be between 0 and 1.0")

    if args.dryrun:
        logging.info(f"""Performing a dry run with target chunk size of {fmt_kb(target_chunk_size_kb)} """
                f"""and an estimated chunk size of {fmt_kb(args.phase_1_estimated_chunk_size_kb)}."""
                f"""No actual modifications to the cluster will occur.""")
    else:
        yes_no(
            f'The next steps will perform an actual merge with target chunk size of {fmt_kb(target_chunk_size_kb)}.'
        )
        if args.phase_1_reset_progress:
            yes_no(f'Previous defragmentation progress will be reset.')
            num_cleared = await coll.clear_chunk_size_estimations()
            logging.info(f'Cleared {num_cleared} already processed chunks.')

    ###############################################################################################
    # Initialisation (Read-Only): Fetch all chunks in memory and calculate the collection version
    # in preparation for the subsequent write phase.
    ###############################################################################################

    num_chunks = await cluster.configDb.chunks.count_documents({'ns': coll.name})
    logging.info(f"""Collection '{coll.name}' has a shardKeyPattern of {coll.shard_key_pattern} and {num_chunks} chunks""")
    shard_to_chunks = {}

    async def load_chunks():
        global collectionVersion
        logging.info('Preperation: Loading chunks into memory')
        assert not shard_to_chunks
        collectionVersion = None
        with tqdm(total=num_chunks, unit=' chunk') as progress:
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
    
        if not args.dryrun:
            sizes = await coll.data_size_kb_per_shard()
            assert (len(sizes) == len(shard_to_chunks))
            for shard_id in shard_to_chunks:
                assert (shard_id in sizes)
                shard_to_chunks[shard_id]['size'] = sizes[shard_id]

    async def write_all_missing_chunk_size():
        if args.dryrun:
            return

        async def write_size(ch, progress):
            bounds = [ch['min'], ch['max']]
            size = await coll.data_size_kb_from_shard(bounds)
            await coll.try_write_chunk_size(bounds, ch['shard'], size)
            progress.update()

        missing_size_query = {'ns': coll.name, 'defrag_collection_est_size': {'$exists': 0}}
        num_chunks_missing_size = await cluster.configDb.chunks.count_documents(missing_size_query)

        if not num_chunks_missing_size:
            return

        logging.info("Calculating missing chunk size estimations") 
        with tqdm(total=num_chunks_missing_size, unit=' chunks') as progress:
            tasks = []
            async for ch in cluster.configDb.chunks.find(missing_size_query):
                tasks.append(
                        asyncio.ensure_future(write_size(ch, progress)))
            await asyncio.gather(*tasks)

    # Mirror the config.chunks indexes in memory
    def build_chunk_index():
        global chunks_id_index, chunks_min_index, chunks_max_index, num_small_chunks
        chunks_id_index = {}
        chunks_min_index = {}
        chunks_max_index = {}
        num_small_chunks = 0
        for s in shard_to_chunks:
            for c in shard_to_chunks[s]['chunks']:
                assert(chunks_id_index.get(c['_id']) == None)
                chunks_id_index[c['_id']] = c
                chunks_min_index[frozenset(c['min'].items())] = c
                chunks_max_index[frozenset(c['max'].items())] = c
                if 'defrag_collection_est_size' in c:
                    if c['defrag_collection_est_size'] < small_chunk_size_kb:
                        num_small_chunks += 1
#                else:
#                    logging.warning("need to perform a chunk size estimation")


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

        estimated_chunk_size_kb = args.phase_1_estimated_chunk_size_kb
        if not args.dryrun:
            estimated_chunk_size_kb = shard_entry['size'] / float(len(shard_entry['chunks']))
        chunk_at_shard_version = max(shard_chunks, key=lambda c: c['lastmod'])
        shard_version = chunk_at_shard_version['lastmod']
        shard_is_at_collection_version = shard_version.time == collection_version.time
        progress.write(f'{shard}: avg chunk size {fmt_kb(estimated_chunk_size_kb)}')
        progress.write(f'{shard}: {shard_version}: ', end='')
        if shard_is_at_collection_version:
            progress.write('Merge will start without major version bump')
        else:
            progress.write('Merge will start with a major version bump')

        num_lock_busy_errors_encountered = 0

        async def update_chunk_size_estimation(ch):
            size_label = 'defrag_collection_est_size'
            if size_label in ch:
                return

            if args.dryrun:
                ch[size_label] = estimated_chunk_size_kb
                return

            chunk_range = [ch['min'], ch['max']]
            ch[size_label] = await coll.data_size_kb_from_shard(chunk_range)
            await coll.try_write_chunk_size(chunk_range, shard, ch[size_label])

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

        class ChunkBatch:
            def __init__(self, chunk_size_estimation):
                self.chunk_size_estimation = chunk_size_estimation
                self.batch = []
                self.batch_size_estimation = 0
                self.trust_batch_estimation = True

            def append(self, ch):
                """Append a chunk to the batch and update the size estimation"""
                self.batch.append(ch)
                if 'defrag_collection_est_size' not in ch:
                    self.trust_batch_estimation = False
                    self.batch_size_estimation += self.chunk_size_estimation
                else:
                    self.batch_size_estimation += ch['defrag_collection_est_size']

            def update_size(self, size):
                """Update batch size estimation"""             
                self.batch_size_estimation = size
                self.trust_batch_estimation = True

            def reset(self):
                """Reset the batch and the size estimation"""
                self.batch = []
                self.batch_size_estimation = 0
                self.trust_batch_estimation = True

            def __len__(self):
                return len(self.batch)

        consecutive_chunks = ChunkBatch(estimated_chunk_size_kb)
        remain_chunks = []

        for c, has_more in lookahead(shard_chunks):
            progress.update()

            if len(consecutive_chunks) == 0:

                # Assume that the user might run phase I more than once. We may encouter chunks with 
                # defrag_collection_est_size set and minimum 75% target chunk size. Do not attempt 
                # to merge these further
                skip_chunk = False
                if 'defrag_collection_est_size' in c:
                    skip_chunk = c['defrag_collection_est_size'] >= target_chunk_size_kb * 0.75

                if skip_chunk or not has_more:
                    await update_chunk_size_estimation(c)
                    remain_chunks.append(c)
                else:
                    consecutive_chunks.append(c)

                continue

            merge_consecutive_chunks_without_size_check = False

            
            def will_overflow_target_size():
                """Returns true if merging the `consecutive_chunks` with the current one `c` will
                produce a chunk that is 20% bigger that the target chunk size.

                If we don't trust the estimation of `consecutive_chunks` or 
                we don't know the size of `c` this function will always return false.
                """
                trust_estimations = consecutive_chunks.trust_batch_estimation and 'defrag_collection_est_size' in c
                return (trust_estimations and
                        consecutive_chunks.batch_size_estimation + c['defrag_collection_est_size'] > (target_chunk_size_kb * 1.20))

            if consecutive_chunks.batch[-1]['max'] == c['min'] and not will_overflow_target_size():
                consecutive_chunks.append(c)
            elif len(consecutive_chunks) == 1:
                await update_chunk_size_estimation(consecutive_chunks.batch[0])
                remain_chunks.append(consecutive_chunks.batch[0])
                consecutive_chunks.reset()

                consecutive_chunks.append(c)

                if not has_more:
                    await update_chunk_size_estimation(consecutive_chunks.batch[0])
                    remain_chunks.append(consecutive_chunks.batch[0])
                    consecutive_chunks.reset()
                    
                continue
            else:
                merge_consecutive_chunks_without_size_check = True

            # To proceed to this stage we must have at least 2 consecutive chunks as candidates to
            # be merged
            assert (len(consecutive_chunks) > 1)

            # After we have collected a run of chunks whose estimated size is 90% of the maximum
            # chunk size, invoke `dataSize` in order to determine whether we can merge them or if
            # we should continue adding more chunks to be merged
            if consecutive_chunks.batch_size_estimation < target_chunk_size_kb * args.threshold_for_size_calculation \
                and not merge_consecutive_chunks_without_size_check and has_more:
                continue

            merge_bounds = [consecutive_chunks.batch[0]['min'], consecutive_chunks.batch[-1]['max']]

            # Determine the "exact" (not 100% exact because we use the 'estimate' option) size of
            # the currently accumulated bounds via the `dataSize` command in order to decide
            # whether this run should be merged or if we should continue adding chunks to it.
            if not consecutive_chunks.trust_batch_estimation and not args.dryrun:
                consecutive_chunks.update_size(await coll.data_size_kb_from_shard(merge_bounds))

            if merge_consecutive_chunks_without_size_check or not has_more:
                pass
            elif consecutive_chunks.batch_size_estimation < target_chunk_size_kb * 0.75:
                # If the actual range size is sill 25% less than the target size, continue adding
                # consecutive chunks
                continue

            elif consecutive_chunks.batch_size_estimation > target_chunk_size_kb * 1.10:
                # TODO: If the actual range size is 10% more than the target size, use `splitVector`
                # to determine a better merge/split sequence so as not to generate huge chunks which
                # will have to be split later on
                pass

            # Perform the actual merge, obeying the configured concurrency
            sem = (sem_at_collection_version
                        if shard_is_at_collection_version else sem_at_less_than_collection_version)
            async with sem:
                new_chunk = consecutive_chunks.batch[0].copy()
                new_chunk['max'] = consecutive_chunks.batch[-1]['max']
                new_chunk['defrag_collection_est_size'] = consecutive_chunks.batch_size_estimation
                remain_chunks.append(new_chunk)
                        
                if not args.dryrun:
                    try:
                        await coll.merge_chunks(consecutive_chunks.batch,
                                                args.phase_1_perform_unsafe_merge)
                        await coll.try_write_chunk_size(merge_bounds, shard,
                                                        consecutive_chunks.batch_size_estimation)
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
            consecutive_chunks.reset()
            if merge_consecutive_chunks_without_size_check:
                consecutive_chunks.append(c)
                if not has_more:
                    await update_chunk_size_estimation(c)
                    remain_chunks.append(c)

            shard_entry['num_merges_performed'] += 1
            shard_is_at_collection_version = True

        # replace list of chunks for phase 2
        shard_entry['chunks'] = remain_chunks

    # Conditionally execute phase 1
    if args.exec_phase == 'phase1' or args.exec_phase == 'all':
        logging.info('Phase I: Merging consecutive chunks on shards')

        await load_chunks()
        assert (len(shard_to_chunks) > 1)

        logging.info(
            f'Collection version is {collectionVersion} and chunks are spread over {len(shard_to_chunks)} shards'
        )
        
        with tqdm(total=num_chunks, unit=' chunk') as progress:
            tasks = []
            for s in shard_to_chunks:
                tasks.append(
                    asyncio.ensure_future(merge_chunks_on_shard(s, collectionVersion, progress)))
            await asyncio.gather(*tasks)
    else:
        logging.info("Skipping Phase I")

    ###############################################################################################
    # PHASE 2 (Move-and-merge): The purpose of this phase is to move chunks, which are not
    # contiguous on a shard (and couldn't be merged by Phase 1) to a shard where they could be
    # further merged to adjacent chunks.
    #
    # This stage relies on the 'defrag_collection_est_size' fields written to every chunk from
    # Phase 1 in order to calculate the most optimal move strategy.
    #

    def has_size(ch):
        return 'defrag_collection_est_size' in ch or \
                'defrag_collection_est_size' in chunks_id_index[ch['_id']]

    # might be called with a chunk document without size estimation
    async def get_chunk_size(ch):
        if 'defrag_collection_est_size' in ch:
            return ch['defrag_collection_est_size']

        local = chunks_id_index[ch['_id']]
        if 'defrag_collection_est_size' in local:
            return local['defrag_collection_est_size']

        chunk_range = [ch['min'], ch['max']]
        data_size_kb = await coll.data_size_kb_from_shard(chunk_range)
        chunks_id_index[ch['_id']]['defrag_collection_est_size'] = data_size_kb

        return data_size_kb

    async def move_merge_chunks_by_size(shard, progress):
        global num_small_chunks
        total_moved_data_kb = 0

        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if len(shard_chunks) == 0:
            return 0

        begin_time = time.monotonic()
        async def exec_throttle():
            duration = time.monotonic() - begin_time
            if duration < args.min_migration_period:
                await asyncio.sleep(args.min_migration_period - duration)
            if args.max_migrations > 0:
                args.max_migrations -= 1
            if args.max_migrations == 0:
                raise Exception("Max number of migrations exceeded")

        async def get_remain_chunk_imbalance(center, target_chunk):
            if target_chunk is None:
                return sys.maxsize

            combined = await get_chunk_size(center) + await get_chunk_size(target_chunk)
            remain = (combined % target_chunk_size_kb)
            if remain == 0:
                return 0
            return min(combined, abs(remain - target_chunk_size_kb))

        num_chunks = len(shard_chunks)

        progress.write(f'Moving small chunks off shard {shard}')

        sorted_chunks = shard_chunks.copy()
        sorted_chunks.sort(key = lambda c: c.get('defrag_collection_est_size', 0))

        for c in sorted_chunks:
            # this chunk might no longer exist due to a move
            if c['_id'] not in chunks_id_index:
                continue

            had_size = has_size(c)
            # size should miss only in dryrun mode
            assert had_size or args.dryrun

            center_size_kb = await get_chunk_size(c)
            
            # chunk are sorted so if we encounter a chunk too big that has not being previously merged
            # we can safely exit from the loop since all the subsequent chunks will be bigger
            if center_size_kb > small_chunk_size_kb:
                if 'merged' in c or not had_size:
                    continue
                else:
                    break

            # chunks should be on other shards, but if this script was executed multiple times or 
            # due to parallelism the chunks might now be on the same shard            

            left_chunk = chunks_max_index.get(frozenset(c['min'].items())) # await cluster.configDb.chunks.find_one({'ns':coll.name, 'max': c['min']})
            right_chunk = chunks_min_index.get(frozenset(c['max'].items())) # await cluster.configDb.chunks.find_one({'ns':coll.name, 'min': c['max']})
            
            # Exclude overweight target shards
            if (left_chunk is not None and right_chunk is not None) and (left_chunk['shard'] != right_chunk['shard']):
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
                is_overweight = False
                if shard != target_shard:
                    is_overweight = total_shard_size[shard] > total_shard_size[target_shard] * args.shard_imbalance_frac
                
                # only move a smaller chunk unless shard is bigger
                if (center_size_kb <= left_size or is_overweight) and (
                    await get_remain_chunk_imbalance(c, left_chunk)) < (await get_remain_chunk_imbalance(c, right_chunk)):

                    merge_bounds = [left_chunk['min'], c['max']]
                    if not args.dryrun:
                        if shard != target_shard:
                            await coll.move_chunk(c, target_shard)
                        
                        await coll.merge_chunks([left_chunk, c], args.phase_1_perform_unsafe_merge)
                        await coll.try_write_chunk_size(merge_bounds, target_shard, new_size)
                    else:
                        progress.write(f'Moving chunk left from {shard} to {target_shard}, '
                                        f'merging {merge_bounds}, new size: {fmt_kb(new_size)}')

                    # update local map, 
                    chunks_id_index.pop(c['_id']) # only first chunk is kept
                    chunks_min_index.pop(frozenset(c['min'].items()))
                    chunks_max_index.pop(frozenset(left_chunk['max'].items()))
                    chunks_max_index[frozenset(c['max'].items())] = left_chunk
                    left_chunk['merged'] = True
                    left_chunk['max'] = c['max']
                    left_chunk['defrag_collection_est_size'] = new_size

                    if shard != target_shard:
                        total_shard_size[shard] -= center_size_kb
                        total_shard_size[target_shard] += center_size_kb
                        total_moved_data_kb += center_size_kb

                    num_chunks -= 1
                    small_chunks_vanished = 1
                    if left_size <= small_chunk_size_kb and new_size > small_chunk_size_kb:
                        small_chunks_vanished += 1
                    num_small_chunks -= small_chunks_vanished
                    progress.update(small_chunks_vanished)
                    await exec_throttle()
                    begin_time = time.monotonic()
                    continue
            
            if right_chunk is not None:
                target_shard = right_chunk['shard']
                right_size = await get_chunk_size(right_chunk)
                new_size = right_size + center_size_kb
                is_overweight = False
                if shard != target_shard:
                    total_shard_size[shard] > total_shard_size[target_shard] * args.shard_imbalance_frac
                
                if center_size_kb <= right_size or is_overweight:

                    merge_bounds = [c['min'], right_chunk['max']]
                    if not args.dryrun:
                        if shard != target_shard:
                            await coll.move_chunk(c, target_shard)
                        
                        await coll.merge_chunks([c, right_chunk], args.phase_1_perform_unsafe_merge)
                        await coll.try_write_chunk_size(merge_bounds, target_shard, new_size)
                    else:
                        progress.write(f'Moving chunk right from {c["shard"]} to {right_chunk["shard"]}, '
                                        f'merging {merge_bounds}, new size: {fmt_kb(new_size)}')

                    # update local map
                    chunks_id_index.pop(right_chunk['_id']) # only first chunk is kept
                    chunks_min_index.pop(frozenset(right_chunk['min'].items()))
                    chunks_max_index.pop(frozenset(c['max'].items()))
                    chunks_max_index[frozenset(right_chunk['max'].items())] = c
                    c['merged'] = True
                    c['shard'] = target_shard
                    c['max'] = right_chunk['max']
                    c['defrag_collection_est_size'] = new_size

                    if shard != target_shard:
                        total_shard_size[shard] -= center_size_kb
                        total_shard_size[target_shard] += center_size_kb
                        total_moved_data_kb += center_size_kb

                    num_chunks -= 1
                    small_chunks_vanished = 1
                    if right_size <= small_chunk_size_kb and new_size > small_chunk_size_kb:
                        small_chunks_vanished += 1
                    num_small_chunks -= small_chunks_vanished
                    progress.update(small_chunks_vanished)
                    await exec_throttle()
                    begin_time = time.monotonic()
                    continue
        # </for c in sorted_chunks:>
        return total_moved_data_kb
    
    async def phase_2():
        # Move and merge small chunks. The way this is written it might need to run multiple times
        total_moved_data_kb = 0

        with tqdm(total=num_small_chunks, unit=' chunks') as progress:
            iteration = 0
            while iteration < 25:
                iteration += 1
                progress.write(f"""Phase II: iteration {iteration}. Number of small chunks {num_small_chunks}, total chunks {len(chunks_id_index)}""")

                moved_data_kb = 0
                tasks = []
                shards_to_process = [s for s in shard_to_chunks]
                while(shards_to_process):
                    # get the shard with most data
                    shard_id = max(shards_to_process, key=lambda s: total_shard_size[s])
                    moved_data_kb += await move_merge_chunks_by_size(shard_id, progress)
                    shards_to_process.remove(shard_id)

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

                if moved_data_kb == 0 or num_small_chunks == 0:
                    return total_moved_data_kb


    if not shard_to_chunks:
        # all subsequent phases assumes we have sizes for all chunks
        # and all the chunks loaded in memory
        await write_all_missing_chunk_size()
        await load_chunks()

    build_chunk_index()


    ###############  Calculate stats #############
    
    total_shard_size = {}

    sum_coll_size = 0
    for shard_id, entry in shard_to_chunks.items():
        estimated_chunk_size_kb = args.phase_1_estimated_chunk_size_kb
        if not args.dryrun:
            estimated_chunk_size_kb = entry['size'] / float(len(entry['chunks']))
        data_size = 0
        for c in entry['chunks']:
            if 'defrag_collection_est_size' in c:
                data_size += c['defrag_collection_est_size']
            else:
                data_size += estimated_chunk_size_kb
        total_shard_size[shard_id] = data_size
        sum_coll_size += data_size
    
    coll_size_kb = await coll.data_size_kb()
    # If we run on a dummy cluster assume collection size
    if args.dryrun and coll_size_kb == 1:
        coll_size_kb = sum_coll_size

    num_shards = len(shard_to_chunks)
    avg_chunk_size_phase_1 = coll_size_kb / len(chunks_id_index)
    ideal_num_chunks = max(math.ceil(coll_size_kb / target_chunk_size_kb), num_shards)
    ideal_num_chunks_per_shard = max(math.ceil(ideal_num_chunks / num_shards), 1)

    ###############  End stats calculation #############
    

    logging.info(f'Collection size {fmt_kb(coll_size_kb)}. Avg chunk size Phase I {fmt_kb(avg_chunk_size_phase_1)}')
    
    for s in shard_to_chunks:
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks'])
        data_size = total_shard_size[s]
        logging.info(f"Number chunks on shard {s: >15}: {num_chunks_per_shard:7}  Data-Size: {fmt_kb(data_size): >9}")

    
    orig_shard_sizes = total_shard_size.copy()

    # Only conditionally execute phase2, break here to get above log lines
    if args.exec_phase == 'phase2' or args.exec_phase == 'all':
        logging.info('Phase II: Moving and merging small chunks')
        
        total_moved_data_kb = await phase_2()
    else:
        logging.info("Skipping Phase II")
        total_moved_data_kb = 0

       
    '''
    for each chunk C in the shard:
    - No split if chunk size < 120% target chunk size
    - Split in the middle if chunk size between 120% and 240% target chunk size
    - Split according to split vector otherwise
    '''
    async def split_oversized_chunks(shard, progress):
        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if args.dryrun or len(shard_chunks) == 0:
            return

        for c in shard_chunks:
            progress.update()

            if 'defrag_collection_est_size' not in c:
                continue

            local_c = chunks_id_index[c['_id']]
            if local_c['defrag_collection_est_size'] > target_chunk_size_kb * 1.2:
                await coll.split_chunk(local_c, target_chunk_size_kb * 2)
            #elif local_c['defrag_collection_est_size'] > target_chunk_size_kb * 1.2:
            #    await coll.split_chunk_middle(local_c)

    if args.exec_phase == 'phase3' or args.exec_phase == 'all':
        logging.info(f'Phase III : Splitting oversized chunks')
 
        num_chunks = len(chunks_id_index)
        with tqdm(total=num_chunks, unit=' chunks', disable=True) as progress:
            for s in shard_to_chunks:
                tasks = []
                tasks.append(
                    asyncio.ensure_future(split_oversized_chunks(s, progress)))
                await asyncio.gather(*tasks)
    else:
        logging.info("Skipping Phase III")
    

    if not args.dryrun and args.write_size_on_exit:
        await write_all_missing_chunk_size()


    print("\n")
    avg_chunk_size_phase_2 = 0
    for s in shard_to_chunks:
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks'])
        data_size = total_shard_size[s]
        avg_chunk_size_phase_2 += data_size
        avg_chunk_size_shard = data_size / num_chunks_per_shard if num_chunks_per_shard > 0 else 0
        print(f"Number chunks on {s: >15}: {num_chunks_per_shard:7}  Data-Size: {fmt_kb(data_size): >9} "
                f" ({fmt_kb(data_size - orig_shard_sizes[s]): >9})  Avg chunk size {fmt_kb(avg_chunk_size_shard): >9}")
    
    avg_chunk_size_phase_2 /= len(chunks_id_index)

    print("\n");
    print(f"""Number of chunks is {len(chunks_id_index)} the ideal number of chunks would be {ideal_num_chunks} for a collection size of {fmt_kb(coll_size_kb)}""")
    print(f'Average chunk size Phase I {fmt_kb(avg_chunk_size_phase_1)} average chunk size Phase II {fmt_kb(avg_chunk_size_phase_2)}')
    print(f"Total moved data: {fmt_kb(total_moved_data_kb)} i.e. {(100 * total_moved_data_kb / coll_size_kb):.2f} %")

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
        '--estimated_chunk_size_mb',
        help="""Only used in dry-runs to estimate the chunk size (in MiB) instead of calling dataSize.

           The default is chosen as 40%% of 64MB, which states that we project that under the
           current 64MB chunkSize default and the way the auto-splitter operates, the collection's
           chunks are only about 40%% full.
           """, metavar='chunk_size_mb', dest='phase_1_estimated_chunk_size_kb',
        type=lambda x: int(x) * 1024, default=64 * 1024 * 0.40)
    argsParser.add_argument(
        '--phase_1_calc_size_threshold',
        help="""Applies only to Phase 1: when the estimated size of a batch surpasses this threshold
        (expressed as a percentage of the target chunk size), a real calculation of the batch size
        will be triggered. Fractional value between 0.0 and 1.0""",
        metavar="fraction_of_chunk_size",
        dest='threshold_for_size_calculation',
        type=float,
        default=0.9)
    argsParser.add_argument(
        '--phase_1_perform_unsafe_merge',
        help="""Applies only to Phase 1 and instructs the script to directly write the merged chunks
           to the config.chunks collection rather than going through the `mergeChunks` command.""",
        metavar='phase_1_perform_unsafe_merge', type=str, default='no', choices=[
            'no', 'unsafe_direct_commit_against_configsvr',
            'super_unsafe_direct_apply_ops_aginst_configsvr'
        ])
    argsParser.add_argument(
        '--phases',
        help="""Which phase of the defragmentation algorithm to execute.""",
        metavar='phase', dest="exec_phase", type=str, default='all', choices=[
            'all', 'phase1', 'phase2', 'phase3'
        ])
    argsParser.add_argument(
        '--phase_2_min_migration_period',
        help="""Minimum time in seconds between the start of subsequent migrations.""",
        metavar='seconds', dest="min_migration_period", type=int, default=0)
    argsParser.add_argument(
        '--phase_2_max_migrations',
        help="""Maximum number of migrations.""",
        metavar='max_migrations', dest="max_migrations", type=int, default=-1)
    
    argsParser.add_argument(
        '--write-size-on-exit',
        help="""Used for debugging purposes, write all missing data size estimation on disk before exit.""",
        dest="write_size_on_exit", action='store_true')

    list = " ".join(sys.argv[1:])
    
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    logging.info(f"Starting with parameters: '{list}'")

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
