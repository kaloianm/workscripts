#!/usr/bin/env python3
#
help_string = '''
This is a tool to defragment a sharded cluster in a way which minimises the rate at which the major
shard version gets bumped in order to minimise the amount of stalls due to refresh.

See the help for more commands.
'''

import argparse
import asyncio
import logging
import math
import pymongo
import sys
import time
import pickle

from common.common import Cluster, yes_no
from common.version import CTOOLS_VERSION
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
        self.fcv = await self.cluster.FCV

    def chunks_query_filter(self):
        if self.fcv >= '5.0':
            return {'uuid': self.uuid}
        else:
            return {'ns': self.name}

    async def data_size_kb(self):
        data_size_response = await self.cluster.client[self.ns['db']].command(
            {
                'collStats': self.ns['coll'],
            },
            codec_options=self.cluster.client.codec_options,
        )
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

    async def data_size_kb_per_shard(self):
        """Returns an dict:
        {<shard_id>: <size>}
        with collection size in KiB for each shard
        """
        pipeline = [{
            '$collStats': {
                'storageStats': {}
            }
        }, {
            '$project': {
                'shard': True,
                'storageStats': {
                    'size': True
                }
            }
        }]
        storage_stats = await self.cluster.client[self.ns['db']][self.ns['coll']
                                                                 ].aggregate(pipeline).to_list(300)

        def bytes_to_kb(size):
            return max(float(size), 1024.0) / 1024.0

        sizes = {}
        for s in storage_stats:
            shard_id = s['shard']
            sizes[shard_id] = bytes_to_kb(s['storageStats']['size'])
        return sizes

    async def data_size_kb_from_shard(self, range):
        data_size_response = await self.cluster.client[self.ns['db']].command(
            {
                'dataSize': self.name,
                'keyPattern': self.shard_key_pattern,
                'min': range[0],
                'max': range[1],
                'estimate': True
            },
            codec_options=self.cluster.client.codec_options,
        )

        # Round up the data size of the chunk to the nearest kilobyte
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

    async def split_chunk(self, chunk, maxChunkSize_kb, conn):
        chunk_size_kb = chunk['defrag_collection_est_size']
        if chunk_size_kb <= maxChunkSize_kb:
            return

        num_split_points = chunk_size_kb // maxChunkSize_kb
        surplus = chunk_size_kb - num_split_points * maxChunkSize_kb

        new_maxChunkSize_kb = maxChunkSize_kb - (maxChunkSize_kb - surplus) / (num_split_points + 1)

        remove_last_split_point = False
        if surplus >= maxChunkSize_kb * 0.8:
            # The last resulting chunk will have a size gte(80% maxChunkSize) and lte(maxChunkSize)
            pass
        elif surplus < maxChunkSize_kb - new_maxChunkSize_kb:
            # The last resulting chunk will be slightly bigger than maxChunkSize
            remove_last_split_point = True
        else:
            # Fairly distribute split points so resulting chunks will be of similar sizes
            maxChunkSize_kb = new_maxChunkSize_kb

        res = await conn.admin.command(
            {
                'splitVector': self.name,
                'keyPattern': self.shard_key_pattern,
                # Double size because splitVector splits at half maxChunkSize
                'maxChunkSizeBytes': maxChunkSize_kb * 2 * 1024,
                'min': chunk['min'],
                'max': chunk['max']
            },
            codec_options=self.cluster.client.codec_options,
        )

        split_keys = res['splitKeys']
        if len(split_keys) > 0:
            if remove_last_split_point:
                split_keys.pop()

            for key in res['splitKeys']:
                res = await self.cluster.adminDb.command(
                    {
                        'split': self.name,
                        'middle': key
                    },
                    codec_options=self.cluster.client.codec_options,
                )

            splits_performed_per_shard[chunk['shard']] += len(split_keys)

    async def move_chunk(self, chunk, to):
        await self.cluster.adminDb.command(
            {
                'moveChunk': self.name,
                'bounds': [chunk['min'], chunk['max']],
                'to': to
            },
            codec_options=self.cluster.client.codec_options,
        )

    async def merge_chunks(self, consecutive_chunks):
        assert (len(consecutive_chunks) > 1)

        await self.cluster.adminDb.command(
            {
                'mergeChunks': self.name,
                'bounds': [consecutive_chunks[0]['min'], consecutive_chunks[-1]['max']]
            },
            codec_options=self.cluster.adminDb.codec_options,
        )

    async def try_write_chunk_size(self, range, expected_owning_shard, size_to_write_kb):
        try:
            chunk_selector = self.chunks_query_filter()
            chunk_selector.update({
                'min': range[0],
                'max': range[1],
                'shard': expected_owning_shard
            })
            update_result = await self.cluster.configDb.chunks.update_one(
                chunk_selector, {'$set': {
                    'defrag_collection_est_size': size_to_write_kb
                }})

            if update_result.matched_count != 1:
                raise Exception(
                    f"Chunk [{range[0]}, {range[1]}] wasn't updated: {update_result.raw_result}")
        except Exception as ex:
            logging.warning(f'Error {ex} occurred while writing the chunk size')

    async def clear_chunk_size_estimations(self):
        update_result = await self.cluster.configDb.chunks.update_many(
            self.chunks_query_filter(), {'$unset': {
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
    return fmt_bytes(num * 1024)


async def throttle_if_necessary(last_time_secs, min_delta_secs):
    secs_elapsed_since_last = (time.perf_counter() - last_time_secs)
    if secs_elapsed_since_last < min_delta_secs:
        secs_to_sleep = min_delta_secs - secs_elapsed_since_last
        await asyncio.sleep(secs_to_sleep)


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos(warn_only=args.dryrun)

    coll = ShardedCollection(cluster, args.ns)
    await coll.init()

    ###############################################################################################
    # Sanity checks (Read-Only). Ensure that:
    # - The current FCV mode is lower than 5.0
    # - The balancer and auto-splitter are stopped
    # - No zones are associated to the collection
    # - MaxChunkSize has been configured appropriately
    #

    await cluster.check_balancer_is_disabled()

    tags_doc = await cluster.configDb.tags.find_one({'ns': args.ns})
    if tags_doc is not None:
        raise Exception("There can be no zones associated with the collection to defragment")

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

    args.write_chunk_size = not args.no_write_chunk_size

    if args.dryrun:
        logging.info(
            f"""Performing a dry run with target chunk size of {fmt_kb(target_chunk_size_kb)} """
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

    num_chunks = await cluster.configDb.chunks.count_documents(coll.chunks_query_filter())
    logging.info(
        f"""Collection '{coll.name}' has a shardKeyPattern of {coll.shard_key_pattern} and {num_chunks} chunks"""
    )
    shard_to_chunks = {}

    async def load_chunks():
        global collectionVersion
        logging.info('Preperation: Loading chunks into memory')
        assert not shard_to_chunks
        collectionVersion = None
        with tqdm(total=num_chunks, unit=' chunk') as progress:
            async for c in cluster.configDb.chunks.find(coll.chunks_query_filter(),
                                                        sort=[('min', pymongo.ASCENDING)]):
                shard_id = c['shard']
                if collectionVersion is None:
                    collectionVersion = c['lastmod']
                if c['lastmod'] > collectionVersion:
                    collectionVersion = c['lastmod']
                if shard_id not in shard_to_chunks:
                    shard_to_chunks[shard_id] = {
                        'chunks': [],
                        'num_merges_performed': 0,
                        'num_moves_performed': 0
                    }
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
        if args.dryrun or not args.write_chunk_size:
            return

        async def write_size(ch, progress):
            bounds = [ch['min'], ch['max']]
            size = await coll.data_size_kb_from_shard(bounds)
            await coll.try_write_chunk_size(bounds, ch['shard'], size)
            progress.update()

        missing_size_query = coll.chunks_query_filter()
        missing_size_query.update({'defrag_collection_est_size': {'$exists': 0}})
        num_chunks_missing_size = await cluster.configDb.chunks.count_documents(missing_size_query)

        if not num_chunks_missing_size:
            return

        logging.info("Calculating missing chunk size estimations")
        with tqdm(total=num_chunks_missing_size, unit=' chunks') as progress:
            tasks = []
            async for ch in cluster.configDb.chunks.find(missing_size_query):
                tasks.append(asyncio.ensure_future(write_size(ch, progress)))
            await asyncio.gather(*tasks)

    # Mirror the config.chunks indexes in memory
    def build_chunk_index():
        global chunks_id_index, chunks_min_index, chunks_max_index, num_small_chunks, num_chunks_no_size
        chunks_id_index = {}
        chunks_min_index = {}
        chunks_max_index = {}
        num_small_chunks = 0
        num_chunks_no_size = 0
        for s in shard_to_chunks:
            for c in shard_to_chunks[s]['chunks']:
                assert (chunks_id_index.get(c['_id']) == None)
                chunks_id_index[c['_id']] = c
                chunks_min_index[pickle.dumps(c['min'])] = c
                chunks_max_index[pickle.dumps(c['max'])] = c
                if 'defrag_collection_est_size' in c:
                    if c['defrag_collection_est_size'] < small_chunk_size_kb:
                        num_small_chunks += 1
                else:
                    num_chunks_no_size += 1

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

        async def update_chunk_size_estimation(ch):
            size_label = 'defrag_collection_est_size'
            if size_label in ch:
                return

            if args.dryrun:
                ch[size_label] = estimated_chunk_size_kb
                return

            chunk_range = [ch['min'], ch['max']]
            ch[size_label] = await coll.data_size_kb_from_shard(chunk_range)

            if args.write_chunk_size:
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
        last_merge_time = time.perf_counter()

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

            def will_overflow_merge_cap():
                """Returns true if merging the `consecutive_chunks` with the current one `c` will
                produce a chunk that is 20% bigger that the target chunk size or if we have reached
                the user configured max number of chunks per batch.

                If we don't trust the estimation of `consecutive_chunks` or 
                we don't know the size of `c` this function will always return false.
                """
                if args.merge_batch_size and len(consecutive_chunks) == args.merge_batch_size:
                    return True
                trust_estimations = consecutive_chunks.trust_batch_estimation and 'defrag_collection_est_size' in c
                return trust_estimations and (consecutive_chunks.batch_size_estimation +
                                              c['defrag_collection_est_size']
                                              > (target_chunk_size_kb * 1.20))

            def too_many_chunks_to_merge_at_once():
                """Merging too many chunks in one go can hit the 16 MB BSON size limit. 
                Let's not allow more than 'max_chunks_to_merge' to be merged.
                """
                max_chunks_to_merge = 20000
                return len(consecutive_chunks) > max_chunks_to_merge

            if consecutive_chunks.batch[-1]['max'] == c['min'] and not will_overflow_merge_cap(
            ) and not too_many_chunks_to_merge_at_once():
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
            if (consecutive_chunks.batch_size_estimation
                    < target_chunk_size_kb * args.threshold_for_size_calculation
                ) and not merge_consecutive_chunks_without_size_check and has_more:
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
                        await throttle_if_necessary(last_merge_time, args.phase1_throttle_secs)
                        await coll.merge_chunks(consecutive_chunks.batch)
                        if args.write_chunk_size:
                            await coll.try_write_chunk_size(
                                merge_bounds, shard, consecutive_chunks.batch_size_estimation)
                        last_merge_time = time.perf_counter()
                    except pymongo_errors.OperationFailure as ex:
                        if ex.details['code'] == 46:  # The code for LockBusy
                            logging.warning(
                                f"""Lock error occurred while trying to merge chunk range {merge_bounds}.
                                    Consider executing with the option `--no-parallel-merges`.""")
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
            if args.no_parallel_merges or args.phase1_throttle_secs:
                for s in shard_to_chunks:
                    await merge_chunks_on_shard(s, collectionVersion, progress)
            else:
                tasks = []
                for s in shard_to_chunks:
                    tasks.append(
                        asyncio.ensure_future(merge_chunks_on_shard(s, collectionVersion,
                                                                    progress)))
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

    # might be called with a chunk document without size estimation
    async def get_chunk_size(ch):
        if 'defrag_collection_est_size' in ch:
            return ch['defrag_collection_est_size']

        local = chunks_id_index[ch['_id']]
        if 'defrag_collection_est_size' in local:
            return local['defrag_collection_est_size']

        chunk_range = [ch['min'], ch['max']]
        data_size_kb = await coll.data_size_kb_from_shard(chunk_range)
        ch['phase2_calculated_size'] = True
        chunks_id_index[ch['_id']]['defrag_collection_est_size'] = data_size_kb

        return data_size_kb

    async def move_merge_chunks_by_size(shard, progress):
        global num_small_chunks
        global num_chunks_no_size
        total_moved_data_kb = 0

        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if len(shard_chunks) == 0:
            return 0

        def check_max_migrations():
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

        progress.write(f'Moving small chunks off shard {shard}')

        sorted_chunks = shard_chunks.copy()
        sorted_chunks.sort(key=lambda c: c.get('defrag_collection_est_size', 0))
        last_migration_time = time.perf_counter()

        for c in sorted_chunks:
            # this chunk might no longer exist due to a move
            if c['_id'] not in chunks_id_index:
                continue

            center_size_kb = await get_chunk_size(c)

            had_size = 'phase2_calculated_size' not in c

            # size should miss only in dryrun mode
            assert had_size or args.dryrun or not args.write_chunk_size

            # chunk are sorted so if we encounter a chunk too big that has not being previously merged
            # we can safely exit from the loop since all the subsequent chunks will be bigger
            if center_size_kb > small_chunk_size_kb:
                if 'merged' in c:
                    continue
                elif not had_size:
                    progress.update(1)
                    continue
                else:
                    break

            # chunks should be on other shards, but if this script was executed multiple times or
            # due to parallelism the chunks might now be on the same shard

            left_chunk = chunks_max_index.get(pickle.dumps(c['min']))
            right_chunk = chunks_min_index.get(pickle.dumps(c['max']))

            # Exclude overweight target shards
            if (left_chunk is not None and right_chunk is not None) and (left_chunk['shard']
                                                                         != right_chunk['shard']):
                if total_shard_size[left_chunk['shard']] > total_shard_size[
                        right_chunk['shard']] * args.shard_imbalance_frac:
                    left_chunk = None
                elif total_shard_size[right_chunk['shard']] > total_shard_size[
                        left_chunk['shard']] * args.shard_imbalance_frac:
                    right_chunk = None
                else:
                    pass

            if left_chunk is not None:
                target_shard = left_chunk['shard']
                left_size = await get_chunk_size(left_chunk)
                new_size = left_size + center_size_kb
                is_overweight = False
                if shard != target_shard:
                    is_overweight = total_shard_size[
                        shard] > total_shard_size[target_shard] * args.shard_imbalance_frac

                # only move a smaller chunk unless shard is bigger
                if (center_size_kb <= left_size
                        or is_overweight) and (await get_remain_chunk_imbalance(
                            c, left_chunk)) < (await get_remain_chunk_imbalance(c, right_chunk)):

                    merge_bounds = [left_chunk['min'], c['max']]
                    if not args.dryrun:
                        await throttle_if_necessary(last_migration_time, args.phase2_throttle_secs)
                        if shard != target_shard:
                            await coll.move_chunk(c, target_shard)

                        await coll.merge_chunks([left_chunk, c])
                        if args.write_chunk_size:
                            await coll.try_write_chunk_size(merge_bounds, target_shard, new_size)
                        last_migration_time = time.perf_counter()
                    else:
                        progress.write(f'Moving chunk left from {shard} to {target_shard}, '
                                       f'merging {merge_bounds}, new size: {fmt_kb(new_size)}')

                    # update local map,
                    chunks_id_index.pop(c['_id'])  # only first chunk is kept
                    chunks_min_index.pop(pickle.dumps(c['min']))
                    chunks_max_index.pop(pickle.dumps(c['max']))
                    chunks_max_index[pickle.dumps(c['max'])] = left_chunk
                    left_chunk['merged'] = True
                    left_chunk['max'] = c['max']
                    left_chunk['defrag_collection_est_size'] = new_size

                    if shard != target_shard:
                        total_shard_size[shard] -= center_size_kb
                        total_shard_size[target_shard] += center_size_kb
                        total_moved_data_kb += center_size_kb

                    # update stats for merged chunk (source)
                    progress.update(1)
                    #update stats for merged chunk (destination)
                    if left_size <= small_chunk_size_kb and new_size > small_chunk_size_kb:
                        progress.update(1)

                    check_max_migrations()
                    continue

            if right_chunk is not None:
                target_shard = right_chunk['shard']
                right_size = await get_chunk_size(right_chunk)
                new_size = right_size + center_size_kb
                is_overweight = False
                if shard != target_shard:
                    is_overweight = total_shard_size[
                        shard] > total_shard_size[target_shard] * args.shard_imbalance_frac

                if center_size_kb <= right_size or is_overweight:

                    merge_bounds = [c['min'], right_chunk['max']]
                    if not args.dryrun:
                        await throttle_if_necessary(last_migration_time, args.phase2_throttle_secs)
                        if shard != target_shard:
                            await coll.move_chunk(c, target_shard)

                        await coll.merge_chunks([c, right_chunk])
                        if args.write_chunk_size:
                            await coll.try_write_chunk_size(merge_bounds, target_shard, new_size)
                        last_migration_time = time.perf_counter()
                    else:
                        progress.write(
                            f'Moving chunk right from {c["shard"]} to {right_chunk["shard"]}, '
                            f'merging {merge_bounds}, new size: {fmt_kb(new_size)}')

                    # update local map
                    chunks_id_index.pop(right_chunk['_id'])  # only first chunk is kept
                    chunks_min_index.pop(pickle.dumps(right_chunk['min']))
                    chunks_max_index.pop(pickle.dumps(c['max']))
                    chunks_max_index[pickle.dumps(right_chunk['max'])] = c
                    c['merged'] = True
                    c['shard'] = target_shard
                    c['max'] = right_chunk['max']
                    c['defrag_collection_est_size'] = new_size

                    if shard != target_shard:
                        total_shard_size[shard] -= center_size_kb
                        total_shard_size[target_shard] += center_size_kb
                        total_moved_data_kb += center_size_kb

                    # update stats for merged chunk (source)
                    progress.update(1)
                    #update stats for merged chunk (destination)
                    if right_size <= small_chunk_size_kb and new_size > small_chunk_size_kb:
                        progress.update(1)

                    check_max_migrations()
                    continue
        # </for c in sorted_chunks:>
        return total_moved_data_kb

    async def phase_2():
        # Move and merge small chunks. The way this is written it might need to run multiple times
        total_moved_data_kb = 0

        total_chunks_to_process = num_small_chunks + num_chunks_no_size

        logging.info(
            f"Number of small chunks: {num_small_chunks}, Number of chunks with unkown size: {num_chunks_no_size}"
        )
        if not total_chunks_to_process:
            return total_moved_data_kb

        with tqdm(total=total_chunks_to_process, unit=' chunks') as progress:
            iteration = 0
            while iteration < 25:
                iteration += 1
                progress.write(
                    f"""Phase II: iteration {iteration}. Remainging chunks to process {progress.total - progress.n}, total chunks {len(chunks_id_index)}"""
                )

                moved_data_kb = 0
                shards_to_process = [s for s in shard_to_chunks]
                while (shards_to_process):
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
                    num_chunks_actual = await cluster.configDb.chunks.count_documents(
                        coll.chunks_query_filter())
                    assert (num_chunks_actual == num_chunks)

                if moved_data_kb == 0 or progress.n == progress.total:
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

    avg_chunk_size_phase_1 = coll_size_kb / len(chunks_id_index)

    ###############  End stats calculation #############

    logging.info(
        f'Collection size {fmt_kb(coll_size_kb)}. Avg chunk size Phase I {fmt_kb(avg_chunk_size_phase_1)}'
    )

    for s in shard_to_chunks:
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks'])
        data_size = total_shard_size[s]
        logging.info(
            f"Number chunks on shard {s: >15}: {num_chunks_per_shard:7}  Data-Size: {fmt_kb(data_size): >9}"
        )

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
        - No split if chunk size < 133% target chunk size
        - Split otherwise
    '''

    async def split_oversized_chunks(shard, progress):
        shard_entry = shard_to_chunks[shard]
        shard_chunks = shard_entry['chunks']
        if args.dryrun or len(shard_chunks) == 0:
            return

        shard_entry = await coll.cluster.configDb.shards.find_one({'_id': shard})
        if shard_entry is None:
            raise Exception(f'Cannot resolve shard {shard}')

        conn = await coll.cluster.make_direct_shard_connection(shard_entry)
        last_split_time = time.perf_counter()
        for c in shard_chunks:
            progress.update()

            chunk_size = await get_chunk_size(c)

            if chunk_size > target_chunk_size_kb * 1.33:
                await throttle_if_necessary(last_split_time, args.phase3_throttle_secs)
                await coll.split_chunk(c, target_chunk_size_kb, conn)
                last_split_time = time.perf_counter()

        conn.close()

    global splits_performed_per_shard
    splits_performed_per_shard = {}
    if args.exec_phase == 'phase3' or args.exec_phase == 'all':
        logging.info(f'Phase III : Splitting oversized chunks')

        num_chunks = len(chunks_id_index)
        with tqdm(total=num_chunks, unit=' chunks') as progress:
            tasks = []
            for s in shard_to_chunks:
                splits_performed_per_shard[s] = 0
                tasks.append(asyncio.ensure_future(split_oversized_chunks(s, progress)))
                if args.phase3_throttle_secs:
                    await asyncio.gather(*tasks)
                    tasks.clear()
            await asyncio.gather(*tasks)

    else:
        logging.info("Skipping Phase III")

    if not args.dryrun and args.write_size_on_exit:
        await write_all_missing_chunk_size()

    print("\n")
    for s in shard_to_chunks:
        num_splits_per_shard = splits_performed_per_shard.get(s, 0)
        num_chunks_per_shard = len(shard_to_chunks[s]['chunks']) + num_splits_per_shard
        avg_chunk_size_shard = total_shard_size[
            s] / num_chunks_per_shard if num_chunks_per_shard > 0 else 0
        print(
            f"Number chunks on {s: >15}: {num_chunks_per_shard:7}  Data-Size: {fmt_kb(total_shard_size[s]): >9} "
            f" ({fmt_kb(total_shard_size[s] - orig_shard_sizes[s]): >9})  Avg chunk size {fmt_kb(avg_chunk_size_shard): >9}"
            f"  Splits performed {num_splits_per_shard}")

    total_coll_size_kb = sum(total_shard_size.values())
    total_num_chunks_phase_2 = len(chunks_id_index)
    avg_chunk_size_phase_2 = total_coll_size_kb / total_num_chunks_phase_2
    total_num_chunks_phase_3 = total_num_chunks_phase_2 + sum(splits_performed_per_shard.values())
    avg_chunk_size_phase_3 = total_coll_size_kb / total_num_chunks_phase_3
    ideal_num_chunks = math.ceil(total_coll_size_kb / target_chunk_size_kb)

    print("\n")
    print(
        f"""Number of chunks is {total_num_chunks_phase_3} the ideal number of chunks would be {ideal_num_chunks} for a collection size of {fmt_kb(total_coll_size_kb)}"""
    )
    print(
        f'Average chunk size: Phase I {fmt_kb(avg_chunk_size_phase_1)} | Phase II {fmt_kb(avg_chunk_size_phase_2)} | Phase III {fmt_kb(avg_chunk_size_phase_3)}'
    )
    print(
        f"Total moved data: {fmt_kb(total_moved_data_kb)} i.e. {(100 * total_moved_data_kb / total_coll_size_kb):.2f} %"
    )


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument(
        'uri',
        help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri',
        type=str,
    )

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
    argsParser.add_argument(
        '--small-chunk-threshold', help="""Threshold for the size of chunks 
        eligable to be moved in Phase II. Fractional value between 0 and 0.5""", metavar='fraction',
        dest='small_chunk_frac', type=float, default=0.25)
    argsParser.add_argument(
        '--shard-imbalance-threshold', help="""Threshold for the size difference 
        between two shards where chunks can be moved to. Fractional value between 1.0 and 1.5""",
        metavar='fraction', dest="shard_imbalance_frac", type=float, default=1.2)
    argsParser.add_argument('--no-write-chunk-size',
                            help="""Store chunk sizes in `config.chunks`""",
                            dest="no_write_chunk_size", action='store_true')
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
        metavar="fraction_of_chunk_size", dest='threshold_for_size_calculation', type=float,
        default=0.9)
    argsParser.add_argument('--phases',
                            help="""Which phase of the defragmentation algorithm to execute.""",
                            metavar='phase', dest="exec_phase", type=str, default='all',
                            choices=['all', 'phase1', 'phase2', 'phase3'])
    argsParser.add_argument(
        '--phase1_merge_batch_size',
        help="""Maximum number of chunks to merge in a single merge operation.""",
        metavar='merge_batch_size', dest="merge_batch_size", type=int, required=False)
    argsParser.add_argument('--phase_2_max_migrations', help="""Maximum number of migrations.""",
                            metavar='max_migrations', dest="max_migrations", type=int, default=-1)
    argsParser.add_argument(
        '--write-size-on-exit', help=
        """Used for debugging purposes, write all missing data size estimation on disk before exit.""",
        dest="write_size_on_exit", action='store_true')
    argsParser.add_argument(
        '--no-parallel-merges',
        help="""Specify whether merges should be executed in parallel or not.""",
        dest="no_parallel_merges", action='store_true')
    argsParser.add_argument(
        '--phase1-throttle-secs', help=
        """Specify the time in fractional seconds used to throttle phase1. Only one merge will be performed every X seconds.""",
        metavar='secs', dest='phase1_throttle_secs', type=float, default=0)
    argsParser.add_argument(
        '--phase2-throttle-secs', help=
        """Specify the time in fractional seconds used to throttle phase2. Only one merge will be performed every X seconds.""",
        metavar='secs', dest="phase2_throttle_secs", type=float, default=0)
    argsParser.add_argument(
        '--phase3-throttle-secs', help=
        """Specify the time in fractional seconds used to throttle phase3. Only one split will be performed every X seconds.""",
        metavar='secs', dest='phase3_throttle_secs', type=float, default=0)

    args = argsParser.parse_args()
    logging.info(f"CTools version {CTOOLS_VERSION} starting with arguments: '{args}'")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(args))
