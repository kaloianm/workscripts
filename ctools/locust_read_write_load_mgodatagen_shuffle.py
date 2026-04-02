#!/usr/bin/env python3
#
# Wraps mgodatagen to produce a shuffled insertion order for the shardKey range,
# breaking the correlation between shardKey values and WiredTiger RecordIds.
#
# With plain mgodatagen (autoincrement), shardKey k lands at RecordId k+1, making
# the shardKey index perfectly co-sorted with collection storage.  This script
# splits the [startInt, startInt+count) range into --shuffle-factor equal batches
# and feeds them to mgodatagen in a randomised order.  The result is a collection
# where the shardKey B-tree index references RecordIds in a non-monotonic pattern,
# producing more realistic I/O for range-based operations.
#
# The first batch creates the collection (no indexes); intermediate batches append
# documents; the last batch appends its documents and creates all indexes.
# Deferring index creation to the end avoids per-document index maintenance
# overhead during the bulk load phase.
#
# Example usage:
#   python3 locust_read_write_load_mgodatagen_shuffle.py --shuffle-factor 8 -f locust_read_write_load_mgodatagen_50GB.json --uri "mongodb://URL/?directConnection=false"
#
#

import argparse
import copy
import json
import logging
import os
import random
import subprocess
import sys
import tempfile
from time import perf_counter


def parse_args():
    p = argparse.ArgumentParser(
        description='Run mgodatagen with a shuffled shardKey insertion order.',
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument('-f', dest='config_file', required=True, metavar='CONFIG',
                   help='mgodatagen JSON config file')
    p.add_argument('--uri', required=True,
                   help='MongoDB connection URI (same as passed to mgodatagen)')
    p.add_argument(
        '--shuffle-factor', type=int, default=8, metavar='K',
        help=('Number of equal batches to split the shardKey range into (default: 8). '
              'The batches are run in a random order, so K=8 produces 8 contiguous '
              'shardKey segments whose insertion order is shuffled.  Higher values '
              'increase interleaving at the cost of more mgodatagen invocations.  '
              'K=1 is equivalent to plain mgodatagen with no shuffling.'))
    p.add_argument('--seed', type=int, default=None,
                   help='Random seed for reproducible batch ordering (default: random)')
    p.add_argument('--dry-run', action='store_true',
                   help='Print the batch plan without running mgodatagen')
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  %(message)s',
                        datefmt='%H:%M:%S')

    args = parse_args()

    if args.shuffle_factor < 1:
        logging.error('--shuffle-factor must be >= 1')
        sys.exit(1)

    with open(args.config_file) as f:
        config = json.load(f)

    coll = config[0]
    sk = coll['content']['shardKey']

    if sk.get('type') != 'autoincrement':
        logging.error("shardKey.type must be 'autoincrement', got %r", sk.get('type'))
        sys.exit(1)

    total = coll['count']
    start = sk.get('startInt', 0)
    k = min(args.shuffle_factor, total)

    if k != args.shuffle_factor:
        logging.warning('--shuffle-factor clamped to %d (= count)', k)

    # Split [start, start+total) into k equal batches.
    batch_size, remainder = divmod(total, k)
    batches = []
    cur = start
    for i in range(k):
        n = batch_size + (1 if i < remainder else 0)
        batches.append((cur, n))
        cur += n

    # Shuffle the insertion order.
    rng = random.Random(args.seed)
    rng.shuffle(batches)

    w = len(str(k))  # width for batch index formatting
    logging.info('Config     : %s', args.config_file)
    logging.info('Namespace  : %s.%s', coll['database'], coll['collection'])
    logging.info('Total docs : %s   shardKey range [%d, %d)', f'{total:,}', start, start + total)
    logging.info('Batches    : %d of ~%s docs each', k, f'{batch_size:,}')
    logging.info('Seed       : %s', args.seed if args.seed is not None else 'random')
    logging.info('')
    logging.info('Insertion order:')
    for i, (s, n) in enumerate(batches):
        logging.info('  batch %*d/%d : shardKeys [%s, %s)', w, i + 1, k, f'{s:,}', f'{s + n:,}')

    if args.dry_run:
        logging.info('')
        logging.info('Dry run — exiting without calling mgodatagen.')
        return

    t_start = perf_counter()

    for i, (batch_start, batch_count) in enumerate(batches):
        # Build a config with just this batch's startInt and count.
        batch_config = copy.deepcopy(config)
        batch_config[0]['count'] = batch_count
        batch_config[0]['content']['shardKey']['startInt'] = batch_start

        # Strip indexes from every batch except the last so that the index
        # build runs once after all documents are present rather than
        # maintaining the B-trees on every inserted document.
        is_last = (i == len(batches) - 1)
        if not is_last:
            batch_config[0].pop('indexes', None)

        tf = tempfile.NamedTemporaryFile(mode='w', suffix='.json', prefix='mgodatagen_shuffle_',
                                         delete=False)
        try:
            json.dump(batch_config, tf, indent=2)
            tf.close()

            cmd = ['mgodatagen', '-f', tf.name, '--uri', args.uri]
            if i > 0:
                cmd.append('--append')

            elapsed = perf_counter() - t_start
            logging.info('')
            logging.info('--- batch %*d/%d : shardKeys [%s, %s)  (+%s docs)  elapsed %.0fs', w,
                         i + 1, k, f'{batch_start:,}', f'{batch_start + batch_count:,}',
                         f'{batch_count:,}', elapsed)

            subprocess.run(cmd, check=True)

        except subprocess.CalledProcessError as e:
            logging.error('mgodatagen failed on batch %d/%d (startInt=%d): %s', i + 1, k,
                          batch_start, e)
            sys.exit(1)
        finally:
            os.unlink(tf.name)

    logging.info('')
    logging.info('Done: %s docs in %d batches, %.0fs total.', f'{total:,}', k,
                 perf_counter() - t_start)


if __name__ == '__main__':
    main()
