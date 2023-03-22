#!/usr/bin/env python3
#
help_string = '''
Locust-based create/read/update workload.

Example usage:
  locust_read_write_load.py --users 500 --web-port 8090 <MongoDB URI>
'''

import argparse
import asyncio
import logging
import sys

from bson import Int64
from common import async_start_shell_command
from locust import User, constant_pacing, events, tag, task
from pymongo import MongoClient, ReadPreference
from random import randrange
from time import perf_counter_ns

connection_string = None
mongo_client = None
collection = None

# Workload configurations
min_shard_key = 0
max_shard_key = 35000000


def nanos_to_millis(nanos):
    return round(nanos / 1000000.0, 2)


@events.init_command_line_parser.add_listener
def on_locust_init_command_line_parser(parser):
    parser.add_argument('--ns', help='Namespace to use', metavar='ns', type=str, default='Perf')


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global connection_string
    connection_string = 'mongodb://localhost' if environment.host is None else environment.host

    logging.info('Starting run with a MongoDB host of ', connection_string)
    logging.info('Collection: ', environment.parsed_options.ns)

    ns = {
        'db': environment.parsed_options.ns.split('.', 1)[0],
        'coll': environment.parsed_options.ns.split('.', 1)[1],
    }

    global mongo_client
    mongo_client = MongoClient(connection_string)
    database = mongo_client[ns['db']]

    global collection
    collection = database.get_collection(ns['coll'], read_preference=ReadPreference.PRIMARY)


class MongoUser(User):
    '''This user will generate a constant load of 1 request per second.'''

    wait_time = constant_pacing(1)

    def on_start(self):
        self.select_shard_key()

    @task(10)
    def insert_shard_key(self):
        self.shard_key = Int64(randrange(min_shard_key, max_shard_key))

        start_time = perf_counter_ns()

        upsert_result = collection.update_one(
            filter={
                'shardKey': self.shard_key,
            },
            update={
                '$inc': {
                    'updates': 1,
                },
            },
            upsert=True,
        )
        assert (upsert_result.modified_count > 0,
                f"Upsert for {self.shard_key} didn't result in any documents being upserted")

        self.environment.events.request_success.fire(
            request_type='insert_shard_key',
            name='insert_shard_key',
            response_time=nanos_to_millis(perf_counter_ns() - start_time),
            response_length=0,
        )

    @task(40)
    def update_by_shard_key(self):
        start_time = perf_counter_ns()

        update_result = collection.update_one(
            filter={
                'shardKey': self.shard_key,
            },
            update={
                '$inc': {
                    'updates': 1,
                },
            },
            upsert=False,
        )
        assert (update_result.modified_count > 0,
                f"Update for {self.shard_key} didn't result in any documents being updated")

        self.environment.events.request_success.fire(
            request_type='update_by_shard_key',
            name='update_by_shard_key',
            response_time=nanos_to_millis(perf_counter_ns() - start_time),
            response_length=0,
        )

    @task(50)
    def find_by_shard_key(self):
        start_time = perf_counter_ns()

        self.select_shard_key()

        self.environment.events.request_success.fire(
            request_type='find_by_shard_key',
            name='find_by_shard_key',
            response_time=nanos_to_millis(perf_counter_ns() - start_time),
            response_length=0,
        )

    def select_shard_key(self):
        closest_shard_key = collection.find_one(
            filter={
                'shardKey': {
                    '$gte': Int64(randrange(min_shard_key, max_shard_key)),
                },
            }, )
        assert closest_shard_key is not None

        self.shard_key = closest_shard_key


async def main(args):
    logging.info(f"Starting with configuration: '{args}'")

    tasks = []

    # Start the coordinator
    coordinator_command = (f'locust -f {__file__} '
                           f'--master --master-bind-port {args.coordinator_port} '
                           f'--users {args.users} --spawn-rate 100 --autostart '
                           f'--web-port {args.web_port} '
                           f'--host {args.host} '
                           f'--ns {args.ns} ')
    logging.info(coordinator_command)

    tasks.append(
        asyncio.ensure_future(async_start_shell_command(coordinator_command, 'coordinator')))

    for _ in range(0, 4):
        worker_command = (f'{sys.executable} -m '
                          f'locust -f {__file__} '
                          f'--worker --master-port {args.coordinator_port} '
                          f'--host {args.host} '
                          f'--ns {args.ns} ')
        tasks.append(asyncio.ensure_future(async_start_shell_command(worker_command, 'worker')))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(description=help_string)
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    argsParser.add_argument('host', help='The host against which to run', metavar='host', type=str)
    argsParser.add_argument('--coordinator-port',
                            help='The port on which the coordinator server will listen',
                            metavar='coordinator-port', type=int, default=9090)
    argsParser.add_argument('--web-port', help='The port on which the web server will listen',
                            metavar='web-port', type=int)
    argsParser.add_argument('--users', help='How many users to generate', metavar='users', type=int)
    argsParser.add_argument('--ns', help='Namespace to use', metavar='ns', type=str, default='Perf')

    logging.info(f'Running with Python source file of {__file__}')
    logging.info(f'Using interpreter of {sys.executable}')
    args = argsParser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
