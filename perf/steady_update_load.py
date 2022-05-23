# Locust-based read/update workload

from locust import User, constant_pacing, events, tag, task
from pymongo import MongoClient
from random import randrange
from time import perf_counter_ns

connection_string = None
mongo_client = None
collection = None


def nanos_to_millis(nanos):
    return round(nanos / 1000000.0, 2)


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global connection_string
    connection_string = 'mongodb://localhost' if environment.host is None else environment.host

    print('Starting run with a MongoDB host of ', connection_string)

    global mongo_client
    mongo_client = MongoClient(connection_string)
    database = mongo_client['MDBW22']

    global collection
    collection = database['BalancerDemo']


class Mongouser(User):
    # This user will generate a constant load of 1 request per second
    wait_time = constant_pacing(1)

    def on_start(self):
        self._switch_account_id()

    def _switch_account_id(self):
        # TODO: Use a natural number query instead
        self.account_id = randrange(0, 180000000)

    @task(66)
    def find_account(self):
        start_time = perf_counter_ns()
        self._switch_account_id()

        collection.find_one({'account_id': self.account_id})

        self.environment.events.request_success.fire(
            request_type='find_account', name='find_account',
            response_time=nanos_to_millis(perf_counter_ns() - start_time), response_length=0)

    @task(33)
    def update_account(self):
        start_time = perf_counter_ns()
        self._switch_account_id()

        collection.update_one({'account_id': self.account_id}, {'$inc': {'modifications': 1}})

        self.environment.events.request_success.fire(
            request_type='update_account', name='update_account',
            response_time=nanos_to_millis(perf_counter_ns() - start_time), response_length=0)
