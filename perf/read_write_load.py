'''
Locust-based read/update workload
Example usage:
 locust -f perf/steady_update_load.py --users 500 --spawn-rate 100 --autostart --web-port 8090/8091 --host hostname
'''

from locust import User, constant_pacing, events, tag, task
from pymongo import MongoClient, ReadPreference
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
    collection = database.get_collection('BalancerDemo', read_preference=ReadPreference.PRIMARY)


class Mongouser(User):
    # This user will generate a constant load of 1 request per second
    wait_time = constant_pacing(1)

    def on_start(self):
        self._switch_account_id()

    def _switch_account_id(self):
        # TODO: Use a natural number query instead
        self.account_id = randrange(0, 350000000)

    @task(60)
    def find_account(self):
        start_time = perf_counter_ns()
        self._switch_account_id()

        assert collection.find_one({'account_id': self.account_id, 'account_sub_id': 0}) is not None

        self.environment.events.request_success.fire(
            request_type='find_account', name='find_account',
            response_time=nanos_to_millis(perf_counter_ns() - start_time), response_length=0)

    @task(30)
    def update_account(self):
        start_time = perf_counter_ns()
        self._switch_account_id()

        update_result = collection.update_one({
            'account_id': self.account_id,
            'account_sub_id': 0
        }, {'$inc': {
            'modifications': 1
        }})

        assert update_result.modified_count > 0

        self.environment.events.request_success.fire(
            request_type='update_account', name='update_account',
            response_time=nanos_to_millis(perf_counter_ns() - start_time), response_length=0)

    @task(10)
    def create_account(self):
        start_time = perf_counter_ns()
        self._switch_account_id()

        insert_result = collection.insert_one({
            'account_id': self.account_id,
            'account_sub_id': 1,
            'modifications': 1
        })

        assert insert_result.inserted_id is not None

        self.environment.events.request_success.fire(
            request_type='create_account', name='create_account',
            response_time=nanos_to_millis(perf_counter_ns() - start_time), response_length=0)
