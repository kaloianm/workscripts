#
# 1. Import 10GB data in the cluster:
#   ./mongoimport --db=BalanceTestDB --collection=Posts --numInsertionWorkers=16 --file=10GBset.json mongodb://localhost
#

import random
import string

from locust import User, constant_pacing, events, tag, task
from pymongo import MongoClient
from random import randrange
from time import perf_counter

connection_string = None
mongo_client = None
database = None


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global connection_string
    connection_string = 'mongodb://localhost' if environment.host is None else environment.host

    print('Starting run with a MongoDB host of ', connection_string)

    global mongo_client
    mongo_client = MongoClient(connection_string)

    global database
    database = mongo_client['BalanceTestDB']


def make_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


class Mongouser(User):
    # This user will generate a constant load of 1 request per second
    wait_time = constant_pacing(1)

    def on_start(self):
        self._switch_customer_id()

    def _switch_customer_id(self):
        # TODO: Use a natural number query instead
        self.customer_id = randrange(0, 11077057)

    @task
    def update_post(self):
        start_time = perf_counter()

        self._switch_customer_id()
        database.Posts.update_one({"customer_id": self.customer_id}, {
            "$set": {
                "updated_post": make_random_string(128)
            },
            "$inc": {
                "modifications": 1
            }
        })

        self._switch_customer_id()
        database.Posts.update_one({"customer_id": self.customer_id}, {
            "$set": {
                "updated_post": make_random_string(128)
            },
            "$inc": {
                "modifications": 1
            }
        })

        self.environment.events.request_success.fire(request_type="update_post", name="update_post",
                                                     response_time=perf_counter() - start_time,
                                                     response_length=0)
