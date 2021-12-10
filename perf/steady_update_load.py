#
# 1. Import the 10GB/20GB dataset required for the test in the cluster:
#     $HOME/mongodb/tools/mongoimport --db=BalanceTest --collection=Posts10GB --numInsertionWorkers=16 --file=$HOME/Temp/10GBset.json mongodb://localhost
#     $HOME/mongodb/tools/mongoimport --db=BalanceTest --collection=Posts20GB --numInsertionWorkers=16 --file=$HOME/Temp/20GBset.json mongodb://localhost
# 2. db.Posts20GB.createIndex({ customer_id: 1 })
# 3. sh.stopBalancer()
# 4. sh.enableSharding('BalanceTest')
# 5. sh.shardCollection('BalanceTest.Posts20GB', { customer_id: 1 })
#

import random
import string

from locust import User, constant_pacing, events, tag, task
from pymongo import MongoClient
from random import randrange
from time import perf_counter

connection_string = None
mongo_client = None
collection = None


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global connection_string
    connection_string = 'mongodb://localhost' if environment.host is None else environment.host

    print('Starting run with a MongoDB host of ', connection_string)

    global mongo_client
    mongo_client = MongoClient(connection_string)
    database = mongo_client['BalanceTest']

    global collection
    collection = database['Posts20GB']


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
        self.customer_id = randrange(0, 11077064)

    @task
    def update_post(self):
        start_time = perf_counter()

        self._switch_customer_id()
        collection.update_one({"customer_id": self.customer_id}, {
            "$set": {
                "updated_post": make_random_string(128)
            },
            "$inc": {
                "modifications": 1
            }
        })

        self._switch_customer_id()
        collection.update_one({"customer_id": self.customer_id}, {
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
