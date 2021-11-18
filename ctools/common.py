# Helper utilities used by the ctools scripts
#

import asyncio
import motor.motor_asyncio
import sys

from bson.binary import UuidRepresentation
from pymongo import uri_parser


# Function for a Yes/No result based on the answer provided as an argument
def yes_no(answer):
    yes = set(['yes', 'y', 'y'])
    no = set(['no', 'n', ''])

    while True:
        choice = input(answer + '\nProceed (yes/NO)? ').lower()
        if choice in yes:
            return
        elif choice in no:
            raise KeyboardInterrupt('User canceled')
        else:
            print("Please respond with 'yes' or 'no'\n")


# Abstracts constructing the name of an executable on POSIX vs Windows platforms
def exe_name(name):
    if (sys.platform == 'win32'):
        return name + '.exe'
    return name


class Cluster:
    def __init__(self, uri, loop):
        self.uri_options = uri_parser.parse_uri(uri)['options']
        if 'uuidRepresentation' in self.uri_options:
            self.uuid_representation = self.uri_options['uuidRepresentation']
        else:
            self.uuid_representation = None

        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)

        self.adminDb = self.client.admin
        self.configDb = self.client.config

    class NotMongosException(Exception):
        pass

    @property
    async def configsvrConnectionString(self):
        serverStatus = await self.adminDb.command({'serverStatus': 1, 'sharding': 1})
        return serverStatus['sharding']['configsvrConnectionString']

    @property
    async def FCV(self):
        fcvDocument = await self.adminDb['system.version'].find_one(
            {'_id': 'featureCompatibilityVersion'})
        return fcvDocument['version']

    @property
    async def shardIds(self):
        return list(
            map(lambda x: x['_id'], await self.configDb.shards.find({}).sort('_id',
                                                                             1).to_list(None)))

    async def check_is_mongos(self, warn_only=False):
        print('Server is running at FCV', await self.FCV)
        try:
            ismaster = await self.adminDb.command('ismaster')
            if 'msg' not in ismaster or ismaster['msg'] != 'isdbgrid':
                raise Cluster.NotMongosException('Not connected to a mongos')
        except Cluster.NotMongosException:
            if warn_only:
                print('WARNING: Not connected to a MongoS')
            else:
                raise

    async def make_direct_shard_connection(self, shard):
        conn_parts = shard['host'].split('/', 1)
        options = [key + '=' + str(self.uri_options[key]) for key in self.uri_options]
        str_options = '&'.join(options).replace('=True', '=true').replace('=False', '=false')
        uri = 'mongodb://' + conn_parts[1] + '/?' + str_options
        if self.uuid_representation:
            UUID_REPRESENTATIONS = {
                UuidRepresentation.UNSPECIFIED: 'unspecified',
                UuidRepresentation.STANDARD: 'standard',
                UuidRepresentation.PYTHON_LEGACY: 'pythonLegacy',
                UuidRepresentation.JAVA_LEGACY: 'javaLegacy',
                UuidRepresentation.CSHARP_LEGACY: 'csharpLegacy'
            }
            return motor.motor_asyncio.AsyncIOMotorClient(
                uri, replicaset=conn_parts[0],
                uuidRepresentation=UUID_REPRESENTATIONS[self.uuid_representation])
        else:
            return motor.motor_asyncio.AsyncIOMotorClient(uri, replicaset=conn_parts[0])

    async def on_each_shard(self, fn):
        tasks = []
        async for shard in self.configDb.shards.find({}):
            tasks.append(
                asyncio.ensure_future(fn(shard['_id'], self.make_direct_shard_connection(shard))))
        await asyncio.gather(*tasks)

    async def make_direct_config_server_connection(self):
        return await self.make_direct_shard_connection({
            '_id': 'config',
            'host': await self.configsvrConnectionString
        })
