"""
Module providing the Cluster abstraction for the ctools set of scripts.
"""

import asyncio

import motor.motor_asyncio

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from pymongo import uri_parser


class Cluster:
    """
    Abstracts the connection to and some administrative operations against a MongoDB cluster. This
    class is highly tailored to the usage in the ctools scripts in the same directory and is not a
    generic utility.
    """

    def __init__(self, uri):
        self.parsed_uri = uri_parser.parse_uri(uri)
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)

        # The internal cluster collections always use the standard UUID representation
        self.system_codec_options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)

        self.adminDb = self.client.get_database('admin', codec_options=self.system_codec_options)
        self.configDb = self.client.get_database('config', codec_options=self.system_codec_options)

    class NotMongosException(Exception):
        pass

    class BalancerEnabledException(Exception):
        pass

    async def configsvrConnectionString(self):
        serverStatus = await self.adminDb.command({'serverStatus': 1, 'sharding': 1})
        return serverStatus['sharding']['configsvrConnectionString']

    async def FCV(self):
        fcvDocument = await self.adminDb['system.version'].find_one(
            {'_id': 'featureCompatibilityVersion'})
        return fcvDocument['version']

    async def shardIds(self):
        return list(
            map(lambda x: x['_id'], await self.configDb.shards.find({}).sort('_id',
                                                                             1).to_list(None)))

    async def check_is_mongos(self, warn_only=False):
        print('Server is running at FCV', await self.FCV())
        try:
            ismaster = await self.adminDb.command('ismaster')
            if 'msg' not in ismaster or ismaster['msg'] != 'isdbgrid':
                raise Cluster.NotMongosException('Not connected to a mongos')
        except Cluster.NotMongosException:
            if warn_only:
                print('WARNING: Not connected to a MongoS')
            else:
                raise

    async def check_balancer_is_disabled(self, warn_only=False):
        try:
            balancer_status = await self.adminDb.command({'balancerStatus': 1})
            assert 'mode' in balancer_status, f'Unrecognized balancer status response: {balancer_status}'
            if balancer_status['mode'] != 'off':
                raise Cluster.BalancerEnabledException(
                    '''The balancer must be stopped before running this script.
                            Please run sh.stopBalancer()''')
        except Cluster.BalancerEnabledException:
            if warn_only:
                print('WARNING: Balancer is still enabled')
            else:
                raise

    async def make_direct_shard_connection(self, shard):
        if (isinstance(shard, str)):
            shard = await self.configDb.shards.find_one({'_id': shard})

        conn_parts = shard['host'].split('/', 1)
        if self.parsed_uri['username']:
            uri = 'mongodb://' + self.parsed_uri['username'] + ':' + self.parsed_uri[
                'password'] + '@' + conn_parts[1]
        else:
            uri = 'mongodb://' + conn_parts[1]

        return motor.motor_asyncio.AsyncIOMotorClient(uri, replicaset=conn_parts[0],
                                                      **self.parsed_uri['options'])

    async def make_direct_config_server_connection(self):
        return await self.make_direct_shard_connection({
            '_id': 'config',
            'host': await self.configsvrConnectionString()
        })

    async def on_each_shard(self, fn):
        tasks = []
        async for shard in self.configDb.shards.find({}):
            tasks.append(
                asyncio.ensure_future(
                    fn(shard['_id'], await self.make_direct_shard_connection(shard))))
        await asyncio.gather(*tasks)
