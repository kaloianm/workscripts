"""
Module providing common functionality for the ctools set of scripts.
"""

import asyncio
import datetime
import logging
import sys

import aiofiles
import bson
import motor.motor_asyncio

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo import uri_parser


class CToolsException(Exception):
    """
    Exception which serves as a common base for all exceptions thrown by the ctools suite.
    """

    def __init__(self, *args, **kwargs):
        super(CToolsException, self).__init__(*args, **kwargs)


def yes_no(answer):
    '''Function for a Yes/No result based on the answer provided as an argument'''

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


def exe_name(name):
    '''Abstracts constructing the name of an executable on POSIX vs Windows platforms'''

    if (sys.platform == 'win32'):
        return name + '.exe'
    return name


async def async_start_shell_command(command, logging_prefix):
    """
    Asynchronously starts a shell command and logs its stdin/stderr to the logging subsystem.
    """

    logging.info(f'[{logging_prefix}]: {command}')

    async with aiofiles.tempfile.TemporaryFile() as temp_file:
        command_shell_process = await asyncio.create_subprocess_shell(command, stdout=temp_file,
                                                                      stderr=temp_file)
        await command_shell_process.wait()

        await temp_file.seek(0)
        async for line in temp_file:
            stripped_line = line.decode('ascii').replace('\n', '')
            logging.info(f'[{logging_prefix}]: {stripped_line}')

        if command_shell_process.returncode != 0:
            raise CToolsException(
                f'[{logging_prefix}]: Command failed with code {command_shell_process.returncode}')


class Cluster:
    """
    Abstracts the connection to and some administrative operations against a MongoDB cluster. This
    class is highly tailored to the usage in the ctools scripts in the same directory and is not a
    generic utility.
    """

    def __init__(self, uri, loop):
        self.uri_options = uri_parser.parse_uri(uri)['options']
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)

        # The internal cluster collections always use the standard UUID representation
        self.system_codec_options = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)

        self.adminDb = self.client.get_database('admin', codec_options=self.system_codec_options)
        self.configDb = self.client.get_database('config', codec_options=self.system_codec_options)

    class NotMongosException(Exception):
        pass

    class BalancerEnabledException(Exception):
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
        uri = 'mongodb://' + conn_parts[1]
        return motor.motor_asyncio.AsyncIOMotorClient(uri, replicaset=conn_parts[0],
                                                      **self.uri_options)

    async def make_direct_config_server_connection(self):
        return await self.make_direct_shard_connection({
            '_id': 'config',
            'host': await self.configsvrConnectionString
        })

    async def on_each_shard(self, fn):
        tasks = []
        async for shard in self.configDb.shards.find({}):
            tasks.append(
                asyncio.ensure_future(
                    fn(shard['_id'], await self.make_direct_shard_connection(shard))))
        await asyncio.gather(*tasks)


class ShardCollectionUtil:
    """
    Utility class to generate the documents for manually sharding a collection through a process
    external to the core server. Does not perform any modifications to the cluster itself.
    """

    def __init__(self, ns, uuid, shard_key, unique, fcv):
        self.ns = ns
        self.uuid = uuid
        self.shard_key = shard_key
        self.unique = unique
        self.fcv = fcv

        self.shard_key_is_string = (self.fcv <= '4.2')

        self.epoch = bson.objectid.ObjectId()
        self.creation_time = datetime.datetime.now()
        self.timestamp = bson.timestamp.Timestamp(self.creation_time, 1)

        logging.info(f'''Sharding an existing collection {self.ns} with the following parameters:
                            uuid: {self.uuid}
                            shard_key: {self.shard_key}
                            unique: {self.unique}
        ''')

    # Accepts an array of tuples which must contain exactly the following fields:
    #    min, max, shard
    # AND MUST be sorted according to range['min']
    def generate_config_chunks(self, chunks):

        def make_chunk_id(i):
            if self.shard_key_is_string:
                return f'shard-key-{self.ns}-{str(i).zfill(8)}'
            else:
                return ObjectId()

        chunk_idx = 0
        for c in chunks:
            chunk_obj = {
                '_id': make_chunk_id(chunk_idx),
                'min': c['min'],
                'max': c['max'],
                'shard': c['shard'],
                'lastmod': bson.timestamp.Timestamp(1, chunk_idx),
            }

            if self.fcv >= '5.0':
                chunk_obj.update({'uuid': self.uuid})
            else:
                chunk_obj.update({
                    'ns': self.ns,
                    'lastmodEpoch': self.epoch,
                })

            chunk_idx += 1
            yield chunk_obj

    def generate_collection_entry(self):
        coll_obj = {
            '_id': self.ns,
            'lastmodEpoch': self.epoch,
            'lastmod': self.creation_time,
            'key': self.shard_key,
            'unique': self.unique,
            'uuid': self.uuid
        }

        if self.fcv >= '5.0':
            coll_obj.update({'timestamp': self.timestamp})
        else:
            coll_obj.update({'dropped': False})

        return coll_obj
