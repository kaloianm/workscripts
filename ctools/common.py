# Helper utilities used by the ctools scripts
#

import asyncio
import motor.motor_asyncio
import sys

from bson.codec_options import CodecOptions


# Function for a Yes/No result based on the answer provided as an argument
def yes_no(answer):
    yes = set(['yes', 'y', 'ye', ''])
    no = set(['no', 'n'])

    while True:
        choice = input(answer).lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond with 'yes' or 'no'\n")


# Abstracts constructing the name of an executable on POSIX vs Windows platforms
def exe_name(name):
    if (sys.platform == 'win32'):
        return name + '.exe'
    return name


class Cluster:
    def __init__(self, uri, loop):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)

        self.adminDb = self.client.admin.with_options(codec_options=CodecOptions(uuid_representation=4))
        self.configDb = self.client.config.with_options(
            codec_options=CodecOptions(uuid_representation=4))

    class NotMongosException(Exception):
        pass

    async def checkIsMongos(self):
        ismaster = await self.adminDb.command('ismaster')
        if 'msg' not in ismaster or ismaster['msg'] != 'isdbgrid':
            raise Cluster.NotMongosException('Not connected to a mongos')

    @property
    async def shardIds(self):
        return list(
            map(lambda x: x['_id'], await self.configDb.shards.find({}).sort('_id',
                                                                             1).to_list(None)))

    async def adminCommand(self, *args, **kwargs):
        return await self.client.admin.command(*args, **kwargs)

    async def runOnEachShard(self, fn):
        async for shard in cluster.configDb.shards.find({}):
            connParts = shard['host'].split('/', 1)
            conn = motor.motor_asyncio.AsyncIOMotorClient(shardConnParts[1],
                                                          replicaset=shardConnParts[0])
            fn(shard['_id'], conn)
