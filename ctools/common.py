# Helper utilities used by the ctools scripts
#

import asyncio
import motor.motor_asyncio
import sys

from bson.codec_options import CodecOptions


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
    def __init__(self, uri, loop, uuidRepresentation):
        self.uuidRepresentation = uuidRepresentation

        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            uri, uuidRepresentation=self.uuidRepresentation)

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

    async def checkIsMongos(self, warn_only=False):
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

    async def adminCommand(self, *args, **kwargs):
        return await self.client.admin.command(*args, **kwargs)

    async def runOnEachShard(self, fn):
        async for shard in cluster.configDb.shards.find({}):
            connParts = shard['host'].split('/', 1)
            conn = motor.motor_asyncio.AsyncIOMotorClient(shardConnParts[1],
                                                          replicaset=shardConnParts[0])
            fn(shard['_id'], conn)
