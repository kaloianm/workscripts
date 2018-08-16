#!/usr/bin/env python3
#

import argparse
import os
import psutil
import shutil
import subprocess

from bson.codec_options import CodecOptions
from common import exe_name, yes_no
from pymongo import MongoClient


# Class to abstract the tool's command line parameters configuration
class ToolConfiguration:

    # Class initialization. The 'args' parameter contains the parsed tool command line arguments.
    def __init__(self, args):
        self.binarypath = args.binarypath
        self.dir = args.dir
        self.configdump = args.configdumpdir[0]

        self.mongoRestoreBinary = os.path.join(self.binarypath, exe_name('mongorestore'))

        self.clusterIntrospectMongoDPort = 19000
        self.clusterStartingPort = 20000

        self.mongoRestoreNumInsertionWorkers = 16

        print('Running cluster import with binaries located at: ', self.binarypath)
        print('Data directory root at: ', self.dir)
        print('Config dump directory at: ', self.configdump)

        if (not self.__cleanup_previous_runs()):
            raise FileExistsError('Unable to cleanup any previous runs.')

        os.makedirs(self.dir)

        # Make it unbuffered so the output of the subprocesses shows up immediately in the file
        kOutputLogFileBufSize = 256
        self._outputLogFile = open(
            os.path.join(self.dir, 'reconstruct.log'), 'w', kOutputLogFileBufSize)

    def log_line(self, line):
        self._outputLogFile.write(str(line) + '\n')

    # Invokes mongorestore of the config database dump against an instance running on 'restorePort'.
    def restore_config_db_to_port(self, restorePort):
        mongorestoreCommand = [
            self.mongoRestoreBinary, '--port',
            str(restorePort), '--numInsertionWorkersPerCollection',
            str(self.mongoRestoreNumInsertionWorkers), self.configdump
        ]

        print('Executing mongorestore command: ' + ' '.join(mongorestoreCommand))
        subprocess.check_call(mongorestoreCommand, stdout=self._outputLogFile,
                              stderr=self._outputLogFile)

    # Invokes the specified 'action' of mlaunch with 'dir' as the environment directory. All the
    # remaining 'args' are just appended to the thus constructed command line.
    def mlaunch_action(self, action, dir, args=[]):
        if (not isinstance(args, list)):
            raise TypeError('args must be of type list.')

        mlaunchPrefix = ['mlaunch', action, '--binarypath', self.binarypath, '--dir', dir]

        if (set(mlaunchPrefix) & set(args)):
            raise ValueError('args duplicates values in mlaunchPrefix')

        mlaunchCommand = mlaunchPrefix + args

        print('Executing mlaunch command: ' + ' '.join(mlaunchCommand))
        subprocess.check_call(mlaunchCommand, stdout=self._outputLogFile)

    # Performs cleanup by killing all potentially running mongodb processes and deleting any
    # leftover files. Basically leaves '--dir' empty.
    def __cleanup_previous_runs(self):
        if (not yes_no('The next step will kill all mongodb processes and wipe out the data path.\n'
                       + 'Proceed (yes/no)? ')):
            return False

        # Iterate through all processes and kill mongod and mongos
        for process in psutil.process_iter():
            try:
                processExecutable = os.path.basename(process.exe())
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass
            else:
                if (processExecutable in [exe_name('mongod'), exe_name('mongos')]):
                    process.kill()
                    process.wait()

        # Remove the output directory
        shutil.rmtree(self.dir)
        return True


# Abstracts management of the 'introspect' mongod instance which is used to read the cluster
# configuration to instantiate
class ClusterIntrospect:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration.
    def __init__(self, config):
        self._config = config

        self._dir = os.path.join(config.dir, 'introspect')
        os.makedirs(self._dir)

        print('Introspecting config dump using instance at port ',
              config.clusterIntrospectMongoDPort)

        # Start the instance and restore the config server dump
        config.mlaunch_action(
            'init', self._dir,
            ['--single', '--port', str(config.clusterIntrospectMongoDPort)])
        config.restore_config_db_to_port(config.clusterIntrospectMongoDPort)

        # Open a connection to the introspect instance
        self.configDb = MongoClient('localhost', config.clusterIntrospectMongoDPort).config

    def terminate(self):
        self._config.mlaunch_action('stop', self._dir)


def generate_shard_name(shardIdCounter):
    if (shardIdCounter < 10):
        return 'shard0' + str(shardIdCounter)
    return 'shard' + str(shardIdCounter)


# Main entrypoint for the application
def main():
    argsParser = argparse.ArgumentParser(
        description=
        'Tool to interpret an export of a cluster config database and construct a new cluster with'
        'exactly the same configuration. Requires mlaunch to be installed and in the system path.')
    argsParser.add_argument('--binarypath', help='Directory containing the MongoDB binaries',
                            metavar='binarypath', type=str, required=True)
    argsParser.add_argument(
        '--dir', help='Directory in which to place the data files (will create subdirectories)',
        metavar='dir', type=str, required=True)
    argsParser.add_argument('configdumpdir',
                            help='Directory containing a dump of the cluster config database',
                            metavar='configdumpdir', type=str, nargs=1)

    config = ToolConfiguration(argsParser.parse_args())

    # Read the cluster configuration from the preprocess instance and construct the new cluster
    introspect = ClusterIntrospect(config)

    numShards = introspect.configDb.shards.count({})
    if (numShards > 10):
        if (not yes_no('The imported configuration data contains large number of shards (' + str(
                numShards) + '). Proceeding will start large number of mongod processes.\n' +
                       'Are you sure you want to continue (yes/no)? ')):
            return 1

    # Make the output directories
    mongodClusterRootPath = os.path.join(config.dir, 'cluster_root')
    os.makedirs(mongodClusterRootPath)

    config.mlaunch_action('init', mongodClusterRootPath, [
        '--replicaset', '--nodes', '1', '--sharded',
        str(numShards), '--csrs', '--mongos', '1', '--port',
        str(config.clusterStartingPort)
    ])

    configServerPort = config.clusterStartingPort + numShards + 1
    config.restore_config_db_to_port(configServerPort)

    configServerConnection = MongoClient('localhost', configServerPort)
    configServerConfigDB = configServerConnection.config

    # Rename the shards from the dump to the shards launched by mlaunch
    print('Renaming shards:')
    existingShardIds = []
    newShardIds = []
    shardIdCounter = 1
    for shard in introspect.configDb.shards.find({}):
        existingShardId = shard['_id']
        existingShardIds.append(existingShardId)

        newShardId = generate_shard_name(shardIdCounter)
        print('Shard ' + existingShardId + ' becomes ' + newShardId)
        newShardIds.append(newShardId)

        if 'tags' in shard:
            result = configServerConfigDB.shards.update_many({
                '_id': newShardId
            }, {'$set': {
                'tags': shard['tags']
            }})
            config.log_line(result.raw_result)

        result = configServerConfigDB.databases.update_many({
            'primary': existingShardId
        }, {'$set': {
            'primary': newShardId
        }})
        config.log_line(result.raw_result)

        result = configServerConfigDB.chunks.update_many({
            'shard': existingShardId
        }, {'$set': {
            'shard': newShardId
        }})
        config.log_line(result.raw_result)

        shardIdCounter = shardIdCounter + 1

    result = configServerConfigDB.shards.delete_many({
        '_id': {
            '$not': {
                '$in': list(map(str, newShardIds))
            }
        }
    })
    config.log_line(result.raw_result)

    # Create the collections and construct sharded indexes on all shard nodes
    for shard in configServerConfigDB.shards.find({}):
        print('Creating shard key indexes on shard ' + shard['_id'])

        shardConnParts = shard['host'].split('/', 1)
        shardConnection = MongoClient(shardConnParts[1], replicaset=shardConnParts[0])

        for collection in configServerConfigDB.collections.find({'dropped': False}):
            collectionParts = collection['_id'].split('.', 1)
            dbName = collectionParts[0]
            collName = collectionParts[1]
            shardKey = collection['key']

            db = shardConnection.get_database(dbName)

            applyOpsCommand = {
                'applyOps': [{
                    'op': 'c',
                    'ns': dbName + '.$cmd',
                    'o': {
                        'create': collName,
                    },
                }]
            }

            if 'uuid' in collection:
                collUUID = collection['uuid']
                applyOpsCommand[0]['applyOps']['ui'] = collUUID

            config.log_line("db.adminCommand(" + str(applyOpsCommand) + ");")
            db.command(applyOpsCommand, codec_options=CodecOptions(uuid_representation=4))

            createIndexesCommand = {
                'createIndexes': collName,
                'indexes': [{
                    'key': shardKey,
                    'name': 'Shard key index'
                }]
            }
            config.log_line(
                "db.getSiblingDB(" + dbName + ").runCommand(" + str(createIndexesCommand) + ");")
            db.command(createIndexesCommand)

        shardConnection.close()

    # Restart the cluster so it picks up the new configuration cleanly
    config.mlaunch_action('restart', mongodClusterRootPath)

    return 0


if __name__ == "__main__":
    main()
