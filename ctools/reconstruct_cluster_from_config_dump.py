#!/usr/bin/env python3
#

import argparse
import os
import subprocess

from bson.codec_options import CodecOptions
from pymongo import MongoClient


# Class to abstract the tool's command line parameters configuration
class ToolConfiguration:

    # Class initialization. The 'args' parameter contains the parsed tool command line arguments.
    def __init__(self, args):
        self.binarypath = args.binarypath
        self.dir = args.dir
        self.configdump = args.configdumpdir[0]

        self.mongoDBinary = os.path.join(self.binarypath, 'mongod')
        self.mongoRestoreBinary = os.path.join(self.binarypath, 'mongorestore')

        self.clusterIntrospectMongoDPort = 19000
        self.clusterStartingPort = 20000

        self.mongoRestoreNumInsertionWorkers = 16

        print('Running cluster import with binaries located at: ', self.binarypath)
        print('Data directory root at: ', self.dir)
        print('Config dump directory at: ', self.configdump)

    # Invokes mongorestore of the config database dump against an instance running on 'restorePort'.
    def restore_config_db_to_port(self, restorePort):
        subprocess.check_call([
            self.mongoRestoreBinary, '--port',
            str(restorePort), '--numInsertionWorkersPerCollection',
            str(self.mongoRestoreNumInsertionWorkers), self.configdump
        ])

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
        subprocess.check_call(mlaunchCommand)


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


# Performs cleanup by killing all potentially running mongodb processes and deleting any leftover
# files. Basically leaves '--dir' empty.
def cleanup_previous_runs(config):
    killAllMongoDBProcessesCmd = 'killall -9 mongod mongos'
    wipeOutDataPathCmd = 'rm -rf ' + config.dir + '/*'

    if (not yes_no('The next step will kill all mongodb processes and wipe out the data path (' +
                   killAllMongoDBProcessesCmd + '; ' + wipeOutDataPathCmd + ').' + '\n' +
                   'Proceed (yes/no)? ')):
        return False

    subprocess.run(killAllMongoDBProcessesCmd, shell=True, check=False)
    subprocess.run(wipeOutDataPathCmd, shell=True, check=False)
    return True


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

    if (not cleanup_previous_runs(config)):
        return -1

    # Make the output directories
    mongodPreprocessDataPath = os.path.join(config.dir, 'config_preprocess_instance')
    os.makedirs(mongodPreprocessDataPath)

    mongodClusterRootPath = os.path.join(config.dir, 'cluster_root')
    os.makedirs(mongodClusterRootPath)

    # Start the instance through which to read the cluster configuration dump and restore the dump
    print('Pre-processing config dump at port ', config.clusterIntrospectMongoDPort)
    config.mlaunch_action(
        'init', mongodPreprocessDataPath,
        ['--single', '--port', str(config.clusterIntrospectMongoDPort)])
    config.restore_config_db_to_port(config.clusterIntrospectMongoDPort)

    # Read the cluster configuration from the preprocess instance and construct the new cluster
    configDBPreprocess = MongoClient('localhost', config.clusterIntrospectMongoDPort).config
    numShards = configDBPreprocess.shards.count({})
    if (numShards > 10):
        if (not yes_no('The imported configuration data contains large number of shards (' + str(
                numShards) + '). Proceeding will start large number of mongod processes.\n' +
                       'Are you sure you want to continue (yes/no)? ')):
            return 1

    mlaunchStartingPort = 20000
    config.mlaunch_action('init', mongodClusterRootPath, [
        '--replicaset', '--nodes', '1', '--sharded',
        str(numShards), '--csrs', '--mongos', '1', '--port',
        str(mlaunchStartingPort)
    ])

    configServerPort = mlaunchStartingPort + numShards + 1
    config.restore_config_db_to_port(configServerPort)

    configServerConnection = MongoClient('localhost', configServerPort)
    configServerConfigDB = configServerConnection.config

    existingShardIds = []
    newShardIds = []
    shardIdCounter = 1
    for shard in configDBPreprocess.shards.find({}):
        existingShardId = shard['_id']
        existingShardIds.append(existingShardId)

        newShardId = 'shard0' + str(shardIdCounter)
        newShardIds.append(newShardId)

        print("db.databases.update({primary: '" + existingShardId + "'}, {$set: {primary: '" +
              newShardId + "'}}, {multi: true});")
        result = configServerConfigDB.databases.update_many({
            'primary': existingShardId
        }, {'$set': {
            'primary': newShardId
        }})
        print(result.raw_result)

        print("db.chunks.update({shard: '" + existingShardId + "'}, {$set: {shard: '" + newShardId +
              "'}}, {multi: true});")
        result = configServerConfigDB.chunks.update_many({
            'shard': existingShardId
        }, {'$set': {
            'shard': newShardId
        }})
        print(result.raw_result)

        shardIdCounter = shardIdCounter + 1

    print("db.shards.remove({_id: {$not: {$in: ", list(map(str, newShardIds)), "}}});")
    result = configServerConfigDB.shards.delete_many({
        '_id': {
            '$not': {
                '$in': list(map(str, newShardIds))
            }
        }
    })
    print(result.raw_result)

    # Create the collections and construct sharded indexes on all shard nodes
    for shard in configServerConfigDB.shards.find({}):
        print('Creating shard key indexes on shard ' + shard['_id'])

        shardConnParts = shard['host'].split('/', 1)
        shardConnection = MongoClient(shardConnParts[1], replicaset=shardConnParts[0])

        for collection in configServerConfigDB.collections.find({'dropped': False}):
            collectionParts = collection['_id'].split('.', 1)
            dbName = collectionParts[0]
            collName = collectionParts[1]
            collUUID = collection['uuid']
            shardKey = collection['key']

            db = shardConnection.get_database(dbName)

            applyOpsCommand = {
                'applyOps': [{
                    'op': 'c',
                    'ns': dbName + '.$cmd',
                    'o': {
                        'create': collName,
                    },
                    'ui': collUUID,
                }]
            }
            print("db.adminCommand(" + str(applyOpsCommand) + ");")
            db.command(applyOpsCommand, codec_options=CodecOptions(uuid_representation=4))

            createIndexesCommand = {
                'createIndexes': collName,
                'indexes': [{
                    'key': shardKey,
                    'name': 'Shard key index'
                }]
            }
            print("db.getSiblingDB(" + dbName + ").runCommand(" + str(createIndexesCommand) + ");")
            db.command(createIndexesCommand)

        shardConnection.close()

    # Restart the cluster so it picks up the new configuration cleanly
    config.mlaunch_action('restart', mongodClusterRootPath)

    return 0


if __name__ == "__main__":
    main()
