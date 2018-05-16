#!/usr/bin/env python3
#

import argparse
import os
import subprocess

from pymongo import MongoClient


# Function for a Yes/No result based on the answer provided as an arguement
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


# Main entrypoint for the application
def main():
    argsParser = argparse.ArgumentParser(
        description=
        'Tool to interpret an export of a cluster config database and construct a new cluster with'
        'exactly the same configuration. Requires mlaunch to be installed and in the system path.')
    argsParser.add_argument('--binarypath', help='Directory containing the MongoDB binaries',
                            metavar='binarypath', type=str, required=True)
    argsParser.add_argument(
        '--datapath',
        help='Directory in which to place the data files (will create subdirectories)',
        metavar='datapath', type=str, required=True)
    argsParser.add_argument('configdumpdir',
                            help='Directory containing a dump of the cluster config database',
                            metavar='configdumpdir', type=str, nargs=1)

    args = argsParser.parse_args()

    print('Running cluster import with binaries located at: ', args.binarypath)
    print('Data directory root at: ', args.datapath)
    print('Config dump directory at: ', args.configdumpdir[0])

    # Perform cleanup by killing all potentially running mongodb processes
    killAllMongoDBProcessesCmd = 'killall -9 mongod mongos'
    wipeOutDataPathCmd = 'rm -rf ' + args.datapath + '/*'
    if (not yes_no('The next step will kill all mongodb processes and wipe out the data path (' +
                   killAllMongoDBProcessesCmd + '; ' + wipeOutDataPathCmd + ').' + '\n' +
                   'Proceed (yes/no)? ')):
        return 1

    subprocess.run(killAllMongoDBProcessesCmd, shell=True, check=False)
    subprocess.run(wipeOutDataPathCmd, shell=True, check=False)

    # Script constants configuration
    mongoDBinary = os.path.join(args.binarypath, 'mongod')
    mongoRestoreBinary = os.path.join(args.binarypath, 'mongorestore')

    mongodPreprocessDataPath = os.path.join(args.datapath, 'config_preprocess_instance')
    mongodClusterRootPath = os.path.join(args.datapath, 'cluster_root')

    mongoDStartingPort = 19000
    mongoRestoreNumInsertionWorkers = 16

    # Start the instance through which to read the cluster configuration dump and restore the dump
    mongodPreprocessPort = mongoDStartingPort
    print('Pre-processing config dump at port ', mongodPreprocessPort)
    os.makedirs(mongodPreprocessDataPath)
    subprocess.check_call([
        mongoDBinary, '--dbpath', mongodPreprocessDataPath, '--logpath',
        os.path.join(mongodPreprocessDataPath + '/mongod.log'), '--port',
        str(mongodPreprocessPort), '--fork'
    ])
    subprocess.check_call([
        mongoRestoreBinary, '--port',
        str(mongodPreprocessPort), '--numInsertionWorkersPerCollection',
        str(mongoRestoreNumInsertionWorkers), args.configdumpdir[0]
    ])

    # Read the cluster configuration from the preprocess instance and construct the new cluster
    configDBPreprocess = MongoClient('localhost', mongodPreprocessPort).config
    numShards = configDBPreprocess.shards.count({})
    if (numShards > 10):
        if (not yes_no('The imported configuration data contains large number of shards (' + str(
                numShards) + '). Proceeding will start large number of mongod processes.\n' +
                       'Are you sure you want to continue (yes/no)? ')):
            return 1

    mlaunchStartingPort = 20000
    mlaunchCommandLine = [
        'mlaunch', 'init', '--replicaset', '--nodes', '1', '--sharded',
        str(numShards), '--csrs', '--mongos', '1', '--port',
        str(mlaunchStartingPort), '--binarypath', args.binarypath, '--dir', mongodClusterRootPath
    ]
    print('Starting cluster using mlaunch command line:', ' '.join(mlaunchCommandLine))
    os.makedirs(mongodClusterRootPath)
    subprocess.check_call(mlaunchCommandLine)

    configServerPort = mlaunchStartingPort + numShards + 1
    subprocess.check_call([
        mongoRestoreBinary, '--port',
        str(configServerPort), '--numInsertionWorkersPerCollection',
        str(mongoRestoreNumInsertionWorkers), args.configdumpdir[0]
    ])

    configServerConfigDB = MongoClient('localhost', configServerPort).config

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

    # TODO: Construct the sharded indexes on all shard nodes

    # Restart the cluster so it picks up the new configuration cleanly
    mlaunchRestartCommandLine = ['mlaunch', 'restart', '--dir', mongodClusterRootPath]
    print('Restarting cluster using mlaunch command line:', ' '.join(mlaunchRestartCommandLine))
    subprocess.check_call(mlaunchRestartCommandLine)

    return 0


if __name__ == "__main__":
    main()
