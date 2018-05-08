#!/usr/bin/env python3
#

import argparse
import subprocess

from pymongo import MongoClient


def main():
    argsParser = argparse.ArgumentParser(
        description=
        'Tool to interpet a cluster config database and construct a cluster with exactly the same configuration'
    )
    argsParser.add_argument(
        '--binarypath',
        help='Directory containing the MongoDB binaries',
        metavar='binarypath',
        type=str,
        required=True)
    argsParser.add_argument(
        '--datapath',
        help=
        'Directory where to place the data files (will create subdirectories)',
        metavar='datapath',
        type=str,
        required=True)
    argsParser.add_argument(
        'configdumpdir',
        help='Config dump file directory',
        metavar='configdumpdir',
        type=str,
        nargs='+')

    args = argsParser.parse_args()
    print('Running cluster import with binaries located at: ', args.binarypath)
    print('Data directory root at: ', args.datapath)
    print('Config dump directory at: ', args.configdumpdir[0])

    subprocess.run('killall -9 mongod mongos', shell=True, check=False)
    subprocess.run('rm -rf ' + args.datapath + '/*', shell=True, check=False)

    mongoDBinary = args.binarypath + '/mongod'
    mongoRestoreBinary = args.binarypath + '/mongorestore'

    mongoDStartingPort = 19000

    mongodPreprocessPort = mongoDStartingPort
    print('Pre-processing config dump at port ', mongodPreprocessPort)
    subprocess.check_call([
        mongoDBinary, '--dbpath', args.datapath, '--logpath',
        args.datapath + '/mongod.log', '--port',
        str(mongodPreprocessPort), '--fork'
    ])
    subprocess.check_call([
        mongoRestoreBinary, '--port',
        str(mongodPreprocessPort), '--numInsertionWorkersPerCollection', '32',
        args.configdumpdir[0]
    ])

    configDBPreprocess = MongoClient('localhost', mongodPreprocessPort).config
    numShards = configDBPreprocess.shards.count({})

    mlaunchStartingPort = 20000
    mlaunchCommandLine = [
        'mlaunch init --replicaset --nodes 1 ', '--sharded',
        str(numShards), '--csrs --mongos 1 --port ',
        str(mlaunchStartingPort), '--binarypath', args.binarypath, '--dir',
        args.datapath + '/cluster'
    ]
    print('mlaunch command line:', ' '.join(mlaunchCommandLine))
    subprocess.run(' '.join(mlaunchCommandLine), shell=True, check=True)

    configServerPort = mlaunchStartingPort + numShards + 1
    subprocess.check_call([
        mongoRestoreBinary, '--port',
        str(configServerPort), '--numInsertionWorkersPerCollection', '32',
        args.configdumpdir[0]
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

        print("db.databases.update({primary: '" + existingShardId + "'}, {$set: {primary: '" + newShardId + "'}}, {multi: true});")
        result = configServerConfigDB.databases.update_many({'primary': existingShardId}, {'$set': {'primary': newShardId}})
        print(result.modified_count)

        print("db.chunks.update({shard: '" + existingShardId + "'}, {$set: {shard: '" + newShardId + "'}}, {multi: true});")
        result = configServerConfigDB.chunks.update_many({'shard': existingShardId}, {'$set': {'shard': newShardId}})
        print(result.modified_count)

        shardIdCounter = shardIdCounter + 1

    print("db.shards.remove({_id: {$not: {$in: ", list(map(str, newShardIds)), "}}});")
    result = configServerConfigDB.shards.delete_many({'_id': {'$not': {'$in': list(map(str, newShardIds))}}})
    print(result.deleted_count)

    mlaunchRestartCommandLine = ['mlaunch restart --dir', args.datapath + '/cluster']
    print('mlaunch restart command line:', ' '.join(mlaunchRestartCommandLine))
    subprocess.run(' '.join(mlaunchRestartCommandLine), shell=True, check=True)

if __name__ == "__main__":
    main()
