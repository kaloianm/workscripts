#!/usr/bin/env python3
#

import argparse
import os
import psutil
import shutil
import subprocess
import sys

from bson.codec_options import CodecOptions
from common import exe_name, yes_no
from copy import deepcopy
from pymongo import MongoClient

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


# Class to abstract the tool's command line parameters configuration
class ToolConfiguration:

    # Class initialization. The 'args' parameter contains the parsed tool command line arguments.
    def __init__(self, args):
        self.binarypath = args.binarypath
        self.introspectRoot = os.path.join(args.dir, 'introspect')
        self.clusterRoot = os.path.join(args.dir, 'cluster')
        self.configdump = args.configdumpdir[0]

        self.mongoRestoreBinary = os.path.join(self.binarypath, exe_name('mongorestore'))

        self.clusterIntrospectMongoDPort = 19000
        self.clusterStartingPort = 20000

        self.mongoRestoreNumInsertionWorkers = 16

        print('Running cluster import with binaries located at:', self.binarypath)
        print('Config dump directory at:', self.configdump)
        print('Data directory root at:', args.dir)
        print('Introspect directory at:', self.introspectRoot)
        print('Cluster directory at:', self.clusterRoot)

        self.__cleanup_previous_runs()

        os.makedirs(self.introspectRoot)
        os.makedirs(self.clusterRoot)

        # Make it unbuffered so the output of the subprocesses shows up immediately in the file
        kOutputLogFileBufSize = 256
        self._outputLogFile = open(os.path.join(args.dir, 'reconstruct.log'), 'w',
                                   kOutputLogFileBufSize)

    def log_line(self, line):
        self._outputLogFile.write(str(line) + '\n')

    # Invokes mongorestore of the config database dump against an instance running on 'restorePort'.
    def restore_config_db_to_port(self, restorePort):
        mongorestoreCommand = [
            self.mongoRestoreBinary, '--port',
            str(restorePort), '--numInsertionWorkersPerCollection',
            str(self.mongoRestoreNumInsertionWorkers)
        ]

        if (os.path.isdir(self.configdump)):
            mongorestoreCommand += ['--dir', self.configdump]
        else:
            mongorestoreCommand += ['--gzip', '--archive={}'.format(self.configdump)]

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
        if (not yes_no(
                'The next step will kill all mongodb processes and wipe out the data path.\n' +
                'Proceed (yes/no)? ')):
            raise KeyboardInterrupt('User disallowed cleanup of the data path')

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

        # Remove the output directories
        try:
            shutil.rmtree(self.introspectRoot)
        except FileNotFoundError:
            pass

        try:
            shutil.rmtree(self.clusterRoot)
        except FileNotFoundError:
            pass


# Abstracts management of the 'introspect' mongod instance which is used to read the cluster
# configuration to instantiate
class ClusterIntrospect:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration.
    def __init__(self, config):
        print('Introspecting config dump using instance at port',
              config.clusterIntrospectMongoDPort)

        # Start the instance and restore the config server dump
        config.mlaunch_action(
            'init', config.introspectRoot,
            ['--single', '--port', str(config.clusterIntrospectMongoDPort)])

        config.restore_config_db_to_port(config.clusterIntrospectMongoDPort)

        # Open a connection to the introspect instance
        self.configDb = MongoClient('localhost', config.clusterIntrospectMongoDPort).config


# Abstracts the manipulations of the mlaunch-started cluster
class MlaunchCluster:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration and
    # 'introspect' is an instance of ClusterIntrospect.
    def __init__(self, config, introspect):
        self._config = config
        self._introspect = introspect

        numShards = introspect.configDb.shards.count({})
        if (numShards > 10):
            if (not yes_no('The imported configuration data contains large number of shards (' +
                           str(numShards) +
                           '). Proceeding will start large number of mongod processes.\n' +
                           'Are you sure you want to continue (yes/no)? ')):
                raise KeyboardInterrupt('Too many shards will be created')

        config.mlaunch_action('init', config.clusterRoot, [
            '--replicaset', '--nodes', '1', '--sharded',
            str(numShards), '--csrs', '--mongos', '1', '--port',
            str(config.clusterStartingPort)
        ])

        # TODO: Find a better way to determine the port of the config server's primary
        self.configServerPort = config.clusterStartingPort + (numShards + 1)

        configServerConnection = MongoClient('localhost', self.configServerPort)
        self.configDb = configServerConnection.config

        self._shardsFromDump = list(introspect.configDb.shards.find({}).sort('_id', 1))
        self._shardsFromMlaunch = list(self.configDb.shards.find({}).sort('_id', 1))

        assert (len(self._shardsFromDump) == len(self._shardsFromMlaunch))

    # Renames the shards from the dump to the shards launched by mlaunch (in the shards collection)
    def restoreAndFixUpShardIds(self):
        self._config.restore_config_db_to_port(self.configServerPort)

        print('Renaming shards in the shards collection by deleting and recreating them:')

        shardsToInsert = []
        for shardFromDump, shardFromMlaunch in zip(deepcopy(self._shardsFromDump),
                                                   self._shardsFromMlaunch):
            shardFromDump['_id'] = shardFromMlaunch['_id']
            shardFromDump['host'] = shardFromMlaunch['host']
            shardsToInsert.append(shardFromDump)

        # Wipe out all the shards (both those that came from the dump and those from the mlaunch
        # cluster, since after the restore they are now jumbled together)
        result = self.configDb.shards.delete_many({})
        self._config.log_line(result.raw_result)

        # Patch the _id and host of the shards in the config dump
        for shardToInsert in shardsToInsert:
            result = self.configDb.shards.insert_one(shardToInsert)
            self._config.log_line(result)

    # Renames the shards from the dump to the shards launched by mlaunch (in the databases and
    # chunks collections)
    def fixUpRoutingMetadata(self):
        print('Renaming shards in the routing metadata:')

        for shardIdFromDump, shardIdFromMlaunch in zip(
                list(map(lambda x: x['_id'], self._shardsFromDump)),
                list(map(lambda x: x['_id'], self._shardsFromMlaunch))):
            print('Shard ' + shardIdFromDump + ' becomes ' + shardIdFromMlaunch)

            result = self.configDb.databases.update_many({'primary': shardIdFromDump},
                                                         {'$set': {
                                                             'primary': shardIdFromMlaunch
                                                         }})
            self._config.log_line(result.raw_result)

            # Rename the shards in the chunks' current owner field
            result = self.configDb.chunks.update_many({'shard': shardIdFromDump},
                                                      {'$set': {
                                                          'shard': shardIdFromMlaunch
                                                      }})
            self._config.log_line(result.raw_result)

            # Rename the shards in the chunks' history
            result = self.configDb.chunks.update_many(
                {'history.shard': shardIdFromDump},
                {'$set': {
                    'history.$[].shard': shardIdFromMlaunch
                }})
            self._config.log_line(result.raw_result)

    # Create the collections and construct sharded indexes on all shard nodes in the mlaunch cluster
    def fixUpShards(self):
        for shard in self.configDb.shards.find({}):
            print('Creating shard key indexes on shard ' + shard['_id'])

            shardConnParts = shard['host'].split('/', 1)
            shardConnection = MongoClient(shardConnParts[1], replicaset=shardConnParts[0])

            for collection in self.configDb.collections.find({'dropped': False}):
                collectionParts = collection['_id'].split('.', 1)
                dbName = collectionParts[0]
                collName = collectionParts[1]
                collUUID = collection['uuid'] if 'uuid' in collection else None

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

                if collUUID:
                    applyOpsCommand['applyOps'][0]['ui'] = collUUID

                self._config.log_line("db.adminCommand(" + str(applyOpsCommand) + ");")
                db.command(applyOpsCommand, codec_options=CodecOptions(uuid_representation=4))

                createIndexesCommand = {
                    'createIndexes': collName,
                    'indexes': [{
                        'key': shardKey,
                        'name': 'Shard key index'
                    }]
                }
                self._config.log_line("db.getSiblingDB(" + dbName + ").runCommand(" +
                                      str(createIndexesCommand) + ");")
                db.command(createIndexesCommand)

            shardConnection.close()

    def restartCluster(self):
        self._config.mlaunch_action('restart', self._config.clusterRoot)


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
    mlaunch = MlaunchCluster(config, introspect)

    mlaunch.restoreAndFixUpShardIds()
    mlaunch.fixUpRoutingMetadata()
    mlaunch.fixUpShards()

    # Restart the cluster so it picks up the new configuration cleanly
    mlaunch.restartCluster()

    return 0


if __name__ == "__main__":
    main()
