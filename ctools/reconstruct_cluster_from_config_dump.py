#!/usr/bin/env python3
#

import argparse
import os
import psutil
import pymongo
import shutil
import socket
import subprocess
import sys

from bson.binary import UuidRepresentation
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
        self.numShards = args.numshards

        self.mongoRestoreBinary = os.path.join(self.binarypath, exe_name('mongorestore'))

        self.clusterIntrospectMongoDPort = 20000
        self.clusterStartingPort = 27017

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
        self._outputLogFile = open(
            os.path.join(args.dir, 'reconstruct.log'), 'w', kOutputLogFileBufSize)

    def log_line(self, entry):
        if (isinstance(entry, pymongo.results.DeleteResult)):
            msg = 'DeleteResult deleted: {}'.format(entry.deleted_count)
        else:
            msg = str(entry)

        self._outputLogFile.write(msg + '\n')

    # Invokes mongorestore of the config database dump against an instance running on 'restorePort'.
    def restore_config_db_to_port(self, restorePort):
        mongorestoreCommand = [
            self.mongoRestoreBinary, '--port',
            str(restorePort), '--numInsertionWorkersPerCollection',
            str(self.mongoRestoreNumInsertionWorkers)
        ]

        if (os.path.isdir(self.configdump)):
            # Discover if there are compressed files in order to decide whether to add the '--gzip'
            # prefix.
            if (os.path.exists(
                    os.path.join(os.path.join(self.configdump, 'config'), 'databases.bson.gz'))):
                self._outputLogFile.write(
                    'Discovered gzipped contents, adding the --gzip option to mongorestore\n')
                mongorestoreCommand += ['--gzip']
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

        mlaunchPrefix = [
            'mlaunch',
            action,
            '--binarypath',
            self.binarypath,
            '--dir',
            dir,
            '--bind_ip_all',
        ]

        if (set(mlaunchPrefix) & set(args)):
            raise ValueError('args duplicates values in mlaunchPrefix')

        mlaunchCommand = mlaunchPrefix + args

        print('Executing mlaunch command: ' + ' '.join(mlaunchCommand))
        subprocess.check_call(mlaunchCommand, stdout=self._outputLogFile)

    # Performs cleanup by killing all potentially running mongodb processes and deleting any
    # leftover files. Basically leaves '--dir' empty.
    def __cleanup_previous_runs(self):
        yes_no('The next step will kill all MongoDB processes and wipe out the data path.')

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
        mongoDb = MongoClient('localhost', config.clusterIntrospectMongoDPort)

        self.configDb = mongoDb.config

        self.numZones = self.configDb.tags.count_documents({})
        self.numShards = self.configDb.shards.count_documents({})
        self.FCV = mongoDb.admin.system.version.find_one(
            {'_id': 'featureCompatibilityVersion'})['version']


# Abstracts the manipulations of the mlaunch-started cluster
class MlaunchCluster:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration and
    # 'introspect' is an instance of ClusterIntrospect.
    def __init__(self, config, introspect):
        self._config = config
        self._introspect = introspect

        if config.numShards is None:
            numShards = introspect.configDb.shards.count_documents({})
        else:
            numShards = config.numShards

        if (numShards > 10):
            yes_no(
                f'The imported configuration data contains {str(numShards)} shards. Proceeding will start a large number of mongod processes.'
            )

        config.mlaunch_action('init', config.clusterRoot, [
            '--sharded',
            str(numShards),
            '--replicaset',
            '--nodes',
            '1',
            '--csrs',
            '--mongos',
            '1',
            '--port',
            str(config.clusterStartingPort),
            '--wiredTigerCacheSizeGB',
            '0.25',
            '--oplogSize',
            '50',
            '--hostname',
            socket.gethostname(),
        ])

        # Set the correct FCV on the cluster being reconstructed
        clusterConnection = MongoClient('localhost', config.clusterStartingPort)
        self._config.log_line(
            clusterConnection.admin.command('setFeatureCompatibilityVersion', introspect.FCV))

        # TODO: Find a better way to determine the port of the config server's primary
        self.configServerPort = config.clusterStartingPort + (numShards + 1)

        configServerConnection = MongoClient('localhost', self.configServerPort)
        self.configDb = configServerConnection.config

    # Renames the shards from the dump to the shards launched by mlaunch (in the shards collection)
    def restoreAndFixUpShardIds(self):
        shardsFromDump = list(self._introspect.configDb.shards.find({}).sort('_id', 1))
        shardsFromMlaunch = list(self.configDb.shards.find({}).sort('_id', 1))

        self._config.restore_config_db_to_port(self.configServerPort)

        print('Renaming shards in the shards collection:')

        self._shardIdRemap = {}

        if len(shardsFromDump) <= len(shardsFromMlaunch):
            for shardFromDump, shardFromMlaunch in zip(deepcopy(shardsFromDump), shardsFromMlaunch):
                self._config.log_line(
                    self.configDb.shards.delete_one({'_id': shardFromDump['_id']}))
                self._config.log_line(
                    self.configDb.shards.delete_one({'_id': shardFromMlaunch['_id']}))

                self._shardIdRemap[shardFromDump['_id']] = shardFromMlaunch['_id']

                shardFromDump['_id'] = shardFromMlaunch['_id']
                shardFromDump['host'] = shardFromMlaunch['host']

                self._config.log_line(self.configDb.shards.insert_one(shardFromDump))

        elif len(shardsFromDump) > len(shardsFromMlaunch):
            # If the dump has more shards than the mlaunch cluster (--numshards was specified with
            # smaller number), assign the dump shards to the mlaunch shards in round-robin fashion
            def roundRobin(arr):
                i = 0
                while True:
                    yield arr[i % len(arr)]
                    i += 1

            for shardFromDump, shardFromMlaunch in zip(shardsFromDump,
                                                       roundRobin(shardsFromMlaunch)):
                self._config.log_line(
                    self.configDb.shards.delete_one({'_id': shardFromDump['_id']}))

                self._shardIdRemap[shardFromDump['_id']] = shardFromMlaunch['_id']

    # Renames the shards from the dump to the shards launched by mlaunch (in the databases and
    # chunks collections)
    def fixUpRoutingMetadata(self):
        print('Renaming shards in the routing metadata:')

        for shardId in self._shardIdRemap:
            shardIdTo = self._shardIdRemap.get(shardId)
            print('Shard', shardId, 'becomes', shardIdTo)

            # Rename the primary shard for all databases
            self._config.log_line(
                self.configDb.databases.update_many({'primary': shardId},
                                                    {'$set': {
                                                        'primary': shardIdTo
                                                    }}))

            # Rename the shards in the chunks' current owner field
            self._config.log_line(
                self.configDb.chunks.update_many({'shard': shardId}, {'$set': {
                    'shard': shardIdTo
                }}))

            # Rename the shards in the chunks' history
            self._config.log_line(
                self.configDb.chunks.update_many({'history.shard': shardId},
                                                 {'$set': {
                                                     'history.$[element].shard': shardIdTo
                                                 }}, array_filters=[{
                                                     'element.shard': shardId
                                                 }]))

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
                self._config.log_line(
                    db.command(
                        applyOpsCommand, codec_options=CodecOptions(
                            uuid_representation=UuidRepresentation.STANDARD)))

                createIndexesCommand = {
                    'createIndexes': collName,
                    'indexes': [{
                        'key': shardKey,
                        'name': 'Shard key index'
                    }]
                }
                self._config.log_line("db.getSiblingDB(" + dbName + ").runCommand(" +
                                      str(createIndexesCommand) + ");")
                self._config.log_line(db.command(createIndexesCommand))

            shardConnection.close()

    def restartCluster(self):
        self._config.mlaunch_action('restart', self._config.clusterRoot)


# Main entrypoint for the application
def main():
    argsParser = argparse.ArgumentParser(
        description=
        'Tool to interpret an export of a cluster config database and construct a new cluster with '
        'exactly the same configuration. Requires mlaunch to be installed and in the system path.')

    # Optional arguments
    argsParser.add_argument(
        '--numshards',
        help='How many shards to create in the constructed cluster. If specified and is less than '
        'the number of the shards in the dump, the extra shards will be assigned in round-robin '
        'fashion on the created cluster. If more than the number of the shards in the dump are '
        'requested, the extra shards will not have any chunks placed on them.', metavar='numshards',
        type=int, required=False)

    # Mandatory arguments
    argsParser.add_argument('--binarypath', help='Directory containing the MongoDB binaries',
                            metavar='binarypath', type=str, required=True)
    argsParser.add_argument(
        '--dir', help='Directory in which to place the data files (will create subdirectories)',
        metavar='dir', type=str, required=True)

    # Positional arguments
    argsParser.add_argument('configdumpdir',
                            help='Directory containing a dump of the cluster config database',
                            metavar='configdumpdir', type=str, nargs=1)

    config = ToolConfiguration(argsParser.parse_args())

    # Read the cluster configuration from the preprocess instance and construct the new cluster
    introspect = ClusterIntrospect(config)

    if (config.numShards is not None and introspect.numZones > 0
            and config.numShards < introspect.numShards):
        raise ValueError('Cannot use `--numshards` with smaller number of shards than those in the '
                         'dump in the case when zones are defined')

    mlaunch = MlaunchCluster(config, introspect)

    mlaunch.restoreAndFixUpShardIds()
    mlaunch.fixUpRoutingMetadata()
    mlaunch.fixUpShards()

    # Restart the cluster so it picks up the new configuration cleanly
    mlaunch.restartCluster()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Command failed due to:', str(e))
        sys.exit(-1)
