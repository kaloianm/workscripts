#!/usr/bin/env python3
#

import argparse
import os
import psutil
import pymongo
import shutil
import socket
import logging
import subprocess
import sys
import random

from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from common import exe_name, yes_no
from copy import deepcopy
from pymongo import MongoClient

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")

# Global script configuration
mongorestore_num_insertion_workers_per_collection = 16


# Read-only class which abstracts the tool's command line parameters configuration.
class ToolConfiguration:

    # Class initialization. The 'args' parameter contains the parsed tool command line arguments.
    def __init__(self, args):
        self.binarypath = args.binarypath
        self.toolbinarypath = args.toolbinarypath
        self.mongoRestoreBinary = os.path.join(self.toolbinarypath, exe_name('mongorestore'))
        self.root = args.dir
        self.introspectRoot = os.path.join(self.root, 'introspect')
        self.clusterRoot = os.path.join(self.root, 'cluster')
        self.configdump = args.configdumpdir[0]
        self.numShards = args.numshards
        self.genData = args.gen_data

        self.clusterIntrospectMongoDPort = 20000
        self.clusterStartingPort = 27017

        logging.info(f'Running cluster import with binaries located at: {self.binarypath}')
        logging.info(f'Tool binaries at: {self.toolbinarypath}')
        logging.info(f'Config dump directory at: {self.configdump}')
        logging.info(f'Reconstruction root at: {self.root}')
        logging.info(f'Introspect directory at: {self.introspectRoot}')
        logging.info(f'Cluster directory at: {self.clusterRoot}')


# Class which abstracts the invocations of external tools on which this script depends, such as
# mlaunch or mongorestore.
class ExternalProcessManager:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration.
    def __init__(self, config):
        self._config = config

        # Make it unbuffered so the output of the subprocesses shows up immediately in the file
        kOutputLogFileBufSize = 256
        self._outputLogFile = open(
            os.path.join(self._config.root, 'reconstruct.log'), 'w', kOutputLogFileBufSize)

    # Invokes mongorestore of the config database dump against the instance running on
    # 'instance_port'
    def mongorestore_config_db_to_port(self, instance_port):
        # yapf: disable
        mongorestore_command = [
            self._config.mongoRestoreBinary,
            '--port', str(instance_port),
            '--numInsertionWorkersPerCollection', str(mongorestore_num_insertion_workers_per_collection)
        ]
        # yapf: enable

        if (os.path.isdir(self._config.configdump)):
            # Discover if there are compressed files in order in order to decide whether to add the
            # '--gzip' prefix to mongorestore
            if (os.path.exists(
                    os.path.join(
                        os.path.join(self._config.configdump, 'config'), 'databases.bson.gz'))):
                logging.info(
                    'Discovered gzipped contents, adding the --gzip option to mongorestore')
                mongorestore_command += ['--gzip']
            mongorestore_command += ['--dir', self._config.configdump]
        else:
            mongorestore_command += ['--gzip', f'--archive={self._config.configdump}']

        logging.info(f'Executing mongorestore command: {" ".join(mongorestore_command)}')
        subprocess.check_call(mongorestore_command, stdout=self._outputLogFile,
                              stderr=self._outputLogFile)

    # Invokes the specified 'action' of mlaunch with 'dir' as the environment directory. All the
    # remaining 'args' are just appended to the thus constructed command line.
    def mlaunch(self, action, dir, args=[]):
        if (not isinstance(args, list)):
            raise TypeError('args must be of type list.')

        # yapf: disable
        mlaunch_command = [
            'mlaunch', action,
            '--binarypath', self._config.binarypath,
            '--dir', dir,
            '--bind_ip_all',
        ]
        # yapf: enable

        if (set(mlaunch_command) & set(args)):
            raise ValueError('args duplicates values in mlaunchCommand')

        mlaunch_command += args

        logging.info(f'Executing mlaunch command: {" ".join(mlaunch_command)}')
        subprocess.check_call(mlaunch_command, stdout=self._outputLogFile,
                              stderr=self._outputLogFile)


# Class which abstracts interations with the introspect mongod instance which is used to read the
# cluster configuration, based on which the final cluster will be instantiated.
class ClusterIntrospect:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration.
    def __init__(self, config, external_process):
        logging.info(
            f'Introspecting config dump using instance at port {config.clusterIntrospectMongoDPort}'
        )

        self._config = config
        self._external_process = external_process

    def restore(self):
        # Start the introspect instance and restore the config server dump
        # yapf: disable
        self._external_process.mlaunch('init', self._config.introspectRoot, [
            '--single',
            '--port', str(self._config.clusterIntrospectMongoDPort),
        ])
        # yapf: disable

        self._external_process.mongorestore_config_db_to_port(self._config.clusterIntrospectMongoDPort)

        # Open a connection to the introspect instance
        mongoDb = MongoClient('localhost', self._config.clusterIntrospectMongoDPort)

        self.configDb = mongoDb.config

        self.num_shards = self.configDb.shards.count_documents({})
        self.num_zones = self.configDb.tags.count_documents({})

        self.FCV = mongoDb.admin.system.version.find_one(
            {'_id': 'featureCompatibilityVersion'})['version']


# Abstracts the manipulations of the mlaunch-started cluster
class MlaunchCluster:

    # Class initialization. The 'config' parameter is an instance of ToolConfiguration and
    # 'introspect' is an instance of ClusterIntrospect.
    def __init__(self, config, introspect, external_process):
        self._config = config
        self._introspect = introspect
        self._external_process = external_process
        self._num_shards_to_start = self._introspect.num_shards if self._config.numShards is None else self._config.numShards

    # Uses mlaunch to start a cluster which matches that of the config dump.
    def start_and_restore_destination_cluster(self):
        if (self._num_shards_to_start > 10):
            yes_no(
                f'The imported configuration data contains {str(self._num_shards_to_start)} shards. Proceeding will start a large number of mongod processes.'
            )

        # yapf: disable
        self._external_process.mlaunch('init', self._config.clusterRoot, [
            '--sharded', str(self._num_shards_to_start),
            '--port', str(self._config.clusterStartingPort),
            '--replicaset', '--nodes', '1',
            '--csrs',
            '--mongos', '1',
            '--wiredTigerCacheSizeGB', '0.25',
            '--oplogSize', '40',
            '--hostname', socket.gethostname(),
        ])
        # yapf: enable

        # Set the FCV on the cluster being reconstructed to match that of the dump
        cluster_connection = MongoClient('localhost', config.clusterStartingPort)
        cluster_connection.admin.command('setFeatureCompatibilityVersion', introspect.FCV)

        # TODO: Find a better way to determine the port of the config server's primary
        self.configServerPort = config.clusterStartingPort + (self._num_shards_to_start + 1)
        config_server_connection = MongoClient('localhost', self.configServerPort)
        self.configDb = config_server_connection.config

        # Snapshot the shards from mlaunch before running restore
        self._shards_from_mlaunch_snapshot = list(self.configDb.shards.find({}).sort('_id', 1))
        logging.info(f'Original shards:\n{str(self._shards_from_mlaunch_snapshot)}')
        self.configDb.shards.delete_many({})

        self._external_process.mongorestore_config_db_to_port(self.configServerPort)

    # Renames the shards from the dump to the shards launched by mlaunch (in the shards collection)
    def fixup_shard_ids(self):
        logging.info('Renaming shard ids in the config.shards collection:')

        shards_from_dump = list(self._introspect.configDb.shards.find({}).sort('_id', 1))

        self._shardid_remap = {}

        if len(shards_from_dump) <= len(self._shards_from_mlaunch_snapshot):
            for from_shard, to_shard in zip(
                    deepcopy(shards_from_dump), deepcopy(self._shards_from_mlaunch_snapshot)):
                self.configDb.shards.delete_one({'_id': from_shard['_id']})

                self._shardid_remap[from_shard['_id']] = to_shard['_id']
                from_shard['_id'] = to_shard['_id']
                from_shard['host'] = to_shard['host']

                self.configDb.shards.insert_one(from_shard)

        elif len(shards_from_dump) > len(self._shards_from_mlaunch_snapshot):
            # If the dump has more shards than the mlaunch cluster (--numshards was specified with
            # smaller number), assign the dump shards to the mlaunch shards in round-robin fashion
            def round_robin(arr):
                i = 0
                while True:
                    yield arr[i % len(arr)]
                    i += 1

            for from_shard, to_shard in zip(shards_from_dump,
                                            round_robin(self._shards_from_mlaunch_snapshot)):
                self.configDb.shards.delete_one({'_id': from_shard['_id']})

                self._shardid_remap[from_shard['_id']] = to_shard['_id']

        logging.info(f'Mappings after rename:\n{str(self._shardid_remap)}')

    # Renames the shards from the dump to the shards launched by mlaunch (in the databases and
    # chunks collections)
    def fixup_routing_table(self):
        logging.info(
            'Renaming shard ids in the routing table (config.databases, config.collections, config.chunks):'
        )

        for shardId in self._shardid_remap:
            shardIdTo = self._shardid_remap.get(shardId)

            # Rename the primary shard for all databases
            self.configDb.databases.update_many({'primary': shardId},
                                                {'$set': {
                                                    'primary': shardIdTo
                                                }})

            # Rename the shards in the chunks' current owner field
            self.configDb.chunks.update_many({'shard': shardId}, {'$set': {'shard': shardIdTo}})

            # Rename the shards in the chunks' history
            self.configDb.chunks.update_many({'history.shard': shardId},
                                             {'$set': {
                                                 'history.$[element].shard': shardIdTo
                                             }}, array_filters=[{
                                                 'element.shard': shardId
                                             }])

    # Create the collections and construct sharded indexes on all shard nodes in the mlaunch cluster
    def fixup_shard_instances(self):
        for shard in self.configDb.shards.find({}):
            logging.info(f"Creating shard key indexes on shard {shard['_id']}")

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

                db.command(
                    applyOpsCommand,
                    codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD))

                createIndexesCommand = {
                    'createIndexes': collName,
                    'indexes': [{
                        'key': shardKey,
                        'name': 'Shard key index'
                    }]
                }
                db.command(createIndexesCommand)

            shardConnection.close()

    def restart(self):
        self._external_process.mlaunch('restart', self._config.clusterRoot)

    def generate_data(self):
        if not self._config.genData:
            return

        conn = MongoClient('localhost', self._config.clusterStartingPort)
        for collection in self.configDb.collections.find({'dropped': False}):
            if 'key' not in collection:
                continue

            collectionParts = collection['_id'].split('.', 1)
            dbName = collectionParts[0]
            collName = collectionParts[1]
            coll = conn[dbName][collName]

            if dbName == 'config':
                continue

            def gen_doc(name, sub, res_dict):
                if isinstance(sub, dict):
                    for k in sub:
                        sub_res = {}
                        gen_doc(k, sub[k], sub_res)
                        res_dict[name] = sub_res
                else:
                    res_dict[name] = int(random.uniform(-1024 * 1024, 128 * 1024 * 1024))

            logging.info(f"Generating data for collection {collection['_id']}")

            batch = []
            for x in range(1024 * 1024):
                doc = {}
                for k in collection['key']:
                    gen_doc(k, collection['key'][k], doc)
                batch.append(doc)

                if x % (32 * 1024) == 0:
                    coll.insert_many(batch)
                    batch = []
                    logging.info(f'Generated {x} documents')


# Performs cleanup by killing all potentially running mongodb processes and deleting any leftover
# files. The end result is that '--dir' will be empty.
def create_empty_work_directories(config):
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
        shutil.rmtree(config.introspectRoot)
    except FileNotFoundError:
        pass

    try:
        shutil.rmtree(config.clusterRoot)
    except FileNotFoundError:
        pass

    os.makedirs(config.introspectRoot)
    os.makedirs(config.clusterRoot)


if __name__ == "__main__":
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
        '--toolbinarypath',
        help='''Directory containing the MongoDB tools binaries (mongorestore, etc)''',
        metavar='toolbinarypath', type=str, required=True)
    argsParser.add_argument(
        '--dir', help='''Directory in which to place the data files (will create subdirectories)''',
        metavar='dir', type=str, required=True)
    argsParser.add_argument(
        '--gen_data',
        help='''Generate random data in the collection after reconstructing the cluster''',
        action='store_true')

    # Positional arguments
    argsParser.add_argument('configdumpdir',
                            help='Directory containing a dump of the cluster config database',
                            metavar='configdumpdir', type=str, nargs=1)

    args = argsParser.parse_args()
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

    # Prepare the configuration and the workspace where the instances will be started
    config = ToolConfiguration(args)
    create_empty_work_directories(config)

    external_process = ExternalProcessManager(config)

    # Read the cluster configuration from the preprocess instance and construct the new cluster
    introspect = ClusterIntrospect(config, external_process)
    introspect.restore()

    logging.info(
        f'Cluster contains {introspect.num_shards} shards and is running at FCV {introspect.FCV}')

    if (config.numShards is not None and introspect.num_zones > 0
            and config.numShards < introspect.num_shards):
        raise ValueError('Cannot use `--numshards` with smaller number of shards than those in the '
                         'dump in the case when zones are defined')

    cluster = MlaunchCluster(config, introspect, external_process)
    cluster.start_and_restore_destination_cluster()
    cluster.fixup_shard_ids()
    cluster.fixup_routing_table()
    cluster.fixup_shard_instances()

    # Restart the cluster so it picks up the new configuration cleanly
    cluster.restart()

    cluster.generate_data()
