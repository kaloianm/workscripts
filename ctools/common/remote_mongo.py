'''Common MongoDB remote host management and cluster operations.'''

import asyncio
import copy
import logging
import motor.motor_asyncio

from common.remote_common import RemoteSSHHost
from signal import Signals


class RemoteMongoHost(RemoteSSHHost):
    '''
    Specialisation of 'RemoteSSHHost' which will be running MongoDB services (mongod/mongos, etc)
    '''

    def __init__(self, host_desc):
        '''
        Constructs a cluster host object from host description, which is a superset of the
        description required by 'RemoteSSHHost' above. The additional fields are:
          RemoteMongoDPath: Path on the host to serve as a root for the MongoD service's data and
            logs. Defaulted to $HOME/mongod_data.
          RemoteMongoSPath: Path on the host to serve as a root for the MongoS service's data and
            logs. Defaulted to $HOME/mongos_data.
        '''

        RemoteSSHHost.__init__(self, host_desc)

        default_mongod_data_path = '$HOME/mongod_data'
        default_mongos_data_path = '$HOME/mongos_data'

        # Populate parameter defaults
        if 'RemoteMongoDPath' not in self.host_desc:
            self.host_desc['RemoteMongoDPath'] = default_mongod_data_path
        if 'RemoteMongoSPath' not in self.host_desc:
            self.host_desc['RemoteMongoSPath'] = default_mongos_data_path

    async def start_mongod_instance(self, port, repl_set_name, extra_args=None):
        '''
        Starts a single MongoD instance on this host, as part of a replica set called 'repl_set_name',
        listening on 'port'. The 'extra_args' is a list of additional command line arguments to
        specify to the mongod command line.
        '''

        if extra_args is None:
            extra_args = []

        await self.exec_remote_ssh_command(
            (f'mkdir -p {self.host_desc["RemoteMongoDPath"]} && '
             f'$HOME/binaries/mongod --replSet {repl_set_name} '
             f'--dbpath {self.host_desc["RemoteMongoDPath"]} '
             f'--logpath {self.host_desc["RemoteMongoDPath"]}/mongod.log '
             f'--port {port} '
             f'--bind_ip_all '
             f'{" ".join(extra_args)} '
             f'--fork '))

    async def start_mongos_instance(self, port, config_hosts, extra_args=None):
        '''
        Starts a single MongoS instance on this host, listening on 'port', pointing to
        'config_hosts'. The 'extra_args' is a list of additional command line arguments to specify
        to the mongos command line.
        '''

        if extra_args is None:
            extra_args = []

        config_host_list = ','.join(f'{h.host}:27019' for h in config_hosts)
        await self.exec_remote_ssh_command(
            (f'mkdir -p {self.host_desc["RemoteMongoSPath"]} && '
             f'$HOME/binaries/mongos --configdb config/{config_host_list} '
             f'--logpath {self.host_desc["RemoteMongoSPath"]}/mongos.log '
             f'--port {port} '
             f'--bind_ip_all '
             f'{" ".join(extra_args)} '
             f'--fork '))


def make_remote_mongo_host(host_info, config, role):
    '''Constructs a RemoteMongoHost from host info, applying global config defaults'''

    if isinstance(host_info, str):
        host_info = {'host': host_info}
    else:
        host_info = copy.deepcopy(host_info)

    if 'RemoteMongoDPath' in config and 'RemoteMongoDPath' not in host_info:
        host_info['RemoteMongoDPath'] = config['RemoteMongoDPath']
    if 'RemoteMongoSPath' in config and 'RemoteMongoSPath' not in host_info:
        host_info['RemoteMongoSPath'] = config['RemoteMongoSPath']

    host_info['shard'] = role

    return RemoteMongoHost(host_info)


async def stop_mongo_processes(hosts, signal, shard=None):
    '''Stops processes that might have been left over from a previous run'''

    logging.info(f"Stopping mongo processes for {shard if shard else 'all hosts'}")
    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue

        tasks.append(
            asyncio.create_task(
                host.exec_remote_ssh_command(
                    (f'killall --wait -s {signal.name} mongo mongod mongos || true'))))
    await asyncio.gather(*tasks)


async def cleanup_mongo_directories(hosts, shard=None):
    '''Cleanup directories that might have been left over from a previous run'''

    logging.info(f"Cleaning leftover directories for {shard if shard else 'all hosts'}")
    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue

        tasks.append(
            asyncio.create_task(
                host.exec_remote_ssh_command((f'rm -rf {host.host_desc["RemoteMongoDPath"]} ;'
                                              f'rm -rf {host.host_desc["RemoteMongoSPath"]}'))))
    await asyncio.gather(*tasks)


async def install_prerequisite_packages(hosts):
    '''Install (using apt) all the prerequisite libraries that the mongodb binaries require'''

    logging.info('Installing prerequisite packages')
    tasks = []
    for host in hosts:
        tasks.append(
            asyncio.create_task(
                host.exec_remote_ssh_command(
                    'sudo apt -y update && sudo apt -y install libsnmp-dev')))
    await asyncio.gather(*tasks)


async def start_mongod_as_replica_set(hosts, port, repl_set_name, extra_args):
    '''
    Starts the mongod processes on the specified 'hosts', where each host will be listening on
    'port' and will be initiated as part of 'repl_set_name'.
    '''

    tasks = []
    for host in hosts:
        tasks.append(
            asyncio.create_task(host.start_mongod_instance(port, repl_set_name, extra_args)))
    await asyncio.gather(*tasks)


async def initiate_replica_set(hosts, port, repl_set_name):
    '''
    Initiates 'hosts' as a replica set with a name of 'repl_set_name' and 'hosts':'port' as members
    '''

    connection_string = f'mongodb://{hosts[0].host}:{port}'
    replica_set_members = list(
        map(
            lambda id_and_host: {
                '_id': id_and_host[0],
                'host': f'{id_and_host[1].host}:{port}',
                'priority': 2 if id_and_host[0] == 0 else 1,
            }, zip(range(0, len(hosts)), hosts)))

    logging.info(
        f'Connecting to {connection_string} in order to initiate it as {repl_set_name} with members of {replica_set_members}'
    )

    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(connection_string, directConnection=True)
    logging.info(await mongo_client.admin.command({
        'replSetInitiate': {
            '_id': repl_set_name,
            'members': replica_set_members,
        },
    }))


async def rsync_to_hosts(hosts, local_pattern, remote_path, shard=None):
    '''Rsyncs all the files that match 'local_pattern' to the 'remote_path' on specified hosts'''

    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue
        tasks.append(asyncio.create_task(host.rsync_files_to_remote(local_pattern, remote_path)))
    await asyncio.gather(*tasks)


async def deploy_binaries(hosts, mongo_bin_path, shard=None):
    '''Deploys MongoDB binaries to the specified hosts'''

    await rsync_to_hosts(hosts, f'{mongo_bin_path}/mongo*', '$HOME/binaries', shard)


async def gather_logs(hosts, cluster_name, local_path, shard=None):
    '''Compresses and rsyncs logs from the hosts to a local directory'''

    def make_host_suffix(host, process_name):
        return f'{process_name}-{host.host_desc["shard"]}-{host.host}'

    # Compress MongoD logs and FTDC
    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue

        tasks.append(
            asyncio.create_task(
                host.exec_remote_ssh_command((
                    f'tar zcvf {host.host_desc["RemoteMongoDPath"]}/{make_host_suffix(host, "mongod")}.tar.gz '
                    f'{host.host_desc["RemoteMongoDPath"]}/mongod.log* '
                    f'{host.host_desc["RemoteMongoDPath"]}/diagnostic.data'))))
    await asyncio.gather(*tasks)

    # Compress MongoS logs and FTDC
    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue

        tasks.append(
            asyncio.create_task(
                host.exec_remote_ssh_command((
                    f'tar zcvf {host.host_desc["RemoteMongoSPath"]}/{make_host_suffix(host, "mongos")}.tar.gz '
                    f'{host.host_desc["RemoteMongoSPath"]}/mongos.log* '
                    f'{host.host_desc["RemoteMongoSPath"]}/mongos.diagnostic.data'))))
    await asyncio.gather(*tasks)

    # Rsync files locally (do not rsync more than 3 at a time)
    sem_max_concurrent_rsync = asyncio.Semaphore(3)

    async def rsync_with_semaphore(host):
        async with sem_max_concurrent_rsync:
            await host.rsync_files_to_local(
                f'{host.host_desc["RemoteMongoDPath"]}/{make_host_suffix(host, "mongod")}.tar.gz',
                f'{local_path}/{cluster_name}/')
            await host.rsync_files_to_local(
                f'{host.host_desc["RemoteMongoSPath"]}/{make_host_suffix(host, "mongos")}.tar.gz',
                f'{local_path}/{cluster_name}/')

    tasks = []
    for host in hosts:
        if shard and host.host_desc['shard'] != shard:
            continue
        tasks.append(asyncio.create_task(rsync_with_semaphore(host)))
    await asyncio.gather(*tasks)
