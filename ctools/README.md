# CTools: Set of Python tools for manipulating sharded clusters
The CTools package is a set of command-line tools for manipulating sharded clusters. All the tools
are built on a common framework.

## [launch_ec2_cluster_hosts.py](https://github.com/kaloianm/workscripts/blob/master/ctools/launch_ec2_cluster_hosts.py#L3)
This is a tool to launch a set of EC2 hosts for creating a new cluster.

See the help for more commands.

## [remote_control_cluster.py](https://github.com/kaloianm/workscripts/blob/master/ctools/remote_control_cluster.py#L3)
This is a tool to create and manipulate a MongoDB sharded cluster given SSH and MongoDB port access
to a set of hosts. The intended usage is:

1. Use the `launch_ec2_cluster_hosts.py launch` script in order to spawn a set of hosts in EC2
which will be used to run the MongoDB processes on.
1. Produce a cluster description cluster.json file with the following format:
```
        {
                "Name": "Test Cluster",
                "Hosts": [
                    # The output from step (1)
                ],
                "DriverHosts": [
                    # The output from step (1)
                ],
                "MongoBinPath": "<Path where the MongoDB binaries are stored>",
                "RemoteMongoDPath": "<Path on the remote machine where the MongoD data/log files will be placed>",
                "RemoteMongoSPath": "<Path on the remote machine where the MongoD data/log files will be placed>",
                "FeatureFlags": [ "<List of strings with feature flag names to enable>" ]
        }
```
1. `remote_control_cluster.py create cluster.json`

See the help for more commands.

## [deframent_sharded_collection.py](https://github.com/kaloianm/workscripts/blob/master/ctools/deframent_sharded_collection.py#L3)
This is a tool to defragment a sharded cluster in a way which minimises the rate at which the major
shard version gets bumped in order to minimise the amount of stalls due to refresh.

See the help for more commands.

## [reconstruct_cluster_from_config_dump.py](https://github.com/kaloianm/workscripts/blob/master/ctools/reconstruct_cluster_from_config_dump.py#L3)
This is a tool to reconstruct a locally-spawned cluster of single-shard nodes (using mtools) which
matches the configuration of a provided config server dump and restore all the sharded collections
as empty sharded collection with the same configuraiton.

See the help for more commands.

## [manually_unshard_collection.py](https://github.com/kaloianm/workscripts/blob/master/ctools/manually_unshard_collection.py#L3)
This is a tool to unshard a collection which is already on a single shard, without downtime.

See the help for more commands.
