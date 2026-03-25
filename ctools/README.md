# CTools: Set of Python tools for manipulating sharded clusters
The CTools package is a set of command-line tools for manipulating sharded clusters. All the tools are built on a common Python framework and use asyncio in order to achieve parallelism.

Click on the links below or run the respective tool with `--help` for more information on how to use it.

### [defragment_sharded_collection](https://github.com/kaloianm/workscripts/blob/master/ctools/defragment_sharded_collection.py#L3)
### [generate_fragmented_sharded_collection](https://github.com/kaloianm/workscripts/blob/master/ctools/generate_fragmented_sharded_collection.py#L3)
### [launch_ec2_cluster_hosts](https://github.com/kaloianm/workscripts/blob/master/ctools/launch_ec2_cluster_hosts.py#L3)
### [launch_ec2_replicaset_hosts](https://github.com/kaloianm/workscripts/blob/master/ctools/launch_ec2_replicaset_hosts.py#L3)
### [locust_read_write_load](https://github.com/kaloianm/workscripts/blob/master/ctools/locust_read_write_load.py#L3)
### [manually_unshard_collection](https://github.com/kaloianm/workscripts/blob/master/ctools/manually_unshard_collection.py#L3)
### [reconstruct_cluster_from_config_dump](https://github.com/kaloianm/workscripts/blob/master/ctools/reconstruct_cluster_from_config_dump.py#L3)
### [remote_control_cluster](https://github.com/kaloianm/workscripts/blob/master/ctools/remote_control_cluster.py#L3)

### Data generation configs (mgodatagen)

Use the following commands to execute them:
```
mgodatagen -f locust_read_write_load_mgodatagen_10GB.json --uri mongodb://URL/?directConnection=false
mgodatagen -f locust_read_write_load_mgodatagen_50GB.json --uri mongodb://URL/?directConnection=false
mgodatagen -f locust_read_write_load_mgodatagen_500GB.json --uri mongodb://URL/?directConnection=false
mgodatagen -f locust_read_write_load_mgodatagen_4TB.json --uri mongodb://URL/?directConnection=false
```

- **locust_read_write_load_mgodatagen_10GB.json** — Generates a ~10GB dataset (1.36M docs with 3.6KB payload and 10 secondary indexes of 180 bytes each). Approximate breakdown: ~7.5GB data + ~2.5GB indexes. Good for quick local testing.
- **locust_read_write_load_mgodatagen_50GB.json** — Generates a ~50GB dataset (6.8M docs, same document structure). Approximate breakdown: ~37GB data + ~12GB indexes.
- **locust_read_write_load_mgodatagen_500GB.json** — Generates a ~500GB dataset (68M docs, same document structure). Approximate breakdown: ~375GB data + ~125GB indexes. Suitable for M50 instances with 1TB data volume.
- **locust_read_write_load_mgodatagen_4TB.json** — Generates a ~4TB dataset (540M docs, same document structure). Approximate breakdown: ~3TB data + ~1TB indexes (10 x 100GB). Requires instances with 5TB+ data volume.
