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
mgodatagen -f locust_read_write_load_mgodatagen_1TB.json --uri mongodb://URL/?directConnection=false
mgodatagen -f locust_read_write_load_mgodatagen_4TB.json --uri mongodb://URL/?directConnection=false
```

All configs share the same document structure: a 2,700-byte non-indexed `payload` field, ten 120-byte indexed string fields (`idx0`–`idx9`), plus a unique-indexed `shardKey` integer and `_id`. This yields approximately **4KB per document** (~4,060 bytes raw). The storage split is roughly **77% data / 23% indexes** (see [Document size breakdown](#document-size-breakdown) below).

- **locust_read_write_load_mgodatagen_10GB.json** — Generates a ~10GB dataset (2M docs). Approximate breakdown: ~8GB data + ~2.4GB indexes. Good for quick local testing.
- **locust_read_write_load_mgodatagen_50GB.json** — Generates a ~50GB dataset (10M docs). Approximate breakdown: ~40GB data + ~12GB indexes.
- **locust_read_write_load_mgodatagen_500GB.json** — Generates a ~500GB dataset (100M docs). Approximate breakdown: ~406GB data + ~120GB indexes. Suitable for M50 instances with 1TB data volume.
- **locust_read_write_load_mgodatagen_1TB.json** — Generates a ~1TB dataset (200M docs). Approximate breakdown: ~812GB data + ~240GB indexes. Requires instances with 1.5TB+ data volume.
- **locust_read_write_load_mgodatagen_4TB.json** — Generates a ~4TB dataset (800M docs). Approximate breakdown: ~3.25TB data + ~960GB indexes. Requires instances with 5TB+ data volume.

#### Document size breakdown

Every document has the following layout (all string fields contain random ASCII characters):

| Field | Type | Size | Indexed? |
|---|---|---|---|
| `_id` | ObjectId | 12 bytes | Yes (always) |
| `shardKey` | int32 | 4 bytes | Yes (unique) |
| `idx0`–`idx9` | string × 10 | 120 bytes each → **1,200 bytes total** | Yes (10 secondary indexes) |
| `payload` | string | **2,700 bytes** | No |
| BSON framing / field names | — | ~144 bytes | — |
| **Total** | | **~4,060 bytes ≈ 4KB** | |

**Per-document storage contribution:**
- Non-indexed data (`payload` + overhead): ~2,844 bytes ≈ **70% of the document**
- Indexed fields (`idx0`–`idx9`): 1,200 bytes ≈ **30% of the document**

**Collection vs. index storage ratio:**  
Each of the 10 secondary indexes stores a copy of its 120-byte key per document, so index storage ≈ `10 × 120 × N` bytes. For a collection of N documents:
- Collection (data): `N × 4,060 bytes` ≈ **77%** of total on-disk footprint
- Indexes (all 10 secondary + shardKey): `N × 1,200 bytes` ≈ **23%** of total on-disk footprint
