# Redis Benchmark Results

I have been exploring both DragonflyDB and Redis-Stack as options for in-memory datastores.

In this exploration I decided to run a few benchmarks in Go using the `github.com/redis/go-redis/v9` client.

## Local Tests (Docker NAT)

All tests were performed on localhost behind Docker NAT, on a `AMD EPYC 7302P 16-Core Processor` inside a Proxmox VM with 24GB of allocated RAM available.

| Test         | Inserts | Value Size | Reads | Pipeline Size | redis-stack | dragonflydb |
|--------------|---------|------------|-------|---------------|-------------|-------------|
| No-Pipeline  | 100k    | 5b         | 5m    | N/A           | 64.117s     | 66.052s     |
| No-Pipeline  | 100k    | 1kb        | 5m    | N/A           | 69.527s     | 70.019s     |
| No-Pipeline  | 100k    | 10kb       | 5m    | N/A           | 101.712s    | 90.429s     |
| Pipeline     | 500k    | 5b         | 25m   | 10k           | 16.583s     | 35.217s     |
| Pipeline     | 500k    | 1kb        | 25m   | 10k           | 42.633s     | 66.865s     |
| Pipeline     | 500k    | 1kb        | 25m   | 1k            | 39.503s     | 68.294s     |
| Pipeline     | 100k    | 10kb       | 5m    | 1k            | 49.189s     | 26.303s     |

From these results we can see that `redis-stack` handles pipelined reads with higher throughput than `dragonflydb`. The tests used pipelines with 10,000 commands in each to prevent I/O errors.

The `redis-stack` utilized 100% of its single CPU core during execution while the `dragonflydb` utilized around 45-60% of the 16 CPU cores assigned to the VM for the duration of the test.

Redis recommends running `Redis Cluster` on a single host to shard out keys more effectively and increase throughput, I've yet to test that but given how little CPU utilization the single `redis-stack` consumed, I'm interested in conducting further testing.

### Versions
Docker Commands
```
docker run --rm -it --name=dragonfly_bench -p 6385:6379 docker.dragonflydb.io/dragonflydb/dragonfly
docker run --rm -it --name redis-stack -p 6380:6379 redis/redis-stack-server:latest
```

```
DragonflyDB: df-v1.3.0-f80afca9c23e2f30373437520a162c591eaa2005
Redis: 6.2.12 - oss
```


## GCP Tests (Host Network)

Tests below were run on a GCP `t2d-standard-2` (2 Core AMD EPYC Milan, 8GB RAM) instance with nothing else running on it, all containers were run in host network mode to avoid docker proxy bottlenecks.

Times shown are an average of 3 executions with DBs recreated in between each run.

| Test        | Inserts | Value Size | Reads | Pipeline Size | redis-stack | dragonflydb |
|-------------|---------|------------|-------|---------------|-------------|-------------|
| No-Pipeline | 100k    | 5b         | 5m    | N/A           | 47.542s     | 85.485s     |
| No-Pipeline | 100k    | 1kb        | 5m    | N/A           | 51.549s     | 87.573s     |
| No-Pipeline | 100k    | 10kb       | 5m    | N/A           | 84.431s     | 144.253s    |
| Pipeline    | 500k    | 5b         | 25m   | 10k           | 16.049s     | 41.891s     |
| Pipeline    | 500k    | 1kb        | 25m   | 10k           | 38.239s     | 69.168s     |
| Pipeline    | 500k    | 1kb        | 25m   | 1k            | 36.669s     | 83.073s     |
| Pipeline    | 100k    | 10kb       | 5m    | 1k            | 38.526s     | 40.733s     |


### Versions
Docker Commands
```
docker run --rm -it --network host --name=dragonfly_bench   docker.dragonflydb.io/dragonflydb/dragonfly
docker run --rm -it --network host --name redis-stack       redis/redis-stack-server:latest
```

```
DragonflyDB: df-v1.3.0-f80afca9c23e2f30373437520a162c591eaa2005
Redis: 6.2.12 - oss
```
