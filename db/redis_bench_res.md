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

Tests below were run on GCP `t2d` instances (AMD EPYC Milan) with nothing else running on them, all containers were run in host network mode to avoid docker proxy bottlenecks.

Times shown are an average of 3 executions with DBs recreated in between each run.

### `t2d-standard-2`


### `t2d-standard-4`

| Test Name   | Inserts | Value Size | Reads | Pipeline Size | Repetitions | redis-stack (write) | redis-stack (read) | dragonfly (write) | dragonfly (read) |
|-------------|---------|------------|-------|---------------|-------------|---------------------|--------------------|-------------------|------------------|
| No-Pipeline | 100k    | 5b         | 5m    | -1            | 3           | 4.307s              | 46.0463s           | 7.780s            | 1m25.603s        |
| No-Pipeline | 100k    | 1kb        | 5m    | -1            | 3           | 4.482s              | 50.594s            | 7.989s            | 1m30.717s        |
| No-Pipeline | 100k    | 10kb       | 5m    | -1            | 3           | 6.260s              | 1m25.990s          | 9.310s            | 2m22.115s        |
| Pipeline    | 500k    | 5b         | 25m   | 10k           | 3           | 1.538s              | 15.214s            | 12.625s           | 37.685s          |
| Pipeline    | 500k    | 1kb        | 25m   | 10k           | 3           | 2.290s              | 37.069s            | 14.342s           | 1m4.967s         |
| Pipeline    | 500k    | 1kb        | 25m   | 1k            | 3           | 2.470s              | 35.964s            | 12.218s           | 1m22.487s        |
| Pipeline    | 100k    | 10kb       | 5m    | 1k            | 3           | 1.644s              | 37.156s            | 4.160s            | 41.889s          |



### `t2d-standard-8`

| Test Name   | Inserts | Value Size | Reads | Pipeline Size | Repetitions | redis-stack (write) | redis-stack (read) | dragonfly (write) | dragonfly (read) |
|-------------|---------|------------|-------|---------------|-------------|---------------------|--------------------|-------------------|------------------|
| No-Pipeline | 100k    | 5b         | 5m    | -1            | 3           | 3.602s              | 27.297s            | 7.468s            | 22.250s          |
| No-Pipeline | 100k    | 1kb        | 5m    | -1            | 3           | 3.678s              | 29.751s            | 7.619s            | 24.723s          |
| No-Pipeline | 100k    | 10kb       | 5m    | -1            | 3           | 4.765s              | 43.024s            | 8.887s            | 40.605s          |
| Pipeline    | 500k    | 5b         | 25m   | 10k           | 3           | 1.480s              | 13.444s            | 15.327s           | 12.918s          |
| Pipeline    | 500k    | 1kb        | 25m   | 10k           | 3           | 2.266s              | 32.299s            | 17.051s           | 21.063s          |
| Pipeline    | 500k    | 1kb        | 25m   | 1k            | 3           | 2.332s              | 30.711s            | 14.921s           | 39.722s          |
| Pipeline    | 100k    | 10kb       | 5m    | 1k            | 3           | 1.472s              | 38.814s            | 3.873s            | 14.283s          |


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
