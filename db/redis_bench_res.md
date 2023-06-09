# Redis Benchmark Results

I have been exploring both DragonflyDB and Redis-Stack as options for in-memory datastores.

In this exploration I decided to run a few benchmarks in Go using the `github.com/redis/go-redis/v9` client.

All tests were performed on localhost behind Docker NAT, on a `AMD EPYC 7302P 16-Core Processor` inside a Proxmox VM with 24GB of allocated RAM available.

I tested some simple operations on small key/value pairs both with and without pipelines and got these results:

```
Test: No-Pipeline, 100k key inserts, 5m reads (50x read amplification)

redis-stack: 64.117s
dragonflydb: 66.052s
```

```
Test: Pipeline, 500k key inserts, 25m reads (50x read amplification)

redis-stack: 16.583s
dragonflydb: 35.217s
```

From these results we can see that `redis-stack` handles pipelined reads with higher throughput than `dragonflydb`. The tests used pipelines with 10,000 commands in each to prevent I/O errors.

The `redis-stack` utilized 100% of its single CPU core during execution while the `dragonflydb` utilized around 45-60% of the 16 CPU cores assigned to the VM for the duration of the test.

Without Docker NAT, the `No-Pipeline` tests executed in half the time for both DBs, while the `Pipeline` test didn't have a noticable difference in execution time for either DB when running under `network=host`.

Redis recommends running `Redis Cluster` on a single host to shard out keys more effectively and increase throughput, I've yet to test that but given how little CPU utilization the single `redis-stack` consumed, I'm interested in conducting further testing.

## Versions
Docker Commands
```
docker run --rm -it --name=dragonfly_bench -p 6385:6379 docker.dragonflydb.io/dragonflydb/dragonfly
docker run -it --name redis-stack -p 6380:6379 redis/redis-stack:latest
```

```
DragonflyDB: df-v1.3.0-f80afca9c23e2f30373437520a162c591eaa2005
Redis: 6.2.12 - oss
```
