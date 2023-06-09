# Redis Benchmark Results

I have been exploring both DragonflyDB and Redis-Stack as options for in-memory datastores.

In this exploration I decided to run a few benchmarks in Go using the `github.com/redis/go-redis/v9` client.

I tested some simple operations on small key/value pairs both with and without pipelines and got these results:

```
Test: No-Pipeline, 100k key inserts, 5m reads

redis-stack: 64.117s
dragonflydb: 66.052s
```

```
Test: Pipeline, 500k key inserts, 25m reads

redis-stack: 16.583s
dragonflydb: 35.217s
```
