# BSky Experiments

This repo has contains some fun Go experiments interacting with BlueSky via the AT Protocol.

The Makefile contains some useful targets for building and running the experiments.

## Graph Builder (for the [BSky Atlas](https://bsky.jazco.dev))

The GraphBuilder is the main project in this repo, it constructs a graph of interactions between users on the platform based on all commits/posts that come in from the firehose.

The Graph is stored in a shared memory datastructure that's dumped to disk in a binary format every 30 seconds, overwriting its past checkpoint.

Every 30 minutes, it starts writing to a new file with an appropriate timestamp so you can track change in the graph over time.

Graph data is written to `data/social-graph.bin` for the latest version and `data/social-graph-{YYYY}_{MM}_{DD}_{HH}_{MM}.bin` for the checkpoints using the date and time according to current time UTC.

### Running the Graph Builder

1. Copy the contents of `.env.example` to `.env` in the root project directory.

2. Add your [BSky App Password](https://staging.bsky.app/settings/app-passwords) credentials (using your normal BSky email address as the username), replacing the placeholder in `ATP_AUTH=`.

3. For OTLP Tracing, configure the `OTEL_EXPORTER_OTLP_ENDPOINT=` to point at a valid OTLPHTTP trace collector.

   - If you don't want this functionality, delete the variable from the `.env` file and it will disable the feature.

4. If you'd like to collect the text and references in EVERY post on BSky, update `REGISTRY_DB_CONNECTION_STRING=` in `.env` to be a valid Postgres connection string to an accessible database.
   - If you don't want this functionality, delete the variable from the `.env` file and it will disable the feature.

To build containers and start up the Graph Builder, run:

```shell
$ docker-compose up --build -d
```

The only dependency required for this is Docker.

### Metrics

Metrics and debug routes exist at `{host}:6060/metrics` and `{host}:6060/debug/pprof/{profile}` for Prometheus metrics on data consumed and `pprof`-based memory and CPU profiling of the graph-builder.
