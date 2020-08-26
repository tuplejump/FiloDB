# FiloDB

[![Join the chat at https://gitter.im/velvia/FiloDB](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/velvia/FiloDB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/filodb/FiloDB.svg?branch=develop)](https://travis-ci.org/filodb/FiloDB)

Distributed, Prometheus-compatible, real-time, in-memory, massively scalable, multi-schema time series / event / operational database.


```
    _______ __      ____  ____ 
   / ____(_) /___  / __ \/ __ )
  / /_  / / / __ \/ / / / __  |
 / __/ / / / /_/ / /_/ / /_/ / 
/_/   /_/_/\____/_____/_____/  
```

![](Dantat.jpg)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
  - [Use Cases](#use-cases)
  - [Anti-use-cases](#anti-use-cases)
- [Pre-requisites](#pre-requisites)
- [Getting Started](#getting-started)
- [End to End Kafka Developer Setup](#end-to-end-kafka-developer-setup)
  - [Using the Gateway to stream Application Metrics](#using-the-gateway-to-stream-application-metrics)
    - [Multiple Servers using Consul](#multiple-servers-using-consul)
    - [Local Scale Testing](#local-scale-testing)
- [Understanding the FiloDB Data Model](#understanding-the-filodb-data-model)
  - [Prometheus FiloDB Schema for Operational Metrics](#prometheus-filodb-schema-for-operational-metrics)
  - [Traditional, Multi-Column Schema](#traditional-multi-column-schema)
  - [Data Modelling and Performance Considerations](#data-modelling-and-performance-considerations)
  - [Sharding](#sharding)
- [Querying FiloDB](#querying-filodb)
  - [FiloDB PromQL Extensions](#filodb-promql-extensions)
  - [First-Class Histogram Support](#first-class-histogram-support)
  - [Using the FiloDB HTTP API](#using-the-filodb-http-api)
  - [Grafana setup](#grafana-setup)
  - [Using the CLI](#using-the-cli)
  - [CLI Options](#cli-options)
  - [Configuring the CLI](#configuring-the-cli)
- [Current Status](#current-status)
- [Deploying](#deploying)
- [Monitoring and Metrics](#monitoring-and-metrics)
  - [Metrics Sinks](#metrics-sinks)
  - [Metrics Configuration](#metrics-configuration)
- [Code Walkthrough](#code-walkthrough)
- [Building and Testing](#building-and-testing)
  - [Debugging serialization and queries](#debugging-serialization-and-queries)
  - [Other debugging tips](#other-debugging-tips)
  - [Benchmarking](#benchmarking)
- [You can help!](#you-can-help)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

FiloDB is an open-source distributed, real-time, in-memory, massively scalable, multi-schema time series / event / operational database with Prometheus query support and some Spark support as well.

The normal configuration for real-time ingestion is deployment as stand-alone processes in a cluster, ingesting directly from Apache Kafka.  The processes form a cluster using peer-to-peer Akka Cluster technology.

* **Massively Scalable** - designed to ingest many millions of entities, sharded across multiple processes, with distributed querying built in
* **Prometheus PromQL Support**
* **Tag-based Indexing** - Support for indexing and fast querying over flexible tags for each time series/partition, just like Prometheus
* **Efficient** - holds a huge amount of data in-memory thanks to columnar compression techniques
* **Low-latency** - designed for highly concurrent, low-latency workloads such as dashboards and alerting
* **Real Time** - data immediately available for querying once ingested
* **Fault Tolerant** - designed for dual-datacenter operation with strong recoverability and no single point of failure. explain explain
* **Multi-Schema and Multi-Stream** - easily segregate and prioritize different classes of metrics and data.  Easily support different types of events.
* **Off Heap** - intelligent memory management minimizes garbage collection

[Overview presentation](http://velvia.github.io/presentations/2015-filodb-spark-streaming/#/) -- see the docs folder for design docs.

To compile the .mermaid source files to .png's, install the [Mermaid CLI](http://knsv.github.io/mermaid/mermaidCLI.html).

### Use Cases

* Real-time operational metrics storage, querying, dashboards, visibility
* Distributed tracing (ie Zipkin like) storage
* Low-latency real-time ad-hoc application metric debugging
* Real-time event storage and querying

### Anti-use-cases

* Heavily transactional, update-oriented workflows
* OLAP / Analytics

## Pre-requisites

1. [Java 11](https://www.oracle.com/java/technologies/javase-downloads.html#JDK11)
2. [SBT](http://www.scala-sbt.org/) to build
3. [Apache Cassandra](http://cassandra.apache.org/) 2.x or 3.x (We prefer using [CCM](https://github.com/pcmanus/ccm) for local testing)
    - For testing, install a single node C* cluster, like this:  `ccm create v39_single -v 3.9 -n 1 -s`
4. [Apache Kafka](http://kafka.apache.org/) 0.10.x or above
 
Optional:

4. [Apache Spark (2.0)](http://spark.apache.org/)

## Getting Started

1. Clone the project and cd into the project directory,

    ```
    $ git clone https://github.com/filodb/FiloDB.git
    $ cd FiloDB
    ```

    - It is recommended you use the last stable released version.
    - To build, run `filo-cli` (see below) and also `sbt spark/assembly`.

Follow the instructions below to set up an end to end local environment.

## End to End Kafka Developer Setup

This section describes how you can run an end-to-end test locally on a Macbook by ingesting time series data into FiloDB In Memory Store, and querying from it using PromQL.

Use your favorite package manager to install and set up pre-requisite infrastructure. Kafka 0.10.2+ or 0.11 can be used. 
```
brew install kafka
brew services start zookeeper
brew services start kafka
```

Create a new Kafka topic with 4 partitions. This is where time series data will be ingested for FiloDB to consume
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev-ds-1m
```

Download and start Cassandra 2.1 or more recent versions (Cassandra 3 and above recommended).

```
bin/cassandra
```

You should install Cassandra using a tool which you're the most familiar with.

For instance, one easy way to install it is via `brew`
```
brew install cassandra
```

Start Cassandra
```
brew services start cassandra
```

Build the required projects
```
sbt standalone/assembly cli/assembly gateway/assembly
```

First initialize the keyspaces and tables in Cassandra. 
```
./scripts/schema-create.sh filodb_admin filodb filodb_downsample prometheus 4 1,5 > /tmp/ddl.cql
cqlsh -f /tmp/ddl.cql
```
Verify that tables were created in `filodb`, `filodb_downsample` and `filodb-admin` keyspaces using `cqlsh`:
First type `cqlsh` to start the cassandra cli. Then check the keyspaces by entering `DESCRIBE keyspaces`.


The script below brings up the FiloDB Dev Standalone server, and then sets up the prometheus dataset (NOTE: if you previously started FiloDB and have not cleared the metadata, then the -s is not needed as FiloDB will recover previous ingestion configs from Cassandra. This script targets directly towards the `develop` branch.)

```
./filodb-dev-start.sh
```

Note that the above script starts the server with configuration at `conf/timeseries-filodb-server.conf`. This config
file refers to the following datasets that will be loaded on bootstrap:
* `conf/timeseries-dev-source.conf`

For queries to work properly you'll want to start a second server to serve all the shards:

```
./filodb-dev-start.sh -l 2 -p
```

To quickly verify that both servers are up and set up for ingestion, do this (the output below was formatted using `| jq '.'`, ports may vary):

```
curl localhost:8080/api/v1/cluster/prometheus/status

{
  "status": "success",
  "data": [
    {
      "shard": 0,
      "status": "ShardStatusActive",
      "address": "akka://filo-standalone"
    },
    {
      "shard": 1,
      "status": "ShardStatusActive",
      "address": "akka://filo-standalone"
    },
    {
      "shard": 2,
      "status": "ShardStatusActive",
      "address": "akka.tcp://filo-standalone@127.0.0.1:57749"
    },
    {
      "shard": 3,
      "status": "ShardStatusActive",
      "address": "akka.tcp://filo-standalone@127.0.0.1:57749"
    }
  ]
}
```

You can also check the server logs at `logs/filodb-server-N.log`.

Now run the time series generator. This will ingest 20 time series (the default) with 100 samples each into the Kafka topic with current timestamps.  The required argument is the path to the source config.  Use `--help` for all the options.

```
./dev-gateway.sh --gen-prom-data conf/timeseries-dev-source.conf
```

NOTE: Check logs/gateway-server.log for logs.

At this point, you should be able to confirm such a message in the server logs: `KAMON counter name=memstore-rows-ingested count=4999`

Now you are ready to query FiloDB for the ingested data. The following command should return matching subset of the data that was ingested by the producer.

```
./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --promql 'heap_usage{_ws_="demo", _ns_="App-2"}'
```

You can also look at Cassandra to check for persisted data. Look at the tables in `filodb` and `filodb-admin` keyspaces.

If the above does not work, try the following:

1) Delete the Kafka topic and re-create it.  Note that Kafka topic deletion might not happen until the server is stopped and restarted

- Before you remove a topic, update server.properties from configuration(conf) folder and have delete.topic.enable property set to true:
`delete.topic.enable=true`
- Run below kafka-topics.sh command with “–delete” option to remove “timeseries-dev” and "timeseries-dev-ds-1m":
```sh
 /usr/local/Cellar/kafka/2.3.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 \
                --topic timeseries-dev \
                --delete  

```
```
/usr/local/Cellar/kafka/2.3.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 \
                --topic timeseries-dev-ds-1m \
                --delete
```
- You should see: 
```
Topic timeseries-dev is marked for deletion.
Note: This will have no impact if delete.topic.enable is not set to true.
```

2) `./filodb-dev-stop.sh` and restart filodb instances like above
3) Re-run `./dev-gateway.sh --gen-prom-data`.  You can check consumption via running the `TestConsumer`, like this:  `java -Xmx4G -Dconfig.file=conf/timeseries-filodb-server.conf -cp standalone/target/scala-2.12/standalone-assembly-0.8-SNAPSHOT.jar  filodb.kafka.TestConsumer conf/timeseries-dev-source.conf`.  Also, the `memstore_rows_ingested` metric which is logged to `logs/filodb-server-N.log` should become nonzero.

To stop the dev server. Note that this will stop all the FiloDB servers if multiple are running.
```
./filodb-dev-stop.sh
```

### Using the Gateway to stream Application Metrics

FiloDB includes a Gateway server that listens to application metrics and data on a TCP port, converts the data to its internal format, shards it properly and sends it Kafka.

**STATUS**: Currently the only supported format is [Influx Line Protocol](https://docs.influxdata.com/influxdb/v1.6/write_protocols/line_protocol_tutorial/).  The only tested configuration is using Telegraf with a Prometheus endpoint source and a socket writer using ILP protocol.

The following will scrape metrics from FiloDB using its Prometheus metrics endpoint, and forward it to Kafka to be queried by FiloDB itself  :)

1. Make sure the above steps are followed for setting up and starting FiloDB, configuring datasets and Kafka topics.
2. Download [Telegraf](https://github.com/influxdata/telegraf)
3. Start the FiloDB gateway:  `./dev-gateway.sh`
3. Start Telegraf using the config file `conf/telegraf.conf` : `telegraf --config conf/telegraf.conf`.  This config file scrapes from a Prom endpoint at port 9095 and forwards it using ILP format to a TCP socket at 8007, which is the gateway default

Now, metrics from the application having a Prom endpoint at port 9095 will be streamed into Kafka and FiloDB.

Querying the total number of ingesting time series for the last 5 minutes, every 10 seconds:

    ./filo-cli  --host 127.0.0.1 --dataset prometheus --promql 'sum(num_ingesting_partitions{_ws_="local_test",_ns_="filodb"})' --minutes 5

Note that histograms are ingested using FiloDB's optimized histogram format, which leads to very large savings in space.  For example, querying the 90%-tile for the size of chunks written to Cassandra, last 5 minutes:

    ./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --promql 'histogram_quantile(0.9, sum(rate(chunk_bytes_per_call{_ws_="local_test",_ns_="filodb"}[3m])))' --minutes 5

Here is how you display the raw histogram data for the same:

    ./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --promql 'chunk_bytes_per_call{_ws_="local_test",_ns_="filodb"}' --minutes 5

#### Multiple Servers using Consul

The original example used a static IP to form a cluster, but a more realistic example is to use a registration service like Consul.
To try using Consul for startup, first set up Consul dev agent:

```
> brew install consul 
> cat /usr/local/etc/consul/config/basic_config.json 
{
"data_dir": "/usr/local/var/consul",
"ui" : true,
"dns_config" : {
    "enable_truncate" : true
}
 	
```

Then run consul consul agent in dev mode:
```
consul agent -dev -config-dir=/usr/local/etc/consul/config/
```
Perform the filo-cli `init` and `create` steps as before.

Start first FiloDB server 
```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-consul.conf -l 1
```
And subsequent FiloDB servers. Change log file suffix with the `-l` option for each server. 
```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-consul.conf -l 2 -p
```

### Downsample Filo Cluster

To bring up local cluster for serving downsampled data

```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-ds.conf -l 1
```
Subsequent servers. Change log file suffix with the `-l` option for each server.
```
./filodb-dev-start.sh -c conf/timeseries-filodb-server-ds.conf -l 2 -p
```

If you had run the unit test `DownsamplerMainSpec` which populates data into the downsample
dataset, you can query downsample results by visiting the following URL: 

```
curl "http://localhost:9080/promql/prometheus/api/v1/query_range?query=my_counter\{_ws_='my_ws',_ns_='my_ns'\}&start=74372801&end=74373042&step=10&verbose=true&spread=2"
```

#### Local Scale Testing

Follow the same steps as in original setup, but do this first to clear out existing metadata:

```bash
./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf --command clearMetadata
```

Then follow the steps to create the dataset etc.  Create a different Kafka topic with 128 partitions:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 128 --topic timeseries-perf
```

Start two servers as follows. This will not start ingestion yet:

```bash
./filodb-dev-start.sh -l 1
./filodb-dev-start.sh -l 2 -p
```

Set up ingestion:

```bash
./filo-cli --host 127.0.0.1 --dataset prometheus --command setup --filename conf/timeseries-128shards-source.conf
```

Now if you curl the cluster status you should see 128 shards which are slowly turning active: `curl http://127.0.0.1:8080/api/v1/cluster/timeseries/status | jq '.'`

Generate records:

```
./dev-gateway.sh --gen-prom-data -p 5000 conf/timeseries-128shards-source.conf
```

## Understanding the FiloDB Data Model

FiloDB is designed to scale to ingest and query millions of discrete time series.  A single time series consists of data points that contain the same **partition key**.  Successive data points are appended.  Each data point must contain a timestamp.  Examples of time series:

* Individual operational metrics
* Data from a single IoT device
* Events from a single application, device, or endpoint

The **partition key** differentiates time series and also controls distribution of time series across the cluster.  For more information on sharding, see the sharding section below.  Components of a partition key, including individual key/values of `MapColumn`s, are indexed and used for filtering in queries.

The data points use a configurable schema consisting of multiple columns.  Each column definition consists of `name:columntype`, with optional parameters. For examples, see the examples below, or see the introductory walk-through above where two datasets are created.

A single partition key schema is used for a running FiloDB cluster, though multiple data schemas may be supported.  These schemas are defined in the config file - see the `partition-schema` and `schemas` sections of `filodb-defaults.conf`.  The CLI command `validateSchemas` may be run to verify schemas defined in config files, as follows:

    ./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf --command validateSchemas

### Dataset Configuration

THIS IS IMPORTANT TO READ AND UNDERSTAND.

Each "dataset" ingests one stream or Kafka topic of raw time series data, and is also the unit of isolation.  Each dataset contains its own offheap memory, and can have independent data retention and ingestion properties.

Datasets are setup and loaded into the server via configuration files referred to by application.conf loaded by the server.
See `conf/timeseries-dev-source.conf` for an example. It is important to note that some aspects of the dataset,
like its column definition are immutable. This is primarily because the data columns are used to populate persistent
cassandra store. Once created, it should not be changed. If altered, it may render cassandra unreadable. 
 
However, some aspects of the dataset which relate to runtime configuration are mutable. For example, amount of chunk
and buffer memory allocated, flush interval etc. That said, these are advanced settings and one needs to know
implications of the configuration before changing them. If a rolling bounce is being done, one needs to remember
that part of the cluster could be with the old config and the rest could have new config applied.

### Prometheus FiloDB Schema for Operational Metrics

* Partition key = `metric:string,tags:map`
* Columns: `timestamp:ts,value:double:detectDrops=true`

The above is the classic Prometheus-compatible schema.  It supports indexing on any tag.  Thus standard Prometheus queries that filter by a tag such as `hostname` or `datacenter` for example would work fine.  Note that the Prometheus metric name, which in Prometheus data is a label/tag with the key `__name__`, is separated out to an explicit metric column.

Note that in the Prometheus data model, more complex metrics such as histograms are represented as individual time series.  This has some simplicity benefits, but does use up more time series and incur extra I/O overhead when transmitting raw data records.

NOTE: `detectDrops=true` allows for proper and efficient rate calculation on Prometheus counters.
 
### Traditional, Multi-Column Schema

Let's say that one had a metrics client, such as CodaHale metrics, which pre-aggregates percentiles and sends them along with the metric.  If we used the Prometheus schema, each percentile would wind up in its own time series.  This is fine, but incurs significant overhead as the partition key has to then be sent with each percentile over the wire.  Instead we can have a schema which includes all the percentiles together when sending the data:

* Partition key = `metricName:string,tags:map`
* Columns: `timestamp:ts,min:double,max:double,p50:double,p90:double,p95:double,p99:double,p999:double`

### Data Modelling and Performance Considerations

For more information on memory configuration, please have a look at the [ingestion guide](doc/ingestion.md).

**Choosing Partition Keys**.

FiloDB is designed to efficiently ingest a huge number of individual time series - depending on available memory, one million or more time series per node is achievable.  Here are some pointers on choosing them:

* It is better to have smaller time series, as the indexing and filtering operations are designed to work on units of time series, and not samples within each time series.
* The default partition key consists of a metric name, and tags represented as a  `MapColumn`.
* Each time series does take up both heap and offheap memory, and memory is likely the main limiting factor.  The amount of configured memory limits the number of actively ingesting time series possible at any moment.

### Sharding

All data for a single time series, which belong to the same partition key, are routed to the same shard, or Kafka partition, and one shard must fit completely into the memory of a single node.

The number of shards in each dataset is preconfigured in the source config.  Please see the [ingestion doc](doc/ingestion.md) for more information on configuration.  

Metrics are routed to shards based on factors:

1. Shard keys, which can be for example an application and the metric name, which define a group of shards to use for that application.  This allows limiting queries to a subset of shards for lower latency. Currently `_ws_`, `_ns_` labels are mandatory to calculate shard key along with `_metric_` column.
2. The rest of the tags or components of a partition key are then used to compute which shard within the group of shards to assign to.

## Querying FiloDB

FiloDB can be queried using the [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/) through its HTTP API or through its CLI.

### FiloDB PromQL Extensions

FiloDB supports computing Z-score queries. It represents the distance between the raw score and the population mean in units of the standard deviation. This can be used for anomaly detection.
 
Since FiloDB supports multiple schemas, with possibly more than one value column per schema, there needs to be a way to specify the target column to query.  This is done using a `::columnName` suffix in the metric name, like this request which pulls out the "min" column:

    http_req_timer::min{_ws_="demo", _ns_="foo"}

By default if `::col` suffix is not specified then the `value-column` option of each data schema is used.

Some special functions exist to aid debugging and for other purposes:

| function | description    |
|----------|----------------|
| `_filodb_chunkmeta_all` | (CLI Only) Returns chunk metadata fields for all chunks matching the time range and filter criteria - ID, # rows, start and end time, as well as the number of bytes and type of encoding used for a particular column.  |
| `histogram_bucket` | Extract a bucket from a HistogramColumn |
| `histogram_max_quantile` | More accurate `histogram_quantile` when the max is known |
| `hist_to_prom_vectors` | Convert a histogram to a set of equivalent Prometheus-style bucket time series |

Example of debugging chunk metadata using the CLI:

    ./filo-cli --host 127.0.0.1 --dataset prometheus --promql '_filodb_chunkmeta_all(heap_usage{_ws_="demo",_ns_="App-0"})' --start XX --end YY

There is also a special filter, `_type_="gauge"`, to filter on only a particular type of metric or schema.  Normally, this is not necessary unless a user changes the type of metric in their application, for example from a gauge to a histogram.  The types are found in the configuration `schemas` section, and by default are `gauge`, `prom-counter`, `prom-histogram`, and `untyped`.

### First-Class Histogram Support

One major difference FiloDB has from the Prometheus data model is that FiloDB supports histograms as a first-class entity.  In Prometheus, histograms are stored with each bucket in its own time series differentiated by the `le` tag.  In FiloDB, there is a `HistogramColumn` which stores all the buckets together for significantly improved compression, especially over the wire during ingestion, as well as significantly faster query speeds (up to two orders of magnitude).  There is no "le" tag or individual time series for each bucket.  Here are the differences users need to be aware of when using `HistogramColumn`:

* There is no need to append `_bucket` to the metric name.
* To compute quantiles:  `histogram_quantile(0.7, sum(rate(http_req_latency{app="foo"}[5m])))`
* To extract a bucket: `http_req_latency{app="foo",_bucket_="100.0"}`  (`_bucket_` is a special filter that translates to the `histogram_bucket` function for bucket extraction)
* Sum over multiple Histogram time series:  `sum(rate(http_req_latency{app="foo"}[5m]))` - you could then compute quantile over the sum.
* To convert a `HistogramColumn` data back to Prometheus-style time series for each bucket, use the `hist_to_prom_vectors` function
  - NOTE: Do NOT use `by (le)` when summing `HistogramColumns`.  This is not appropriate as the "le" tag is not used.  FiloDB knows how to sum multiple histograms together correctly without grouping tricks.
  - FiloDB prevents many incorrect histogram aggregations in Prometheus when using `HistogramColumn`, such as handling of multiple histogram schemas across time series and across time.

FiloDB offers an improved accuracy `histogram_max_quantile` function designed to work with a max column from the source.  If clients are able to send the max value captured during a window, then we can report more accurate upper quantiles (ie 99%, 99.9%, etc.) that do not suffer from clipping.

### Step & Lookback PromQL Improvements

The value provided within square brackets in the Range Vector Selector expression of the PromQL expression
is commonly known as "range" or "lookback". It indicates how far behind each instant
the query engine will lookback to normalize the value for any given instant.

**Problem 1:** PromQL has an important gotcha on range value. Hard coded lookback
can yield to queries with unintended effects. Lets consider the expression `rate(foo[5m])`. The result
of this query would have completely different meanings with different step values. Since step parameter
typically is not controlled by user (as in Grafana), it can have silent negative effects. For example, if
user expands the time range and hardcoded lookback becomes smaller than step, the same query will no
longer do what the user originally intended to do - the lookback will not include enough
samples for an effective query.

To alleviate this, we introduce a new range notation where lookback can be specified as a multiple of step.
The notation `[1i]` would cause lookback to be 1 times step. Notation `[2i]` would make lookback as 2 times
step and so on. This feature is in alignment with [MetricsQL](https://github.com/VictoriaMetrics/VictoriaMetrics/wiki/MetricsQL)
approach to solving this. 

This notation will help users who do not know what range value to use in their queries. The `[1i]` should be a good
default for all queries. 2 or more times step can be used when smoothing is needed.

**Problem 2:** There is yet another gotcha that we solve. If step == lookback, lookback windows in consecutive instants
become adjacent and non-overlapping. In the case of cumulative counters, where rate or increase is used, this can
still yield to wrong results. The increase or decrease between the adjacent windows would be omitted from the query. 

The solution that we have adopted to solve this is to extend the lookback window by the publish interval of the
time series. The padding is done if the publish interval is supplied. The publish interval is supplied as an
internal `_step_` tag within the time series tags. The intent here is to make it better over time. by making it
a first class column in the partition key schema. 

Important to note that the lookback padding happens **only** if step multiple notation was used in the PromQL expression.
If a fixed range was used in the query, that value will be honored as such. This way, backward compatibility will
always be retained.

Examples
* `sum_over_time(foo[5i])` with step = 10s will be equivalent to `sum_over_time(foo[50s])` , thus lookback = 5 * 10s
* `sum_over_time(foo[1i])` with step = 10s will be equivalent to `sum_over_time(foo[10s])` , thus lookback = 1 * 10s
* `rate(foo[1i])` with step = 60s and publishInterval = 10s will be equivalent to `rate(foo[70s])`, thus 
   lookback = 1 * 60s + 10s. Publish interval is padded when both counter functions and step-factor notation is used.
* `sum_over_time(foo[2.5i])` - decimal scale can also be used. Here lookback will be 2.5 times the step.
* `foo offset 2i` - offset can also use step multiple notation  
### Using the FiloDB HTTP API

Please see the [HTTP API](doc/http_api.md) doc.

Example:

    curl 'localhost:8080/promql/timeseries/api/v1/query?query=memstore_rows_ingested_total{_ws_="demo", _ns_="filodb"}[5m]&time=1568756529'

```json
{
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "host": "MacBook-Pro-229.local",
          "shard": "1",
          "__name__": "memstore_rows_ingested_total",
          "dataset": "prometheus",
          "_ws_": "demo"
          "_ns_": "filodb"
        },
        "values": [
          [
            1539908420,
            "252.0"
          ],
          [
            1539908430,
            "252.0"
          ],
          [
            1539908440,
            "252.0"
          ],
          [
            1539908450,
            "252.0"
          ],
          [
            1539908460,
            "252.0"
          ],
          [
            1539908470,
            "360.0"
          ]
        ]
      },
      {
        "metric": {
          "host": "MacBook-Pro-229.local",
          "shard": "0",
          "__name__": "memstore_rows_ingested_total",
          "dataset": "prometheus",
          "_ws_": "demo"
          "_ns_": "filodb"
        },
        "values": [
          [
            1539908420,
            "462.0"
          ],
          [
            1539908430,
            "462.0"
          ],
          [
            1539908440,
            "462.0"
          ],
          [
            1539908450,
            "462.0"
          ],
          [
            1539908460,
            "462.0"
          ],
          [
            1539908470,
            "660.0"
          ]
        ]
      }
    ]
  },
  "status": "success"
}
```

The HTTP API can also be used to quickly check on the cluster and shard status:

    curl localhost:8080/api/v1/cluster/timeseries/status | jq '.'

```json
{
  "status": "success",
  "data": [
    {
      "shard": 0,
      "status": "ShardStatusActive",
      "address": "akka://filo-standalone"
    },
    {
      "shard": 1,
      "status": "ShardStatusActive",
      "address": "akka://filo-standalone"
    },
    {
      "shard": 2,
      "status": "ShardStatusActive",
      "address": "akka.tcp://filo-standalone@127.0.0.1:52519"
    },
    {
      "shard": 3,
      "status": "ShardStatusActive",
      "address": "akka.tcp://filo-standalone@127.0.0.1:52519"
    }
  ]
}
```

### Grafana setup

Since FiloDB exposes a Prometheus-compatible HTTP API, it is possible to set up FiloDB as a Grafana data source.

* Set the data source type to "Prometheus"
* In the HTTP URL box, enter in the FiloDB HTTP URL (usually the load balancer for all the FiloDB endpoints).   Be sure to append `/promql/timeseries/`, where you would put the name of the dataset instead of "timeseries" if it is not called timeseries.

### Using the CLI

The CLI is now primarily used to interact with standalone FiloDB servers, including querying, getting status, and as a way to initialize dataset definitions and do admin tasks.  The following examples use the FiloDB/Gateway/Telegraf setup from the section [Using the Gateway to stream Application Metrics](#using-the-gateway-to-stream-application-metrics) -- but be sure to start a second FiloDB server, using `./filodb-dev-start.sh -l 2 -p`.

The **indexnames** command lists all of the indexed tag keys or column names, based on the partition key or Prometheus key/value tags that define time series:

    ./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --command indexnames

```
le
host
shard
__name__
dataset
```

The **indexvalues** command lists the top values (as well as their cardinality) in specific shards for any given tag key or column name.  Here we list the top metrics (for Prometheus schema, which uses the tag `__name__`) in shard 0:

    ./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --command indexvalues --indexName __name__ --shards 0

```
             chunk_bytes_per_call_bucket  10
                  chunks_per_call_bucket  10
       kafka_container_size_bytes_bucket  10
        num_samples_per_container_bucket  10
       blockstore_blocks_reclaimed_total  1
                  blockstore_free_blocks  1
blockstore_time_ordered_blocks_reclaimed_total  1
                  blockstore_used_blocks  1
             memstore_data_dropped_total  1
memstore_encoded_bytes_allocated_bytes_total  1
```

Now, let's query a particular metric:

    ./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset prometheus --promql 'memstore_rows_ingested_total{_ws_="demo", _ns_="filodb"}'

```
Sending query command to server for prometheus with options QueryOptions(<function1>,16,60,100,None)...
Query Plan:
PeriodicSeries(RawSeries(IntervalSelector(List(1539908042000),List(1539908342000)),List(ColumnFilter(app,Equals(filodb)), ColumnFilter(__name__,Equals(memstore_rows_ingested_total))),List()),1539908342000,10000,1539908342000)
/shard:1/b2[[__name__: memstore_rows_ingested_total, app: filodb, dataset: prometheus, host: MacBook-Pro-229.local, shard: 1]]
  2018-10-18T17:19:02.000-07:00 (1s ago) 1539908342000  36.0

/shard:3/b2[[__name__: memstore_rows_ingested_total, app: filodb, dataset: prometheus, host: MacBook-Pro-229.local, shard: 0]]
  2018-10-18T17:19:02.000-07:00 (2s ago) 1539908342000  66.0
```

### CLI Options

The `filo-cli` accepts arguments and options as key-value pairs, specified like `--limit=100`  For quick help run it with no arguments.  A subset of useful options:

| key          | description   |
|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dataset    | It is required for all the operations. Its value should be the name of the dataset |
| host       | The hostname or IP address of the FiloDB standalone host |
| port       | The port number of the FiloDB standalone host.  Defaults to 2552.  |
| start      | The start of the query timerange in seconds since epoch  |
| step       | The step size in seconds of the PromQL query.  Successive windows occur at every step seconds   |
| stop       | The end of the query timerange in seconds since epoch    |
| minutes    | A shortcut to set the start at N minutes ago, and the stop at current time.  Should specify a step also.   |
| chunks     | Either "memory" or "buffers" to select either all the in-memory chunks or the write buffers only.  Should specify a step also. |
| database   | Specifies the "database" the dataset should operate in.  For Cassandra, this is the keyspace.  If not specified, uses config value.  |
| limit      | Limits the number of time series in the output  |
| sampleLimit | Maximum number of output samples in the query result.  An exception is thrown if the output returns more results than this.  |
| shards     | (EXPERT) overrides the automatic shard calculation by passing in a comma-separated list of specific shards to query.  Very useful to debug sharding issues.  |
| everyNSeconds  | Repeats the query every (argument) seconds     |
| timeoutSeconds | The number of seconds for the network timeout  |

### Configuring the CLI

Using the CLI to initialize Cassandra clusters and datasets necessitates passing in the right configuration.  The easiest is to pass in a config file:

    ./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf --command init

You may also set the `FILO_CONFIG_FILE` environment var instead, but any `-Dfilodb.config.file` args passed in takes precedence.

Individual configuration params may also be changed by passing them on the command line.  They must be the first arguments passed in.  For example:

    ./filo-cli -Dfilodb.cassandra.keyspace=mykeyspace --command init

All `-D` config options must be passed before any other arguments.

You may also configure CLI logging by copying `cli/src/main/resources/logback.xml` to your deploy folder, customizing it, and passing on the command line `-Dlogback.configurationFile=/path/to/filo-cli-logback.xml`.

You can also change the logging directory by setting the FILO_LOG_DIR environment variable before calling the CLI.

### Debugging Binary Vectors and Binary Records

Following command can be used to formulate BR for a partKey using Prometheus filter. Use this to formulate CQL to issue a cassandra query

    > ./filo-cli --command promFilterToPartKeyBR --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --schema prom-counter
    0x2c0000000f1712000000200000004b8b36940c006d794d65747269634e616d650e00c104006d794e73c004006d795773
    
Following command can be used to decode partKey BR into tag/value pairs

    > ./filo-cli --command partKeyBrAsString --hexPk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773
    b2[schema=prom-counter  _metric_=myMetricName,tags={_ns_: myNs, _ws_: myWs}]

Following command can be used to decode a ChunkSetInfo read from Cassandra

    > ./filo-cli  --command decodeChunkInfo --hexChunkInfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000
    ChunkSetInfo id=-2620393330526787566 numRows=6 startTime=1574272801000 endTime=1574273042000
    
Following command can be used to decode a Binary Vector. Valid vector types are `d` for double, `i` for integer `l` for long, `h` for histogram and `s` for string
     
    > ./filo-cli  --command decodeVector  --vectorType d --hexVector 0x1b000000080800000300000000000000010000000700000006080400109836
    DoubleLongWrapDataReader$
    3.0,5.0,13.0,15.0,13.0,11.0,9.0
 
## Current Status

| Component | Status     |
|-----------|------------|
| FiloDB Standalone | Stable, tested at scale          |
| Gateway           | Experimental                     |
| Cassandra         | Stable, works with C-2.x and 3.x |
| Kafka             | Stable                           |
| Spark             | Deprecated                       |

FiloDB PromQL Support: currently FiloDB supports about 60% of PromQL.  We are working to add more support regularly.

## Deploying

- `sbt standalone/assembly`
- `sbt cli/assembly`
- `sbt gateway/assembly`
- Copy and modify `conf/timeseries-filodb-server.conf`, deploy it
- Create a source config.  See [ingestion docs](doc/ingestion.md) as well as `conf/timeseries-128shards-source.conf` as examples.
- Run the cli jar as the filo CLI command line tool and initialize keyspaces if using Cassandra: `filo-cli-*.jar --command init`
- Create datasets
- See [Akka Bootstrapper](doc/akka-bootstrapper.md) for different methods of bootstrapping FiloDB clusters
- Start the gateway server(s)
- Run the CLI setup command to set up the standalone nodes to start ingesting a dataset from Kafka

NOTE: The setup command only has to be run the first time you start up the standalone servers.  After that, the setup is persisted to Cassandra so that on startup, FiloDB nodes will automatically start ingestion from that dataset.

Recommended flags:

- `-XX:MaxInlineLevel=20`

## Monitoring and Metrics

FiloDB uses [Kamon](http://kamon.io) for metrics and Akka/Futures/async tracing.  Not only does this give us summary statistics, but this also gives us Zipkin-style tracing of the ingestion write path in production, which can give a much richer picture than just stats.

### Metrics Sinks

Kamon metrics sinks are configured using the config key `kamon.reporters`.  For an example see `conf/timeseries-filodb-server.conf`.  Simply list the sinks/reporters to enable in that key.  See the Kamon [docs for Reporters](https://kamon.io/documentation/1.x/reporters/prometheus/) for more info and config options on each one.  Here are some possible values for reporters:

* `kamon.prometheus.PrometheusReporter` - this exposes a Prometheus read endpoint at port 9095 by default.  Easily connect this to a Prometheus server, or feed the metrics back into FiloDB itself or many other sinks using Influx [Telegraf](https://github.com/influxdata/telegraf)
* `kamon.zipkin.ZipkinReporter` - reports trace spans to a Zipkin server
* `filodb.coordinator.KamonMetricsLogReporter` - this is part of the coordinator module and will log all metrics (including segment trace metrics) at every Kamon tick interval, which defaults to 10 seconds.  Which metrics to log including pattern matching on names can be configured.
* `filodb.coordinator.KamonSpanLogReporter` - logs traces.  Super useful to debug timing and flow for queries and chunk writes.  Logging every trace might be really expensive; you can toggle tracing probability via the `kamon.trace` config section - see `conf/timeseries-filodb-server.conf`.

### Metrics Configuration

Kamon has many configurable options.  To get more detailed traces on the write / segment append path, for example, here is how you might pass to `spark-submit` or `spark-shell` options to set detailed tracing on and to trace 3% of all segment appends:

    --driver-java-options '-XX:+UseG1GC -XX:MaxGCPauseMillis=500 -Dkamon.trace.level-of-detail=simple-trace -Dkamon.trace.random-sampler.chance=3'

To change the metrics flush interval, you can set `kamon.metric.tick-interval` and `kamon.statsd.flush-interval`.  The statsd flush-interval must be equal to or greater than the tick-interval.

Methods of configuring Kamon (except for the metrics logger):

- The best way to configure Kamon is to pass this Java property: `-Dkamon.config-provider=filodb.coordinator.KamonConfigProvider`.  This lets you configure Kamon through the same mechanisms as the rest of FiloDB: `-Dfilo.config.file` for example, and the configuration is automatically passed to each executor/worker.  Otherwise:
- Passing Java options on the command line with `-D`, or for Spark, `--driver-java-options` and `--executor-java-options`
- Passing options in a config file and using `-Dconfig.file`.  NOTE: `-Dfilo.config.file` will not work because Kamon uses a different initialization stack. Need to be done for both drivers and executors.

## Code Walkthrough

Please go to the [architecture](doc/architecture.md) doc.

## Building and Testing

Run the tests with `sbt test`, or for continuous development, `sbt ~test`.  Noisy cassandra logs can be seen in `filodb-test.log`.

The docs use [mermaid](https://github.com/knsv/mermaid) and [doctoc](https://github.com/thlorenz/doctoc).  On a Mac, to install:

    brew install yarn
    yarn global add mermaid.cli
    yarn global add doctoc

Multi-JVM tests output a separate log file per process, in the `logs` dir under `multijvm-nodeN-test.log`.

Some useful environment vars:
* `LOG_AKKA_TO_CONSOLE` - define this to have noisy Akka Cluster logs output to the console
* `MAYBE_MULTI_JVM` - enable multi-JVM tests for the Kafka and Standalone modules.  These require both Cassandra and Kafka to be up and running.

### Debugging serialization and queries

Right now both Java and Kryo serialization are used for Akka messaging.  Kryo is used for query result serialization.  If there are mysterious hangs, or other potentially serialization-related bugs, here is where to investigate:

1. Run the `SerializationSpec` in coordinator.client module, especially if changes have been done to the Akka configuration.  This test uses the Akka serialization module to ensure settings and serializers work correctly.
2. Set `MAYBE_MULTI_JVM` to true and run `cassandra/test` and `standalone/test`. They test multi-node communication for both ingestion and querying.
3. Set the following log levels to trace queries and serialization in detail:
    - `filodb.coordinator.queryengine` to DEBUG - especially useful to see how `ExecPlans` get distributed
    - `com.esotericsoftware.minlog` to DEBUG or TRACE -- detailed overall serialization
    - `com.esotericsoftware.kryo.io` to TRACE - detailed `BinaryRecord` / `BinaryVector` serde debugging 
4. In particular, enable the above when running the CLI with the standalone FiloDB process to do PromQL queries.

To dynamically change the log level, you can use the `/admin/loglevel` HTTP API (per host).  Example:

    curl -d 'trace' http://localhost:8080/admin/loglevel/com.esotericsoftware.minlog

### Other debugging tips

To debug raw record ingestion and data mismatches:

* Set the `trace-filters` source config setting... see `timeseries-dev-source.conf` and `TimeSeriesShard` `tracedPartFilters`.  This will log every sample for time series matching criteria set in trace-filters.
* Use the filtering capability in `filodb.kafka.TestConsumer` to print out raw records received from Kafka.

### Benchmarking

To run benchmarks, from within SBT:

    cd jmh
    jmh:run -i 5 -wi 5 -f3

Typically, one might run a specific benchmark.  This is how we run the query benchmark, with options for inlining and maximizing performance:

    jmh:run -i 15 -wi 10 -f3 -jvmArgsAppend -XX:MaxInlineLevel=20 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 filodb.jmh.QueryInMemoryBenchmark

You can get the huge variety of JMH options by running `jmh:run -help`.  For good profiling, there are options such as `-prof jmh.extras.JFR` as well as `perfasm` / `dtraceasm` options.  If you would like really good profiling analysis, including memory/heap allocation, I would suggest running one fork with many more iterations, like this:

    jmh:run -i 1000 -wi 10 -f1 -jvmArgsAppend -XX:MaxInlineLevel=20 -jvmArgsAppend -Xmx4g -jvmArgsAppend -XX:MaxInlineSize=99 filodb.jmh.QueryInMemoryBenchmark

This should last a good 15 minutes at least.  While it is running, fire up JMC (java Mission Control) and flight record the "jmh.ForkMain" process for 15 minutes.  This gives you excellent CPU as well as memory allocation analysis.

Another good option is generating a FlameGraph:  `-prof jmh.extras.Async:dir=/tmp/filodbprofile`. Be sure to read the instructions for setting up FlameGraph profiling.  You can also run a stack profiler with an option like ` -prof stack:lines=4;detailLine=true`, but the analysis is not as good as JMC or Async Profiler/FlameGraph/

There is also a script, `run_benchmarks.sh`

For running basic continuous profiling in a test environment, a simple profiler can be enabled. It periodically writes a report of the top called methods, as a percentage over a sampling interval. Methods which simply indicate that threads are blocked are excluded. See the profiler section in the filodb-defaults.conf file, and copy this section to a local configuration file.

## You can help!

Contributions are welcome!
