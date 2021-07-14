<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Ingestion](#ingestion)
    - [Kafka Ingestion](#kafka-ingestion)
      - [Basic Configuration](#basic-configuration)
      - [Kafka Message Format](#kafka-message-format)
    - [Testing the Consumer](#testing-the-consumer)
    - [Memory Configuration](#memory-configuration)
  - [Recovery and Persistence](#recovery-and-persistence)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB

## Ingestion
FiloDB allows multiple topics/streams to be ingested simultaneously.  Each stream may have different source configuration as well as MemStore / memory / chunking configuration.

The main ingestion source is likely to be Apache Kafka, though it is possible to implement any custom ingestion source by implementing the `filodb.coordinator.IngestionStream` and `IngestionStreamFactory` traits.

### Kafka Ingestion
Each FiloDB dataset/stream is sharded, with one shard equal to a Kafka partition, and each stream corresponding to a different Kafka topic.  In fact each stream can be configured to come from different brokers and have different store characteristics.  For example:

* Dataset A - high value aggregate time series:  topic aggregates, flush every hour, 500M storage per shard
* Dataset B - lower importance time series, higher volume:  topic prod_all, flush every 3 hours, 2GB storage per shard

#### Basic Configuration
Create your custom source config file, e.g. [example-source.conf](../kafka/src/main/resources/example-source.conf)

NOTE: for the latest and most up to date, see [timeseries-dev-source.conf](../conf/timeseries-dev-source.conf).

```yaml
dataset = "example"

# Name of schema used for this dataset stream.  See filodb.schemas in filodb-defaults or any other server conf
schema = "prom-counter"

# e.g. one shard per kafka partition
num-shards = 100
min-num-nodes = 5
chunk-duration = 1 hour
sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"
sourceconfig {
  # Required FiloDB Kafka settings
  filo-topic-name = "topics.topic1"

  # Custom client configurations
  my.custom.client.config = "custom.value"

  # And any standard kafka configurations, e.g.
  # This accepts both the standard kafka value of a comma-separated
  # string and a Typesafe list of String values
  # EXCEPT: do not populate value.deserializer, as the Kafka format is fixed in FiloDB to be messages of RecordContainer's
  bootstrap.servers = "host1:port,host2:port"
  auto.offset.reset=latest
  # optional etc.
  partitioner.class = "com.example.OptionalCustomPartitioner"
  group.id = "org.example.cluster1.filodb.consumer1"

  # Values controlling in-memory store chunking, flushing, etc.
  store {
    # Interval it takes to flush ALL time series in a shard.  This time is further divided by groups-per-shard
    flush-interval = 1h

    # TTL for on-disk / C* data.  Data older than this may be purged.
    disk-time-to-live = 3 days

    max-chunks-size = 500

    # Number of bytes of offheap mem to allocate to chunk storage in each shard.  Ex. 1000MB, 1G, 2GB
    # Assume 5 bytes per sample, should be roughly equal to (# samples per time series) * (# time series)
    shard-mem-size = 256MB

    # Number of bytes of offheap mem to allocate to write buffers for all shards.  Ex. 1000MB, 1G, 2GB
    ingestion-buffer-mem-size = 200MB

    # Number of subgroups within each shard.  Persistence to a ChunkSink occurs one subgroup at a time, as does
    # recovery from failure.  This many batches of flushes must occur to cover persistence of every partition
    groups-per-shard = 60
  }

}
```

#### Kafka Message Format

Each Kafka Message is expected to be a [RecordContainer](../core/src/main/scala/filodb.core/binaryrecord2/RecordContainer.scala), which is a byte container for multiple [BinaryRecords](binaryrecord-spec.md).  See the previous link for more details about this format.  It is an efficient format which supports different dataset schemas.

- Each Kafka topic must have RecordContainers encoded using the SAME RecordSchema
- One topic per dataset ingested
- The `KafkaIngestionStream` enforces the use of the built-in `RecordContainerDeserializer` as that is expected to be the format

In the future converters to BinaryRecord from standard formats will be provided.

You can also look at [SourceSinkSuite.scala](../kafka/src/it/scala/filodb/kafka/SourceSinkSuite.scala).

### Testing the Consumer

* `sbt standalone/assembly`
* `java -cp standalone/target/scala-2.12/standalone-assembly-0.7.0.jar filodb.kafka.TestConsumer my-kafka-sourceconfig.conf`

See the TestConsumer for more info.

### Memory Configuration

How much to allocate for `ingestion-buffer-mem-size` and `shard-mem-size` as well as heap?  Here are some guidelines:

* **Heap memory** - heap usage grows by the number of time series stored by FiloDB in memory, but not by the number of chunks or amount of data within each series.  As of 8/6/18 1.5 million time series will fit within 1GB of heap.  At least 5-10 more GB is recommended though for extra memory for ingestion, recovery, and querying.
* **Ingestion buffer** - The ingestion buffer is a per-dataset offheap memory area for ingestion write buffers and some other time series-specific data structures.  It needs to be scaled with the number of time series actively ingesting in the system, a few KB for each series.  Once the ingestion buffer runs out, no more time series can be added and eviction of existing time series starting with the oldest non-actively ingesting time series will begin to free up room.  If not enough room can be freed, new time series and in extreme cases even new data may not be ingested.
* `shard-mem-size` - this is the offheap block storage used to store encoded chunks for the time series data samples and metadata for each chunk.  This should be sized for the number of time series as well as the length of retention desired for all the time series.  The configuration is currently **per-shard**.  When this memory runs out, the oldest blocks will be reclaimed automatically and those chunks will be dropped from time series.

## Recovery and Persistence

Datasets are divided into shards.  One shard must wholly fit into one node/process.  Within each shard, individual time series or "partitions" are further grouped into sub-groups.  The number of groups per shard is configurable.

Data in a shard are flushed one sub-group at a time, on a rotating basis.  When it is time for a sub-group to be flushed, the write buffers for each time series is then optimized into an immutable chunk and the chunks and a checkpoint is written to a (hopefully) persistent ChunkSink.  A watermark for that sub-group then determines up to which ingest offset data has been persisted (thus, which offset one needs to perform recovery from).

Recovery upon start of ingestion is managed by the coordinator's `IngestionActor`.  The process goes like this:

 1. StartShardIngestion command is received and start() called
 2. MemStore.setup() is called for that shard
 3. IF no checkpoint data is found, THEN normal ingestion is started
 4. IF checkpoints are found, then recovery is started from the minimum checkpoint offset
    and goes until the maximum checkpoint offset.  These offsets are per subgroup of the shard.
    Progress will be sent at regular intervals
 5. Once the recovery has proceeded beyond the end checkpoint then normal ingestion is started

The Lucene index for the time series are also recovered.  See [indexing](indexing.md) for more details.

Checking on the shard status (possible via CLI right now, perhaps via HTTP in future) will yield a recovery status first with progress %, then after recovery is done, the status will revert back to Normal.


