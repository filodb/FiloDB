<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Ingestion](#ingestion)
    - [Kafka Ingestion](#kafka-ingestion)
      - [Basic Configuration](#basic-configuration)
      - [Kafka Message Format](#kafka-message-format)
  - [Testing the Consumer](#testing-the-consumer)
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
    flush-interval = 2 minutes

    # TTL for on-disk / C* data.  Data older than this may be purged.
    disk-time-to-live = 3 days

    # amount of time paged chunks should be retained in memory
    demand-paged-chunk-retention-period = 72 hours

    max-chunks-size = 500

    # Fixed amount of memory, in MBs, to allocate for encoded chunks per shard
    shard-memory-mb = 512

    # Max # of partitions or time series the WriteBufferPool can allocate and that can be ingested at a time
    max-num-partitions = 100000

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

## Testing the Consumer

* `sbt standalone/assembly`
* `java -cp standalone/target/scala-2.11/standalone-assembly-0.7.0.jar filodb.kafka.TestConsumer my-kafka-sourceconfig.conf`

See the TestConsumer for more info.

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

Checking on the shard status (possible via CLI right now, perhaps via HTTP in future) will yield a recovery status first with progress %, then after recovery is done, the status will revert back to Normal.


