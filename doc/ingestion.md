<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Ingestion](#ingestion)
    - [Kafka Ingestion](#kafka-ingestion)
      - [Basic Configuration](#basic-configuration)
      - [Sample Code](#sample-code)
  - [Recovery and Persistence](#recovery-and-persistence)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB

## Ingestion
FiloDB allows multiple topics/streams to be ingested simultaneously.

### Kafka Ingestion
Each dataset has multiple streams, one per shard.
The config to be passed in defines the Kafka broker, topic, and other settings, one config per dataset/stream.

#### Basic Configuration
Create your custom source config file, e.g. [example-source.conf](../kafka/src/main/resources/example-source.conf)

```yaml
dataset = "example"
# e.g. one shard per kafka partition
numshards = 100
min-num-nodes = 5
chunk-duration = 1 hour
sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"
sourceconfig {
  filo-topic-name = "topics.topic1"
  filo-kafka-servers = ["host1:port", "host2:port"]
  filo-record-converter = "com.example.CustomRecordConverter"
  # And any standard kafka configurations, e.g.
  # auto.offset.reset=latest
  # value.deserializer = "com.example.CustomKafkaDeserializer"
  # partitioner.class = "com.example.OptionalCustomPartitioner"
}
```
The [defaults](../kafka/src/main/resources/filodb-defaults.conf) you can override and see
[all configuration here](../kafka/src/main/resources).

#### Sample Code
Users must provide three basic implementations to FiloDB for Kafka ingestion
* A custom Kafka `RowReader` for this data type, or use an existing one for simpler data types, e.g. `org.velvia.filo.ArrayStringRowReader`
* A custom Kafka [RecordConverter](../kafka/src/main/scala/filodb/kafka/RecordConverter.scala)
to convert a your event data type to a FiloDB sequence of `IngestRecord` instances
    - [A simple converter for String types](../kafka/src/main/scala/filodb/kafka/RecordConverter.scala#L30-L38)
* A custom Kafka Deserializer - example using the provided helper [KafkaSerdes](../kafka/src/main/scala/filodb/kafka/KafkaSerdes.scala)

```scala
import filodb.kafka.KafkaSerdes
import org.apache.kafka.common.serialization.{Serializer, Deserializer}

/* For FiloDB to consume and transform the stream of custom records to ingest. */
final class DataTDeserializer extends Deserializer[DataT] with KafkaSerdes {
  override def deserialize(topic: String, bytes: Array[Byte]): DataT = DataT.from(bytes)
}

/* Not needed for FiloDB but you can create like this for your producer side with KafkaSerdes as well. */
final class DataTSerializer extends Serializer[DataT] with KafkaSerdes {
  override def serialize(topic: String, data: DataT): Array[Byte] = data.toByteArray
}
```

A simple test

```scala
implicit val timeout: Timeout = 10.seconds
implicit val io = Scheduler.io("example-scheduler")

val settings = new KafkaSettings(sourceConfig)

val producer = PartitionedProducerSink.create[String, String](settings, io)
val consumer = PartitionedConsumerObservable.create(settings, tps.head).executeOn(io)
val key = JLong.valueOf(0L)
val count = 10000

val pushT = Observable.range(0, count)
  .map(msg => new ProducerRecord(settings.IngestionTopic, key, msg.toString))
  .bufferIntrospective(1024)
  .consumeWith(producer)

val streamT = consumer
  .map(record => (record.offset, record.value.toString))
  .toListL

val task = Task.zip2(Task.fork(streamT), Task.fork(pushT)).runAsync
```

You can also look at [SourceSinkSuite.scala](../kafka/src/it/scala/filodb/kafka/SourceSinkSuite.scala).

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


