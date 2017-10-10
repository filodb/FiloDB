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
columns = ["one", "two", "three"]
# e.g. one shard per kafka partition
numshards = 100
min-num-nodes = 5
sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"
sourceconfig {
  filo-topic-name = "topics.topic1"
  filo-kafka-servers = ["host1:port", "host2:port"]
  filo-record-converter = "com.example.CustomRecordConverter"
}
# And any standard kafka configurations, e.g.
auto.offset.reset=latest
value.deserializer = "com.example.CustomKafkaDeserializer"
partitioner.class = "com.example.OptionalCustomPartitioner"
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

