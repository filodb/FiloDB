package filodb.kafka

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import filodb.coordinator.IngestionStreamFactory
import filodb.core.memstore.SomeData
import filodb.core.metadata.Dataset
import filodb.core.store.IngestionConfig

/**
 * A simple app which uses a sourceconfig of your choice to test reading
 * data from Kafka (or whatever configured source factory) and test reading from certain offsets.
 *
 * To launch: java -Xmx4G -cp <path>/standalone-assembly-0.7.0.jar filodb.kafka.TestConsumer  \
 *                    my-kafka-sourceconfig.conf
 * It will read 10 records and then quit, printing out the offsets of each record.
 * Optional: pass in a second arg which is the offset to seek to.
 */
object TestConsumer extends App {
  val sourceConfPath = args(0)
  val offsetOpt = args.drop(1).headOption.map(_.toLong)

  val sourceConf = ConfigFactory.parseFile(new java.io.File(sourceConfPath))
  //scalastyle:off
  println(s"TestConsumer starting with config $sourceConf\nand offset $offsetOpt")

  import monix.execution.Scheduler.Implicits.global

  // For now, hard code dataset to a Prometheus like dataset
  // TODO: allow specification of dataset, then load from the MetaStore
  val dataset = Dataset("prometheus", Seq("tags:map"), Seq("timestamp:long", "value:double"))

  val ingestConf = IngestionConfig(sourceConf, classOf[KafkaIngestionStreamFactory].getClass.getName).get
  val datasetRef = ingestConf.ref
  val ctor = Class.forName(ingestConf.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]

  val stream = streamFactory.create(sourceConf, dataset, 0, offsetOpt)
  val fut = stream.get.take(10)
                  .foreach { case SomeData(container, offset) =>
                    println(s"\n----- Offset $offset -----")
                    container.foreach { case (base, offset) =>
                      println(s"   ${dataset.ingestionSchema.stringify(base, offset)}")
                    }
                  }
  Await.result(fut, 10.minutes)
}