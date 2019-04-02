package filodb.kafka

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler

import filodb.coordinator.{FilodbSettings, IngestionStreamFactory, StoreFactory}
import filodb.core.memstore.SomeData
import filodb.core.store.IngestionConfig

/**
 * A simple app which uses a sourceconfig of your choice to test reading
 * data from Kafka (or whatever configured source factory) and test reading from certain offsets.
 * It reads dataset definition from MetaStore, so please pass the server.conf with Cassandra/metastore details.
 *
 * To launch: java -Xmx4G -Dconfig.file=conf/timeseries-filodb-server.conf \
 *                 -cp <path>/standalone-assembly-0.7.0.jar filodb.kafka.TestConsumer  \
 *                    my-kafka-sourceconfig.conf <partition#>
 * It will read 10 records and then quit, printing out the offsets of each record.
 * Optional: pass in a second arg which is the offset to seek to.
 */
object TestConsumer extends App {
  val settings = new FilodbSettings()
  val storeFactory = StoreFactory(settings, Scheduler.io())

  val sourceConfPath = args(0)
  val offsetOpt = args.drop(1).headOption.map(_.toLong)
  val shard = if (args.length > 1) args(1).toInt else 0

  val sourceConf = ConfigFactory.parseFile(new java.io.File(sourceConfPath))
  //scalastyle:off
  println(s"TestConsumer starting with shard $shard, config $sourceConf\nand offset $offsetOpt")

  import monix.execution.Scheduler.Implicits.global

  val ingestConf = IngestionConfig(sourceConf, classOf[KafkaIngestionStreamFactory].getClass.getName).get
  val dataset = Await.result(storeFactory.metaStore.getDataset(ingestConf.ref), 30.seconds)

  val ctor = Class.forName(ingestConf.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]

  val stream = streamFactory.create(sourceConf, dataset, shard, offsetOpt)
  val fut = stream.get.take(10)
                  .foreach { case SomeData(container, offset) =>
                    println(s"\n----- Offset $offset -----")
                    container.foreach { case (base, offset) =>
                      println(s"   ${dataset.ingestionSchema.stringify(base, offset)}")
                    }
                  }
  Await.result(fut, 10.minutes)
}