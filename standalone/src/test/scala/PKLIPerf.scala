import java.lang.management.{BufferPoolMXBean, ManagementFactory}

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable
import spire.syntax.cfor.cforRange
import scala.concurrent.duration._
import scala.concurrent.Await

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.Schemas.untyped
import filodb.core.DatasetRef
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.timeseries.TestTimeseriesProducer

object PKLIPerf extends App with StrictLogging {
  println(s"Building Part Keys")
  val ref = DatasetRef("prometheus")
  val partKeyIndex = new PartKeyLuceneIndex(ref, untyped.partition, true, true,0, 1.hour.toMillis)
  val numSeries = 1000000
  val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)
  val untypedData = TestTimeseriesProducer.timeSeriesData(0, numSeries) take numSeries
  untypedData.foreach(_.addToBuilder(ingestBuilder))

  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)

  val converter = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = untyped.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
  }
  // Build part keys from the ingestion records
  ingestBuilder.allContainers.foreach(_.consumeRecords(converter))

  var partId = 1
  val now = System.currentTimeMillis()
  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = {
      val partKey = untyped.partition.binSchema.asByteArray(base, offset)
      partKeyIndex.addPartKey(partKey, partId, now)()
      partId += 1
    }
  }

  val start = System.nanoTime()

  println(s"Indexing started")
  partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))
  partKeyIndex.refreshReadersBlocking()
  val end = System.nanoTime()

  println(s"Indexing finished. Added $partId part keys Took ${(end-start)/1000000000L}s")
  import scala.collection.JavaConverters._

  println(s"Index Memory Map Size: " +
    s"${ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala.find(_.getName == "mapped").get.getMemoryUsed}")

  /**
   * Should not use Scala or Monix's default Global Implicit since it does
   * not have a configurable uncaught exception handler.
   */
  implicit lazy val globalImplicitScheduler = Scheduler.computation(
    name = "global-implicit",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in GlobalScheduler", _)))

  val fut = Observable.repeat(1).mapParallelUnordered(Runtime.getRuntime.availableProcessors()) { i =>
    Task.eval {
      cforRange(0 until 8) { i =>
        val filter = Seq(
          ColumnFilter("_ws_", Filter.Equals("demo")))
        partKeyIndex.labelValuesEfficient(filter, now, now + 1000, "_metric_", 10000)
      }
    }
  }.completedL.runToFuture

  Await.result(fut, 5.hours)
}
