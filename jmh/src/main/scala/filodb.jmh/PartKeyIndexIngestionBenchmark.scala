package filodb.jmh

import scala.collection.mutable.ArrayBuffer

import org.openjdk.jmh.annotations.{Level, Param, Scope, Setup, State, TearDown}
import org.openjdk.jmh.annotations
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import spire.syntax.cfor.cforRange

import filodb.core.memstore.{PartKeyIndexRaw, PartKeyLuceneIndex, PartKeyTantivyIndex}
import filodb.core.metadata.Schemas.untyped
import filodb.memory.BinaryRegionConsumer

// scalastyle:off
@State(Scope.Benchmark)
abstract class PartKeyIndexIngestionBenchmark extends PartKeyIndexBenchmark {
  // How many part keys are added / removed per second
  final val itemsPerSecond = 10
  // How often do we commit to disk / refresh readers
  final val commitWindowInSeconds = 30

  // 0 days, 1 day, 5 days, 30 days
  @Param(Array("0", "1", "5", "30"))
  var durationInDays: Long = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    println(s"Simulating $durationInDays days of churn")
    val start = System.nanoTime()
    val churnSteps = ((durationInDays * 24 * 60 * 60) / commitWindowInSeconds).toInt
    var partId = 0
    val itemsPerStep = commitWindowInSeconds * itemsPerSecond

    val partKeys = new ArrayBuffer[Array[Byte]]()
    val consumer = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = {
        val partKey = untyped.partition.binSchema.asByteArray(base, offset)
        partKeys += partKey
      }
    }
    partKeyBuilder.allContainers.foreach(_.consumeRecords(consumer))

    cforRange ( 0 until churnSteps ) { _ =>
      cforRange ( 0 until itemsPerStep ) { _ =>
        val partKey = partKeys(partId)
        // When we ingested we used 1 based part IDs, not 0 based
        partKeyIndex.upsertPartKey(partKey, partId+1, now)()

        partId += 1
        partId = partId % numSeries
      }
    }

    val end = System.nanoTime()
    println(s"Finished ingesting new changes. Took ${(end-start)/1000000000L}s")

    partKeyIndex.refreshReadersBlocking()
    val end2 = System.nanoTime()
    println(s"Churning finished. Took ${(end2-start)/1000000000L}s")
    println(s"New Index Memory Map Size: ${partKeyIndex.indexMmapBytes}")
    println(s"Doc count: ${partKeyIndex.indexNumEntries}")
  }
}

@State(Scope.Benchmark)
class PartKeyLuceneIndexIngestionBenchmark extends PartKeyIndexIngestionBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    new PartKeyLuceneIndex(ref, untyped.partition, true, true, 0, 1.hour.toMillis)
  }
}

@State(Scope.Benchmark)
class PartKeyTantivyIndexIngestionBenchmark extends PartKeyIndexIngestionBenchmark {
  override protected def createPartKeyIndex(): PartKeyIndexRaw = {
    PartKeyTantivyIndex.startMemoryProfiling()

    new PartKeyTantivyIndex(ref, untyped.partition, 0, 1.hour.toMillis)
  }

  @TearDown(annotations.Level.Trial)
  def teardown(): Unit = {
    PartKeyTantivyIndex.stopMemoryProfiling()
    val index = partKeyIndex.asInstanceOf[PartKeyTantivyIndex]

    println(s"\nCache stats:\n${index.dumpCacheStats()}\n")
  }
}