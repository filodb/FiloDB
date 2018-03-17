package filodb.core.query

import com.typesafe.config.ConfigFactory
import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.memstore.{FlushStream, TimeSeriesMemStore}
import filodb.core.metadata.Column
import filodb.core.store._

class ExecPlanSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global

  import MachineMetricsData._
  import Column.ColumnType._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore())
  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  after {
    memStore.reset()
    memStore.sink.reset()
    memStore.metastore.clearAllData()
  }

  val partKeys = (0 to 9).map(n => dataset1.partKey(s"Series $n"))

  it("LocalVectorReader should return result with vectors and proper schema when execute") {
    memStore.setup(dataset1, 0)
    val data = records(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val plan = new ExecPlan.LocalVectorReader(Seq(1), FilteredPartitionScan(split), AllChunkScan)
    plan.executeToResult(memStore, dataset1).runAsync.futureValue match {
      case r @ VectorListResult(range, schema, vectorList) =>
        range shouldEqual None
        schema shouldEqual ResultSchema(Seq(ColumnInfo("min", DoubleColumn)), 0)
        vectorList should have length (10)
        vectorList.map(_.info.get.partKey) shouldEqual partKeys
        vectorList.map(_.readers.length) shouldEqual Seq.fill(10)(1)
        vectorList.map(_.readers.head.vectors.size) shouldEqual Seq.fill(10)(1)

        r.toRowReaders.toSeq.map(_._1.get.partKey) shouldEqual partKeys
        r.toRowReaders.toSeq.flatMap(_._2.map(_.getDouble(0))).take(6) shouldEqual Seq(1.0, 11.0, 2.0, 12.0, 3.0, 13.0)
      case other: Any =>
        throw new RuntimeException(s"Should not have gotten $other")
    }

    plan.chunkMethod shouldEqual AllChunkScan
    plan.schema(dataset1) shouldEqual ResultSchema(Seq(ColumnInfo("min", DoubleColumn)), 0)
  }

  it("LocalVectorReader should throw error if key range and don't pass in row key columns") {
    val keyRange = RowKeyChunkScan(dataset1, Seq(105000L), Seq(115000L))
    memStore.setup(dataset1, 0)
    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val plan = new ExecPlan.LocalVectorReader(Seq(1, 2), FilteredPartitionScan(split), keyRange)
    intercept[IllegalArgumentException] {
      plan.executeToResult(memStore, dataset1).runAsync.futureValue
    }
  }

  it("LocalVectorReader and VectorListResult should limit records to key range") {
    // This will result in dropping first sample from series 0..4, keeping
    // both samples of series 5, and dropping last sample from series 6..9
    val keyRange = RowKeyChunkScan(dataset1, Seq(105000L), Seq(115000L))
    val schemaCols = Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min",DoubleColumn))

    memStore.setup(dataset1, 0)

    // Ingest and flush.  We force flushing because right now we cannot range scan the write buffer.
    val stream = Observable.fromIterable(records(linearMultiSeries()).take(20).grouped(5).toSeq)
      .executeWithModel(BatchedExecution(5))
    val flushStream = FlushStream.everyN(4, 5, stream)
    memStore.ingestStream(dataset1.ref, 0, stream, flushStream)(ex => throw ex).futureValue

    Thread sleep 300

    val split = memStore.getScanSplits(dataset1.ref, 1).head
    val plan = new ExecPlan.LocalVectorReader(Seq(0, 1), FilteredPartitionScan(split), keyRange)
    plan.executeToResult(memStore, dataset1).runAsync.futureValue match {
      case r @ VectorListResult(range, schema, vectorList) =>
        range shouldEqual Some((keyRange.startkey, keyRange.endkey))
        schema shouldEqual ResultSchema(schemaCols, 1)
        vectorList should have length (10)
        vectorList.map(_.info.get.partKey).toSet shouldEqual partKeys.toSet   // order of partitions not guaranteed
        // vectorList.map(_.readers.length) shouldEqual Seq.fill(10)(1)
        vectorList.map(_.readers.head.vectors.size) shouldEqual Seq.fill(10)(2)

        r.toRowReaders.toSeq.map(_._1.get.partKey).toSet shouldEqual partKeys.toSet
        r.toRowReaders.toSeq.flatMap(_._2.map(_.getDouble(1))).take(8).toSet shouldEqual Set(
                                            11.0, 12.0, 13.0, 14.0, 15.0, 6.0, 16.0, 7.0)
      case other: Any =>
        throw new RuntimeException(s"Should not have gotten $other")
    }

    plan.chunkMethod shouldEqual keyRange
    plan.schema(dataset1) shouldEqual ResultSchema(schemaCols, 1)
  }

  it("streamLastTuplePlan should return plan which executes returns TupleListResult") {
    memStore.setup(dataset1, 0)
    val data = records(linearMultiSeries()).take(30)   // 3 records per series x 10 series
    memStore.ingest(dataset1.ref, 0, data)

    val keys = (2 to 4).map(n => dataset1.partKey(s"Series $n"))
    val plan = ExecPlan.streamLastTuplePlan(dataset1, Seq(0, 1), MultiPartitionScan(keys))
    println(plan.toString)

    plan.chunkMethod shouldEqual LastSampleChunkScan
    plan.executeToResult(memStore, dataset1).runAsync.futureValue match {
      case r @ TupleListResult(schema, tuples) =>
        schema shouldEqual ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn)), 1)
        tuples should have length (3)
        tuples.map(_.info.get.partKey) shouldEqual keys
        tuples.map(_.data.getDouble(1)) shouldEqual Seq(23.0, 24.0, 25.0)
        tuples.map(_.data.isEmpty) shouldEqual Seq(false, false, false)

        r.toRowReaders.toSeq.map(_._1.get.partKey) shouldEqual keys
        r.toRowReaders.toSeq.flatMap(_._2.map(_.getDouble(1))) shouldEqual Seq(23.0, 24.0, 25.0)
      case other: Any =>
        throw new RuntimeException(s"Should not have gotten $other")
    }
  }
}