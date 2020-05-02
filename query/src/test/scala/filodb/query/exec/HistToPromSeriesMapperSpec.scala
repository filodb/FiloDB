package filodb.query.exec

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.{MachineMetricsData => MMD}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => ZCUTF8}
import filodb.memory.format.vectors.HistogramWithBuckets

class HistToPromSeriesMapperSpec extends FunSpec with Matchers with ScalaFutures {
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  import monix.execution.Scheduler.Implicits.global

  val eightBucketData = MMD.linearHistSeries().take(20)
  val rvKey = CustomRangeVectorKey(eightBucketData.head(5).asInstanceOf[Map[ZCUTF8, ZCUTF8]] +
                                   (ZCUTF8("metric") -> ZCUTF8(eightBucketData.head(4).asInstanceOf[String])))
  val eightBTimes = eightBucketData.map(_(0).asInstanceOf[Long])
  val eightBHists = eightBucketData.map(_(3).asInstanceOf[HistogramWithBuckets])
  val rows = eightBTimes.zip(eightBHists).map { case (t, h) => new TransientHistRow(t, h) }
  val sourceSchema = new ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                          ColumnInfo("value", ColumnType.HistogramColumn)), 1)

  it("should convert single schema histogram to appropriate Prom bucket time series") {
    val rv = IteratorBackedRangeVector(rvKey, rows.toIterator)

    val mapper = HistToPromSeriesMapper(MMD.histDataset.schema.partition)
    val sourceObs = Observable.now(rv)

    mapper.schema(sourceSchema).columns shouldEqual Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                                        ColumnInfo("value", ColumnType.DoubleColumn))

    val destObs = mapper.apply(sourceObs, queryConfig, 1000, sourceSchema, Nil)
    val destRvs = destObs.toListL.runAsync.futureValue

    // Should be 8 time series since there are 8 buckets
    destRvs.length shouldEqual 8

    // RVs should have same timestamps as source data and expected bucket values.  Also check keys
    destRvs.foreach { rv =>
      val kvMap = rv.key.labelValues.map { case (k, v) => k.toString -> v.toString }
      kvMap.contains("le") shouldEqual true
      kvMap("metric").endsWith("_bucket") shouldEqual true
      val le = kvMap("le").toDouble
      rv.rows.map(_.getLong(0)).toSeq shouldEqual eightBTimes

      // Figure out bucket number and extract bucket values for comparison
      var bucketNo = 0
      while (eightBHists.head.bucketTop(bucketNo) < le) bucketNo += 1
      rv.rows.map(_.getDouble(1)).toSeq shouldEqual eightBHists.map(_.bucketValue(bucketNo))
    }
  }

  val tenBucketData = MMD.linearHistSeries(startTs = 150000L, numBuckets = 10).take(10)
  val tenBTimes = tenBucketData.map(_(0).asInstanceOf[Long])
  val tenBHists = tenBucketData.map(_(3).asInstanceOf[HistogramWithBuckets])
  val tenRows = tenBTimes.zip(tenBHists).map { case (t, h) => new TransientHistRow(t, h) }

  it("should convert multiple schema histograms to Prom bucket time series") {
    val rv = IteratorBackedRangeVector(rvKey, (rows ++ tenRows).toIterator)

    val mapper = HistToPromSeriesMapper(MMD.histDataset.schema.partition)
    val sourceObs = Observable.now(rv)

    mapper.schema(sourceSchema).columns shouldEqual Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                                        ColumnInfo("value", ColumnType.DoubleColumn))

    val destObs = mapper.apply(sourceObs, queryConfig, 1000, sourceSchema, Nil)
    val destRvs = destObs.toListL.runAsync.futureValue

    // Should be 10 time series since there are up to 10 buckets
    destRvs.length shouldEqual 10

    // Buckets that should have data for all timestamps: 2,4,8,16,32,64,128,256
    // Buckets that should have NaN for first 20 timestamps: 512,1024
    destRvs.foreach { rv =>
      val kvMap = rv.key.labelValues.map { case (k, v) => k.toString -> v.toString }
      kvMap.contains("le") shouldEqual true
      kvMap("metric").endsWith("_bucket") shouldEqual true
      val le = kvMap("le").toDouble
      rv.rows.map(_.getLong(0)).toSeq shouldEqual (eightBTimes ++ tenBTimes)

      // Figure out bucket number and extract bucket values for comparison
      var bucketNo = 0
      while (tenBHists.head.bucketTop(bucketNo) < le) bucketNo += 1

      val bucketValues = rv.rows.map(_.getDouble(1)).toSeq
      if (bucketNo < 8) {
        bucketValues take 20 shouldEqual eightBHists.map(_.bucketValue(bucketNo))
      } else {
        (bucketValues take 20).forall(java.lang.Double.isNaN) shouldEqual true
      }
      bucketValues drop 20 shouldEqual tenBHists.map(_.bucketValue(bucketNo))
    }
  }
}