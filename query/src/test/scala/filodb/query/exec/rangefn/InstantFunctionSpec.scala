package filodb.query.exec.rangefn

import scala.util.Random

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.{MachineMetricsData => MMD, MetricsTestData}
import filodb.core.query.{CustomRangeVectorKey, RangeVector, RangeVectorKey, ResultSchema}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String, vectors => bv}
import filodb.query._
import filodb.query.exec.TransientRow

class InstantFunctionSpec extends RawDataWindowingSpec with ScalaFutures {

  val resultSchema = ResultSchema(MetricsTestData.timeseriesDataset.infosFromIDs(0 to 1), 1)
  val histSchema = ResultSchema(MMD.histDataset.infosFromIDs(Seq(0, 3)), 1)
  val histMaxSchema = ResultSchema(MMD.histMaxDS.infosFromIDs(Seq(0, 4, 3)), 1, colIDs=Seq(0, 4, 3))
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))
  val sampleBase: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(3L, 3239.3423d),
        new TransientRow(4L, 94935.1523d)).iterator
    })
  val rand = new Random()
  val error = 0.00000001d

  it("should work with instant function mapper") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array.fill(100)(new RangeVector {
      val data: Stream[TransientRow] = Stream.from(0).map { n =>
        new TransientRow(n.toLong, rand.nextDouble())
      }.take(20)

      override def key: RangeVectorKey = ignoreKey

      override def rows: Iterator[RowReader] = data.iterator
    })
    fireInstantFunctionTests(samples)
  }

  it ("should handle NaN") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, Double.NaN),
          new TransientRow(2L, 5.6d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 4.6d),
          new TransientRow(2L, 4.4d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 0d),
          new TransientRow(2L, 5.4d)).iterator
      }
    )
    fireInstantFunctionTests(samples)
  }

  it ("should handle special cases") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 2.0d/0d),
          new TransientRow(2L, 4.5d),
          new TransientRow(2L, 0d),
          new TransientRow(2L, -2.1d),
          new TransientRow(2L, 5.9d),
          new TransientRow(2L, Double.NaN),
          new TransientRow(2L, 3.3d)).iterator
      }
    )
    fireInstantFunctionTests(samples)
  }

  private def fireInstantFunctionTests(samples: Array[RangeVector]): Unit = {
    // Abs
    val expected = samples.map(_.rows.map(v => scala.math.abs(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected, InstantFunctionId.Abs)
    // Ceil
    val expected2 = samples.map(_.rows.map(v => scala.math.ceil(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected2, InstantFunctionId.Ceil)
    // ClampMax
    val expected3 = samples.map(_.rows.map(v => scala.math.min(v.getDouble(1), 4)))
    applyFunctionAndAssertResult(samples, expected3, InstantFunctionId.ClampMax, Seq(4.toDouble))
    // ClampMin
    val expected4 = samples.map(_.rows.map(v => scala.math.max(v.getDouble(1), 4.toDouble)))
    applyFunctionAndAssertResult(samples, expected4, InstantFunctionId.ClampMin, Seq(4))
    // Floor
    val expected5 = samples.map(_.rows.map(v => scala.math.floor(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected5, InstantFunctionId.Floor)
    // Log
    val expected6 = samples.map(_.rows.map(v => scala.math.log(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected6, InstantFunctionId.Ln)
    // Log10
    val expected7 = samples.map(_.rows.map(v => scala.math.log10(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected7, InstantFunctionId.Log10)
    // Log2
    val expected8 = samples.map(_.rows.map(v => scala.math.log10(v.getDouble(1)) / scala.math.log10(2.0)))
    applyFunctionAndAssertResult(samples, expected8, InstantFunctionId.Log2)
    // Sqrt
    val expected10 = samples.map(_.rows.map(v => scala.math.sqrt(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected10, InstantFunctionId.Sqrt)
    // Exp
    val expected11 = samples.map(_.rows.map(v => scala.math.exp(v.getDouble(1))))
    applyFunctionAndAssertResult(samples, expected11, InstantFunctionId.Exp)
    // Round
    testRoundFunction(samples)
  }

  private def testRoundFunction(samples: Array[RangeVector]): Unit = {
    // Round
    val expected9 = samples.map(_.rows.map(v => {
      val value = v.getDouble(1)
      val toNearestInverse = 1.0
      if (value.isNaN || value.isInfinite)
        value
      else
        scala.math.floor(value * toNearestInverse + 0.5) / toNearestInverse
    }))
    applyFunctionAndAssertResult(samples, expected9, InstantFunctionId.Round)
    // Round with param
    val expected10 = samples.map(_.rows.map(v => {
      val value = v.getDouble(1)
      val toNearestInverse = 1.0 / 10
      if (value.isNaN || value.isInfinite)
        value
      else
        scala.math.floor(value * toNearestInverse + 0.5) / toNearestInverse
    }))
    applyFunctionAndAssertResult(samples, expected10, InstantFunctionId.Round, Seq(10))
  }

  it ("should handle unknown functions") {
    // sort_desc
    the[UnsupportedOperationException] thrownBy {
      val miscellaneousVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.SortDesc)
      miscellaneousVectorFnMapper(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "SortDesc not supported."
  }

  it ("should validate invalid function params") {
    // clamp_max
    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper1 = exec.InstantVectorFunctionMapper(InstantFunctionId.ClampMax)
      instantVectorFnMapper1(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: Cannot use ClampMax without providing a upper limit of max."
    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper2 = exec.InstantVectorFunctionMapper(InstantFunctionId.ClampMax, Seq("hi"))
      instantVectorFnMapper2(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: Cannot use ClampMax without providing a upper limit of max as a Number."

    // clamp_min
    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper3 = exec.InstantVectorFunctionMapper(InstantFunctionId.ClampMin)
      instantVectorFnMapper3(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: Cannot use ClampMin without providing a lower limit of min."
    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper4 = exec.InstantVectorFunctionMapper(InstantFunctionId.ClampMin, Seq("hi"))
      instantVectorFnMapper4(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: Cannot use ClampMin without providing a lower limit of min as a Number."

    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper5 = exec.InstantVectorFunctionMapper(InstantFunctionId.Sqrt, Seq(1))
      instantVectorFnMapper5(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: No additional parameters required for the instant function."

    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper5 = exec.InstantVectorFunctionMapper(InstantFunctionId.Round, Seq("hi"))
      instantVectorFnMapper5(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: to_nearest optional parameter should be a Number."

    the[IllegalArgumentException] thrownBy {
      val instantVectorFnMapper5 = exec.InstantVectorFunctionMapper(InstantFunctionId.Round, Seq(1, 2))
      instantVectorFnMapper5(MetricsTestData.timeseriesDataset,
        Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    } should have message "requirement failed: Only one optional parameters allowed for Round."

    // histogram quantile
    the[IllegalArgumentException] thrownBy {
      val ivMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramQuantile)
      ivMapper(MetricsTestData.timeseriesDataset, Observable.fromIterable(sampleBase), queryConfig, 1000, histSchema)
    } should have message "requirement failed: Quantile (between 0 and 1) required for histogram quantile"

    the[IllegalArgumentException] thrownBy {
      val ivMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramQuantile, Seq("b012"))
      ivMapper(MetricsTestData.timeseriesDataset, Observable.fromIterable(sampleBase), queryConfig, 1000, histSchema)
    } should have message "requirement failed: histogram_quantile parameter must be a number"

    // histogram bucket
    the[IllegalArgumentException] thrownBy {
      val ivMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramBucket)
      ivMapper(MetricsTestData.timeseriesDataset, Observable.fromIterable(sampleBase), queryConfig, 1000, histSchema)
    } should have message "requirement failed: Bucket/le required for histogram bucket"

    the[IllegalArgumentException] thrownBy {
      val ivMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramBucket, Seq("b012"))
      ivMapper(MetricsTestData.timeseriesDataset, Observable.fromIterable(sampleBase), queryConfig, 1000, histSchema)
    } should have message "requirement failed: histogram_bucket parameter must be a number"
  }

  it ("should fail with wrong calculation") {
    // ceil
    val expectedVal = sampleBase.map(_.rows.map(v => scala.math.floor(v.getDouble(1))))
    val instantVectorFnMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.Ceil)
    val resultObs = instantVectorFnMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleBase), queryConfig, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 should not equal val2
        }
      }
    }
  }

  it("should compute histogram_quantile on Histogram RV") {
    val (data, histRV) = histogramRV(numSamples = 10)
    val expected = Seq(0.8, 1.6, 2.4, 3.2, 4.0, 5.6, 7.2, 9.6)
    applyFunctionAndAssertResult(Array(histRV), Array(expected.toIterator),
                                 InstantFunctionId.HistogramQuantile, Seq(0.4), histSchema)

    // check output schema
    val instantVectorFnMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramQuantile,
                                                                 Seq(0.99))
    val outSchema = instantVectorFnMapper.schema(MMD.histDataset, histSchema)
    outSchema.columns.map(_.colType) shouldEqual resultSchema.columns.map(_.colType)
  }

  it("should compute histogram_max_quantile on Histogram RV") {
    val (data, histRV) = MMD.histMaxRV(100000L, numSamples = 7)
    val expected = data.zipWithIndex.map { case (row, i) =>
      // Calculating the quantile is quite complex... sigh
      val _max = row(3).asInstanceOf[Double]
      if ((i % 8) == 0) (_max * 0.9) else {
        val _hist = row(4).asInstanceOf[bv.MutableHistogram]
        val rank = 0.9 * _hist.bucketValue(_hist.numBuckets - 1)
        val ratio = (rank - _hist.bucketValue((i-1) % 8)) / (_hist.bucketValue(i%8) - _hist.bucketValue((i-1) % 8))
        _hist.bucketTop((i-1) % 8) + ratio * (_max -  _hist.bucketTop((i-1) % 8))
      }
    }
    applyFunctionAndAssertResult(Array(histRV), Array(expected.toIterator),
                                 InstantFunctionId.HistogramMaxQuantile, Seq(0.9), histMaxSchema)
  }

  it("should return proper schema after applying histogram_max_quantile") {
    val instantVectorFnMapper = exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramMaxQuantile,
                                                                 Seq(0.99))
    val outSchema = instantVectorFnMapper.schema(MMD.histMaxDS, histMaxSchema)
    outSchema.columns.map(_.colType) shouldEqual resultSchema.columns.map(_.colType)
  }

  it("should compute histogram_bucket on Histogram RV") {
    val (data, histRV) = histogramRV(numSamples = 10)
    val expected = Seq(1.0, 2.0, 3.0, 4.0, 4.0, 4.0, 4.0, 4.0)
    applyFunctionAndAssertResult(Array(histRV), Array(expected.toIterator),
                                 InstantFunctionId.HistogramBucket, Seq(16.0), histSchema)

    // Specifying a nonexistant bucket returns NaN
    applyFunctionAndAssertResult(Array(histRV), Array(Seq.fill(8)(Double.NaN).toIterator),
                                 InstantFunctionId.HistogramBucket, Seq(9.0), histSchema)
  }

  it("should test date time functions") {
    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 1456790399), // 2016-02-29 23:59:59 February 29th
          new TransientRow(2L, 1456790400), // 2016-03-01 00:00:00 March 1st
          new TransientRow(3L, 1230768000), // 2009-01-01 00:00:00 just after leap second
          new TransientRow(4L, 1230767999)  // 2008-12-31 23:59:59 just before leap second.
        ).iterator
      }
    )
    applyFunctionAndAssertResult(samples, Array(List(2.0, 3.0).toIterator), InstantFunctionId.Month)
    applyFunctionAndAssertResult(samples, Array(List(2016.0, 2016.0, 2009.0, 2008.0).toIterator), InstantFunctionId.Year)
    applyFunctionAndAssertResult(samples, Array(List(59.0, 0.0, 0.0, 59.0).toIterator), InstantFunctionId.Minute)
    applyFunctionAndAssertResult(samples, Array(List(23.0, 0.0, 0.0, 23.0).toIterator), InstantFunctionId.Hour)
    applyFunctionAndAssertResult(samples, Array(List(29.0, 31.0, 31.0, 31.0).toIterator), InstantFunctionId.DaysInMonth)
    applyFunctionAndAssertResult(samples, Array(List(29.0, 1.0, 1.0, 31.0).toIterator), InstantFunctionId.DayOfMonth)
    applyFunctionAndAssertResult(samples, Array(List(1.0, 2.0, 4.0, 3.0).toIterator), InstantFunctionId.DayOfWeek)
  }

  it("should handle NaN for date time functions") {
    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, Double.NaN),
          new TransientRow(2L, Double.NaN)
        ).iterator
      }
    )
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.Month)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.Year)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.Minute)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.Hour)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.DaysInMonth)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.DayOfMonth)
    applyFunctionAndAssertResult(samples, Array(List(Double.NaN, Double.NaN).toIterator), InstantFunctionId.DayOfWeek)
  }

  private def applyFunctionAndAssertResult(samples: Array[RangeVector], expectedVal: Array[Iterator[Double]],
                                instantFunctionId: InstantFunctionId, funcParams: Seq[Any] = Nil,
                                schema: ResultSchema = resultSchema): Unit = {
    val instantVectorFnMapper = exec.InstantVectorFunctionMapper(instantFunctionId, funcParams)
    val resultObs = instantVectorFnMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(samples), queryConfig, 1000, schema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            if (val1.isInfinity) val2.isInfinity shouldEqual true
            else if (val1.isNaN) val2.isNaN shouldEqual true
            else val1 shouldEqual val2 +- 0.0001
        }
      }
    }
  }
}