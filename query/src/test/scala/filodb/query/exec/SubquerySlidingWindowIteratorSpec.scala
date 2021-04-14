package filodb.query.exec

import filodb.core.query.ResultSchema
import filodb.query.exec.rangefn.RawDataWindowingSpec
import filodb.core.MetricsTestData
import filodb.core.metadata.Column.ColumnType
import filodb.query.exec.rangefn.RangeFunction
import filodb.query.exec.rangefn.RangeFunction.RangeFunctionGenerator
import filodb.core.query.TransientRow

class SubquerySlidingWindowIteratorSpec extends RawDataWindowingSpec {

  val tsResSchema = ResultSchema(MetricsTestData.timeseriesSchema.dataInfos, 1, colIDs = Seq(0, 1))

  it("should deal with skips used in subqueries SumOverTime") {
    // suppose we want
    // sum_over_time(foo[6:3])[4:2], where now=18
    // 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
    // this will translate to sum over time of:
    // point of time 18-4 with range 18-4-6, 18-4-3, 18-4, ie (8, 11, 14), sum 14 -> 33
    // point of time 18-2 with range 18-2-6, 18-2-3, 18-2, ie (10, 13, 16), sum 16 -> 39
    // point of time 18   with range 18-6,   18-3,   18,   ie (12, 15, 18), sum 18 -> 45
    val samples = Seq(
      0L -> 0d, 1L -> 1d, 2L -> 2d, 3L -> 3d, 4L -> 4d, 5L -> 5d, 6L -> 6d,
      7L -> 7d, 8L -> 8d, 9L -> 9d, 10L -> 10d, 11L -> 11d, 12L -> 12d,
      13L -> 13d, 14L -> 14d, 15L -> 15d, 16L -> 16d, 17L -> 17d, 18L -> 18d, 19L -> 19d, 20L -> 20d
    )
    val windowResults = Seq(
      14L -> 33d, 16L -> 39d, 18L -> 45d
    )

    val rawRows = samples.map(s => new TransientRow(s._1, s._2))
    import filodb.core.query.NoCloseCursor._
    val generator : RangeFunctionGenerator = RangeFunction.generatorFor(
      tsResSchema,
      Some(InternalRangeFunction.SumOverTime),
      ColumnType.DoubleColumn,
      queryConfig, Nil,
      false
    )
    val slidingWinIterator = new SubquerySlidingWindowIterator(
      14L,
      18L,
      2L,
      6L,
      3l,
      rawRows.iterator,
      generator,
      queryConfig
    )
    slidingWinIterator.map(r => (r.getLong(0) -> r.getDouble(1))).toList shouldEqual windowResults

  }

}
