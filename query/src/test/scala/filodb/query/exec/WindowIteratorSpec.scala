package filodb.query.exec

import filodb.core.metadata.Column.ColumnType
import filodb.query.RangeFunctionId
import filodb.query.exec.rangefn.{RangeFunction, RawDataWindowingSpec}

/**
 * Tests both the SlidingWindowIterator and the ChunkedWindowIterator
 */
class WindowIteratorSpec extends RawDataWindowingSpec {

  val counterSamples = Seq(
                    1538416054000L->207545926d,
                    1538416059000L->207596377d,
                    1538416064000L->207598713d,
                    1538416069000L->207659389d,
                    1538416074000L->207662375d,
                    1538416079000L->207664711d,
                    1538416084000L->207725387d,
                    1538416089000L->207728373d,
                    1538416094000L->207730709d,
                    1538416099000L->207743920d,
                    1538416104000L->207794371d,
                    1538416109000L->207796707d,
                    1538416114000L->207809918d,
                    1538416119000L->207860369d,
                    1538416124000L->207862705d,
                    1538416129000L->207923381d,
                    1538416134000L->207926367d,
                    1538416139000L->207928703d,
                    1538416144000L->207989379d,
                    1538416149000L->207992365d,
                    1538416154000L->207994701d,
                    1538416159000L->208007912d,
                    1538416164000L->208058363d,
                    1538416169000L->208060699d,
                    1538416174000L->208121375d,
                    1538416179000L->208124361d,
                    1538416184000L->208126697d,
                    1538416189000L->208187373d,
                    1538416194000L->208190359d,
                    1538416199000L->208192695d,
                    1538416204000L->208253371d,
                    1538416209000L->208256357d,
                    1538416214000L->208258693d,
                    1538416219000L->208319369d,
                    1538416224000L->208322355d,
                    1538416229000L->208324691d,
                    1538416234000L->208385367d,
                    1538416239000L->208388353d,
                    1538416244000L->208390689d,
                    1538416259000L->208456687d,
                    1538416249000L->208451365d, // out of order, should be dropped
                    1538416254000L->208454351d, // out of order, should be dropped
                    1538416264000L->208469898d,
                    1538416269000L->208520349d,
                    1538416274000L->208522685d,
                    1538416279000L->208583361d,
                    1538416284000L->208586347d,
                    1538416289000L->208588683d,
                    1538416294000L->208601894d,
                    1538416299000L->208652345d,
                    1538416304000L->208654681d,
                    1538416309000L->208715357d,
                    1538416314000L->208718343d,
                    1538416319000L->208720679d,
                    1538416324000L->208781355d,
                    1538416329000L->208784341d,
                    1538416334000L->208786677d,
                    1538416344000L->208850339d,
                    1538416339000L->208847353d,  // out of order, should be dropped
                    1538416349000L->208852675d,
                    1538416354000L->208913351d,
                    1538416359000L->208916337d,
                    1538416364000L->208918673d,
                    1538416369000L->208931884d,
                    1538416374000L->208982335d,
                    1538416379000L->208984671d,
                    1538416384000L->209045347d,
                    1538416389000L->209048333d,
                    1538416394000L->209050669d,
                    1538416399000L->209111345d,
                    1538416404000L->209114331d,
                    1538416409000L->209116667d,
                    1538416414000L->209177343d,
                    1538416419000L->209180329d,
                    1538416424000L->209182665d,
                    1538416429000L->209243341d,
                    1538416434000L->209246327d,
                    1538416439000L->209248663d,
                    1538416444000L->209261874d,
                    1538416449000L->209312325d,
                    1538416454000L->209314661d,
                    1538416459000L->209375337d,
                    1538416464000L->209378323d,
                    1538416469000L->209380659d,
                    1538416474000L->209441335d,
                    1538416479000L->209444321d,
                    1538416484000L->209446657d,
                    1538416489000L->209507333d,
                    1538416494000L->209510319d,
                    1538416499000L->209512655d,
                    1538416504000L->209573331d,
                    1538416509000L->209576317d,
                    1538416514000L->209578653d,
                    1538416519000L->209639329d,
                    1538416524000L->209642315d,
                    1538416529000L->209644651d,
                    1538416534000L->209705327d,
                    1538416539000L->209708313d,
                    1538416544000L->209710649d,
                    1538416549000L->209771325d,
                    1538416554000L->209774311d,
                    1538416559000L->209776647d,
                    1538416564000L->209837323d,
                    1538416569000L->209840309d,
                    1538416574000L->209842645d,
                    1538416579000L->209903321d,
                    1538416584000L->209906307d,
                    1538416589000L->209908643d,
                    1538416594000L->209969319d,
                    1538416599000L->209972305d,
                    1538416604000L->209974641d,
                    1538416609000L->210035317d,
                    1538416614000L->210038303d,
                    1538416619000L->210040639d,
                    1538416624000L->210101315d,
                    1538416629000L->210104301d,
                    1538416634000L->210106637d,
                    1538416639000L->210167313d,
                    1538416644000L->210170299d,
                    1538416649000L->210172635d)

  it ("should ignore out of order samples for RateFunction") {
    val rawRows = counterSamples.map(s => new TransientRow(s._1, s._2))
    val slidingWinIterator = new SlidingWindowIterator(rawRows.iterator, 1538416154000L, 20000, 1538416649000L,20000,
      RangeFunction(Some(RangeFunctionId.Rate), ColumnType.DoubleColumn, queryConfig, useChunked = false).asSliding,
      queryConfig)
    slidingWinIterator.foreach{ v =>
      // if out of order samples are not removed, counter correction causes rate to spike up to very high value
      v.value should be < 10000d
    }
  }

  it ("should drop out of order samples with LastSampleFunction") {

    val samples = Seq(
      100L->645926d,
      153L->696377d,
      270L->759389d,
      250L->698713d, // out of order, should be dropped
      280L->762375d,
      360L->764711d,
      690L->798373d,
      430L->765387d, // out of order, should be dropped
      700L->830709d
    )
    val rawRows = samples.map(s => new TransientRow(s._1, s._2))
    val start = 50L
    val end = 1000L
    val step = 5
    val slidingWinIterator = new SlidingWindowIterator(rawRows.iterator, start, step,
      end, 0, RangeFunction(None, ColumnType.DoubleColumn, queryConfig, useChunked = false).asSliding, queryConfig)
    val result = slidingWinIterator.map(v => (v.timestamp, v.value)).toSeq
    result.map(_._1) shouldEqual (start to end).by(step)
    result.foreach{ v =>
      v._2 should not equal 698713d
      v._2 should not equal 765387d
    }
  }

  it("should calculate SumOverTime correctly even after time series stops. " +
    "It should exclude values at curWindowStart") {

    val samples = Seq(
      100000L->1d,
      153000L->2d,
      250000L->3d, // should not be part of window 240000 to 350000
      270000L->4d,
      280000L->5d,
      360000L->6d,
      430000L->7d,
      690000L->8d,
      700000L->9d,
      710000L->Double.NaN // NOTE: Prom end of time series marker
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      150000->1.0,
      250000->5.0,
      350000->9.0,
      450000->13.0,
      750000->17.0
    )
    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 50000L, 100000, 1100000L, 100000,
      RangeFunction(Some(RangeFunctionId.SumOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    // NOTE: dum_over_time sliding iterator does not handle the NaN at the end correctly!
    // slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual windowResults
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults

    val chunkedIt = new ChunkedWindowIteratorD(rv, 50000L, 100000, 1100000L, 100000,
      RangeFunction(Some(RangeFunctionId.SumOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults
  }

  it("should calculate the rate of given samples matching the prometheus rate function") {
    val samples = Seq(
      1548191486000L -> 84d,
      1548191496000L -> 152d,
      1548191506000L -> 195d,
      1548191516000L -> 222d,
      1548191526000L -> 245d,
      1548191536000L -> 251d,
      1548191546000L -> 329d,
      1548191556000L -> 374d,
      1548191566000L -> 431d
    )
    val windowResults = Seq(
      1548191496000L -> 0.34,
      1548191511000L -> 0.555,
      1548191526000L -> 0.60375,
      1548191541000L -> 0.668,
      1548191556000L -> 1.0357142857142858
    )
    val rawRows = samples.map(s => new TransientRow(s._1, s._2))
    val slidingWinIterator = new SlidingWindowIterator(rawRows.iterator, 1548191496000L, 15000, 1548191796000L, 300000,
      RangeFunction(Some(RangeFunctionId.Rate), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    slidingWinIterator.foreach { v =>
      windowResults.find(a => a._1 == v.timestamp).foreach(b => v.value shouldEqual b._2 +- 0.0000000001)
    }

    val rv = timeValueRV(samples)
    val chunkedIt = new ChunkedWindowIteratorD(rv, 1548191496000L, 15000, 1548191796000L, 300000,
      RangeFunction(Some(RangeFunctionId.Rate), ColumnType.DoubleColumn,  queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.foreach { v =>
      windowResults.find(a => a._1 == v.timestamp).foreach(b => v.value shouldEqual b._2 +- 0.0000000001)
    }

  }

  it ("should calculate lastSample when ingested samples are more than 5 minutes apart") {
    val samples = Seq(
      1540832354000L->1d,
      1540835954000L->2d,
      1540839554000L->3d,
      1540843154000L->4d,
      1540846754000L->237d,
      1540850354000L->330d
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      1540846755000L->237,
      1540846770000L->237,
      1540846785000L->237,
      1540846800000L->237,
      1540846815000L->237,
      1540846830000L->237,
      1540846845000L->237,
      1540846860000L->237,
      1540846875000L->237,
      1540846890000L->237,
      1540846905000L->237,
      1540846920000L->237,
      1540846935000L->237,
      1540846950000L->237,
      1540846965000L->237,
      1540846980000L->237,
      1540846995000L->237,
      1540847010000L->237,
      1540847025000L->237,
      1540847040000L->237, // note that value 237 becomes stale at this point. No samples with 237 anymore.
      1540850355000L->330,
      1540850370000L->330,
      1540850385000L->330,
      1540850400000L->330,
      1540850415000L->330,
      1540850430000L->330,
      1540850445000L->330,
      1540850460000L->330,
      1540850475000L->330,
      1540850490000L->330,
      1540850505000L->330,
      1540850520000L->330,
      1540850535000L->330,
      1540850550000L->330,
      1540850565000L->330,
      1540850580000L->330,
      1540850595000L->330,
      1540850610000L->330,
      1540850625000L->330,
      1540850640000L->330) // 330 becomes stale now.

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 1540845090000L,
                               15000, 1540855905000L, 0,
                               RangeFunction(None, ColumnType.DoubleColumn, queryConfig, useChunked = false).asSliding,
                               queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).toList.filter(!_._2.isNaN) shouldEqual windowResults

    val chunkedWinIt = new ChunkedWindowIteratorD(rv, 1540845090000L,
                         15000, 1540855905000L, queryConfig.staleSampleAfterMs,
                         RangeFunction(None, ColumnType.DoubleColumn, queryConfig, useChunked = true).asChunkedD,
                         queryConfig)
    chunkedWinIt.map(r => (r.getLong(0), r.getDouble(1))).toList.filter(!_._2.isNaN) shouldEqual windowResults
  }

  it("should not return NaN if value is present at time - staleSampleAfterMs") {
    val samples = Seq(
      100000L -> 100d,
      153000L -> 160d,
      200000L -> 200d
    )
    val windowResults = Seq(
      100000L -> 100d,
      200000L -> 200d,
      300000L -> 200d,
      400000L -> 200d,
      500000L -> 200d
    )
    val rv = timeValueRV(samples)

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 100000L,
      100000, 600000L, 0,
      RangeFunction(None, ColumnType.DoubleColumn, queryConfig, useChunked = false).asSliding,
      queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).toList.filter(!_._2.isNaN) shouldEqual windowResults

    // ChunkedWindowIterator requires window to be staleSampleAfterMs + 1 when window of SlidingWindowIterator is 0
    val chunkedWinIt = new ChunkedWindowIteratorD(rv, 100000L,
      100000, 600000L, queryConfig.staleSampleAfterMs + 1,
      RangeFunction(None, ColumnType.DoubleColumn, queryConfig, useChunked = true).asChunkedD, queryConfig)
    chunkedWinIt.map(r => (r.getLong(0), r.getDouble(1))).toList.filter(!_._2.isNaN) shouldEqual windowResults
  }

  it("should calculate AvgOverTime correctly even for windows with no values") {

    val samples = Seq(
      100000L -> 1d,
      153000L -> 2d,
      250000L -> 3d,
      270000L -> 4d,
      280000L -> 5d,
      360000L -> 6d,
      430000L -> 7d,
      690000L -> 8d,
      700000L -> 9d,
      710000L -> Double.NaN // NOTE: Prom end of time series marker
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      150000 -> 1.0,
      250000 -> 2.5,
      350000 -> 4.5,
      450000 -> 6.5
    )

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.AvgOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults

    val chunkedIt = new ChunkedWindowIteratorD(rv, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.AvgOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults
  }

  it("should calculate CountOverTime correctly even for windows with no values") {
    val samples = Seq(
      100000L -> 1d,
      153000L -> 2d,
      250000L -> 3d,
      270000L -> 4d,
      280000L -> 5d,
      360000L -> 6d,
      430000L -> 7d,
      690000L -> 8d,
      700000L -> 9d,
      710000L -> Double.NaN
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      150000 -> 1.0,
      250000 -> 2.0,
      350000 -> 2.0,
      450000 -> 2.0
    )

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.CountOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults


    val chunkedIt = new ChunkedWindowIteratorD(rv, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.CountOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults
  }

  it("should calculate MinOverTime correctly even for windows with no values") {
    val samples = Seq(
      100000L -> 1d,
      153000L -> 2d,
      250000L -> 3d,
      270000L -> 4d,
      280000L -> 5d,
      360000L -> 6d,
      430000L -> 7d,
      690000L -> 8d,
      700000L -> 9d,
      710000L -> Double.NaN
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      150000 -> 1.0,
      250000 -> 2.0,
      350000 -> 4.0,
      450000 -> 6.0
    )

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.MinOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults

    val chunkedIt = new ChunkedWindowIteratorD(rv, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.MinOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults

  }

  it("should calculate MaxOverTime correctly even for windows with no values") {
    val samples = Seq(
      100000L -> 1d,
      153000L -> 2d,
      250000L -> 3d,
      270000L -> 4d,
      280000L -> 5d,
      360000L -> 6d,
      430000L -> 7d,
      690000L -> 8d,
      700000L -> 9d,
      710000L -> Double.NaN
    )
    val rv = timeValueRV(samples)

    val windowResults = Seq(
      150000 -> 1.0,
      250000 -> 3.0,
      350000 -> 5.0,
      450000 -> 7.0
    )

    val slidingWinIterator = new SlidingWindowIterator(rv.rows, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.MaxOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = false).asSliding, queryConfig)
    slidingWinIterator.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults

    val chunkedIt = new ChunkedWindowIteratorD(rv, 50000L, 100000, 700000L, 100000,
      RangeFunction(Some(RangeFunctionId.MaxOverTime), ColumnType.DoubleColumn, queryConfig,
                    useChunked = true).asChunkedD, queryConfig)
    chunkedIt.map(r => (r.getLong(0), r.getDouble(1))).filter(!_._2.isNaN).toList shouldEqual windowResults
  }
}
