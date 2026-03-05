package filodb.core.query

import java.util.concurrent.atomic.AtomicLong

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import filodb.core.metadata.Column.ColumnType

class QueryUtilsSpec extends AnyFunSpec with Matchers{
  val doubleSchema = ResultSchema(
    Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)), 1)

  val histSchema = ResultSchema(
    Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.HistogramColumn)), 1)

  describe("trackSamplesScanned tests") {
    // Minimal RangeVectorKey with configurable keySize
    def makeKey(size: Int): RangeVectorKey = new RangeVectorKey {
      def labelValues = Map.empty
      def sourceShards = Nil
      def partIds = Nil
      def schemaNames = Nil
      def keySize: Int = size
    }

    // Minimal RangeVector with configurable numRows, outputRange, keySize
    def makeRV(numRowsOpt: Option[Int], outputRangeOpt: Option[RvRange], keySz: Int): RangeVector =
      new RangeVector {
        val key = makeKey(keySz)
        def rows(): RangeVectorCursor = new RangeVectorCursor {
          def hasNext = false
          def next() = throw new NoSuchElementException
          def close() = {}
        }
        def outputRange = outputRangeOpt
        override def numRows = numRowsOpt
      }

    describe("trackSamplesScanned (overload 1 - explicit counts)") {
      it("empty counters should be a no-op") {
        // Should not throw; counters list unchanged (trivially)
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 10, partKeyBytes = 100, classOf[String],
          List.empty, doubleSchema, SamplesScannedConfig())
      }

      it("default config, single counter - only row samples count") {
        val counter = new AtomicLong(0)
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 10, partKeyBytes = 100, classOf[String],
          List(counter), doubleSchema, SamplesScannedConfig())
        // rowSamples = 10 * 1.0 (DoubleColumn not in multiplier map) * 1.0 (defaultSamplesPerRow) = 10
        // seriesSamples = 5 * 0.0 = 0; partKeySamples = 100 * 0.0 = 0
        counter.get() shouldEqual 10
      }

      it("samples split evenly across multiple counters") {
        val counters = List.fill(3)(new AtomicLong(0))
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 6, partKeyBytes = 0, classOf[String],
          counters, doubleSchema, SamplesScannedConfig())
        // total = 6; 6/3 = 2 per counter
        counters.foreach(_.get() shouldEqual 2)
      }

      it("HistogramColumn schema applies row multiplier of 20") {
        val counter = new AtomicLong(0)
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 5, partKeyBytes = 0, classOf[String],
          List(counter), histSchema, SamplesScannedConfig())
        // rowSamples = 5 * 20.0 * 1.0 = 100
        counter.get() shouldEqual 100
      }

      it("class-specific samplesPerRow is applied") {
        val counter = new AtomicLong(0)
        val config = SamplesScannedConfig(classToSamplesPerRow = Map(classOf[String] -> 3.0))
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 4, partKeyBytes = 0, classOf[String],
          List(counter), doubleSchema, config)
        // rowSamples = 4 * 1.0 * 3.0 = 12
        counter.get() shouldEqual 12
      }

      it("class-specific samplesPerSeries is applied") {
        val counter = new AtomicLong(0)
        val config = SamplesScannedConfig(classToSamplesPerSeries = Map(classOf[String] -> 2.0))
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 0, partKeyBytes = 0, classOf[String],
          List(counter), doubleSchema, config)
        // seriesSamples = 5 * 2.0 = 10
        counter.get() shouldEqual 10
      }

      it("class-specific samplesPerPartKeyByte is applied") {
        val counter = new AtomicLong(0)
        val config = SamplesScannedConfig(classToSamplesPerPartKeyByte = Map(classOf[String] -> 0.5))
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 0, partKeyBytes = 20, classOf[String],
          List(counter), doubleSchema, config)
        // partKeySamples = 20 * 0.5 = 10
        counter.get() shouldEqual 10
      }
    }

    describe("trackSamplesScanned (overload 2 - RangeVector)") {
      it("uses rv.numRows when present") {
        val counter = new AtomicLong(0)
        val rv = makeRV(Some(7), None, 0)
        QueryUtils.trackSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, SamplesScannedConfig())
        counter.get() shouldEqual 7
      }

      it("falls back to outputRange when numRows is None") {
        val counter = new AtomicLong(0)
        val rv = makeRV(None, Some(RvRange(startMs = 0, stepMs = 100, endMs = 500)), 0)
        QueryUtils.trackSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, SamplesScannedConfig())
        // estimateNumRows = (500-0)/100 = 5
        counter.get() shouldEqual 5
      }

      it("falls back to 1 when neither numRows nor outputRange is available") {
        val counter = new AtomicLong(0)
        val rv = makeRV(None, None, 0)
        QueryUtils.trackSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, SamplesScannedConfig())
        counter.get() shouldEqual 1
      }

      it("uses rv.key.keySize as partKeyBytes") {
        val counter = new AtomicLong(0)
        val rv = makeRV(None, None, 20)
        val config = SamplesScannedConfig(classToSamplesPerPartKeyByte = Map(classOf[String] -> 1.0))
        QueryUtils.trackSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, config)
        // rowSamples = 1 (fallback) * 1.0 * 1.0 = 1
        // partKeySamples = 20 * 1.0 = 20
        counter.get() shouldEqual 21
      }
    }

    describe("trackChildSamplesScanned") {
      it("empty counters should be a no-op") {
        val rv = makeRV(Some(10), None, 50)
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List.empty, doubleSchema, SamplesScannedConfig())
      }

      it("default config yields zero child samples") {
        val counter = new AtomicLong(0)
        val rv = makeRV(Some(10), None, 50)
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, SamplesScannedConfig())
        // rowSamples = 10 * 1.0 * 0.0 = 0; seriesSamples = 0.0; partKeySamples = 50 * 0.0 = 0
        counter.get() shouldEqual 0
      }

      it("class-specific samplesPerChildRow is applied") {
        val counter = new AtomicLong(0)
        val rv = makeRV(Some(10), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 2.0))
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, config)
        // rowSamples = 10 * 1.0 * 2.0 = 20
        counter.get() shouldEqual 20
      }

      it("class-specific samplesPerChildSeries is applied (not multiplied by numRows)") {
        val counter = new AtomicLong(0)
        val rv = makeRV(Some(0), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildSeries = Map(classOf[String] -> 7.0))
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, config)
        // seriesSamples = 7.0
        counter.get() shouldEqual 7
      }

      it("class-specific samplesPerChildPartKeyByte is applied") {
        val counter = new AtomicLong(0)
        val rv = makeRV(None, None, 10)
        val config = SamplesScannedConfig(classToSamplesPerChildPartKeyByte = Map(classOf[String] -> 3.0))
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List(counter), doubleSchema, config)
        // rowSamples = 1 * 1.0 * 0.0 = 0; partKeySamples = 10 * 3.0 = 30
        counter.get() shouldEqual 30
      }

      it("HistogramColumn with child row multiplier is applied") {
        val counter = new AtomicLong(0)
        val rv = makeRV(Some(5), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 1.0))
        QueryUtils.trackChildSamplesScanned(rv, classOf[String],
          List(counter), histSchema, config)
        // rowSamples = 5 * 20.0 * 1.0 = 100
        counter.get() shouldEqual 100
      }
    }
  }

  it("should correctly identify regex chars") {
    val testsNoRegex = Seq(
      "",
      ",",
      "no regex-1234!@#%&_,/`~=<>';:"
    )
    // includes one test for each single regex char
    val testsRegex = QueryUtils.REGEX_CHARS.map(c => c.toString) ++ Seq(
      "\\",
      "\\\\",
      "foo\\.bar",  // escape chars don't affect the result (at least for now).
      "foo\\\\.bar",
      "foo|bar",
      "foo\\|bar",
      ".foo\\|bar"
    )
    for (test <- testsNoRegex) {
      QueryUtils.containsRegexChars(test) shouldEqual false
    }
    for (test <- testsRegex) {
      QueryUtils.containsRegexChars(test) shouldEqual true
    }
  }

  it ("should correctly identify non-pipe regex chars") {
    val testsPipeOnly = Seq(
      "",
      "|",
      "||||",
      "a|b|c|d|e",
      "foobar-1|2|34!@|#%&_,|/`~=<>|';:",
      // NOTE: some regex chars are missing from QueryUtils.REGEX_CHARS.
      // This is intentional to preserve existing behavior.
      "^foo|bar$"
    )
    // includes one test for each single non-pipe regex char
    val testsNonPipeOnly = QueryUtils.REGEX_CHARS.filter(c => c != '|').map(c => c.toString) ++ Seq(
      "\\",
      "\\\\",
      "foo\\|bar",  // escape chars don't affect the result (at least for now).
      "foo\\\\|bar",
      "foo.bar.baz|",
      "!@#$%^&*()_+{}[];':\""
    )
    for (test <- testsPipeOnly) {
      QueryUtils.containsPipeOnlyRegex(test) shouldEqual true
    }
    for (test <- testsNonPipeOnly) {
      QueryUtils.containsPipeOnlyRegex(test) shouldEqual false
    }
  }

  it("should correctly split strings at unescaped pipes") {
    val tests = Seq(
      ("this|is|a|test", Seq("this", "is", "a", "test")),
      ("this|is|a||test", Seq("this", "is", "a", "", "test")),
      ("this\\|is|a|test", Seq("this\\|is", "a", "test")),
      ("this\\\\|is|a|test", Seq("this\\\\", "is", "a", "test")),
      ("||this\\|is|\\+a|test||", Seq("", "", "this\\|is", "\\+a", "test", "", "")),
    )
    for ((string, expected) <- tests) {
      val res = QueryUtils.splitAtUnescapedPipes(string)
      res shouldEqual expected
    }
  }
}
