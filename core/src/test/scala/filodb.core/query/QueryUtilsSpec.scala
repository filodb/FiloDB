package filodb.core.query

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

  describe("track[Child]SamplesScanned tests") {
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

    def assertSamplesScanned(queryStats: QueryStats, expectedCounts: Map[Seq[String], Long]): Unit = {
      for ((key, value) <- expectedCounts) {
        assert(value == queryStats.stat(key).samplesScanned.get())
      }

      // Have to awkwardly account for the Nil key; Nil is always added for some plans.
      val nilInExpected = expectedCounts.exists(_._1.isEmpty)
      val nilInActual = queryStats.stat.contains(Nil)
      if (!nilInExpected && nilInActual) {
        // Make sure we've already asserted the values of all other keys.
        assert(queryStats.stat.size == 1 + expectedCounts.size)
        // Make sure no samples-scanned have been counted against Nil.
        assert(queryStats.stat(Nil).samplesScanned.get() == 0)
      } else {
        // Make sure we've already asserted the values of all keys.
        assert(queryStats.stat.size == expectedCounts.size)
      }
    }

    describe("trackSamplesScanned tests") {
      it("should be a no-op for empty stats") {
        val stats = QueryStats()
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 10, partKeyBytes = 100,
          classOf[String], stats, doubleSchema, SamplesScannedConfig())
        assert(stats.stat.isEmpty)
      }

      it("should count only row samples with default config") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 10, partKeyBytes = 100,
          classOf[String], stats, doubleSchema, SamplesScannedConfig())
        // rowSamples = 10 * 1.0 (DoubleColumn not in multiplier map) * 1.0 (defaultSamplesPerRow) = 10
        // seriesSamples = 5 * 0.0 = 0; partKeySamples = 100 * 0.0 = 0
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
      }

      it("should split samples evenly across QueryStats entries") {
        val stats = QueryStats()
        stats.stat.put(Seq("hello"), Stat())
        stats.stat.put(Seq("goodbye"), Stat())
        stats.stat.put(Seq("applesauce"), Stat())

        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 6, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, SamplesScannedConfig())
        // total = 6; 6/3 = 2 per counter
        assertSamplesScanned(stats, Map(
          Seq("hello") ->      2,
          Seq("goodbye") ->    2,
          Seq("applesauce") -> 2
        ))
      }

      it("should apply default row multiplier of 20 for HistogramColumn schema") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 5, partKeyBytes = 0,
          classOf[String], stats, histSchema, SamplesScannedConfig())
        // rowSamples = 5 * 20.0 * 1.0 = 100
        assertSamplesScanned(stats, Map(Seq("key") -> 100))
      }

      it("should apply default samplesPerRow") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(defaultSamplesPerRow = 3.0)
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 4, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, config)
        // rowSamples = 4 * 1.0 * 3.0 = 12
        assertSamplesScanned(stats, Map(Seq("key") -> 12))
      }

      it("should apply default samplesPerSeries") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(defaultSamplesPerSeries =  2.0)
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 0, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, config)
        // seriesSamples = 5 * 2.0 = 10
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
      }

      it("should apply default samplesPerPartKeyByte") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(defaultSamplesPerPartKeyByte = 0.5)
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 0, partKeyBytes = 20,
          classOf[String], stats, doubleSchema, config)
        // partKeySamples = 20 * 0.5 = 10
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
      }

      it("should apply class-specific samplesPerRow") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(classToSamplesPerRow = Map(classOf[String] -> 3.0))
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 4, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, config)
        // rowSamples = 4 * 1.0 * 3.0 = 12
        assertSamplesScanned(stats, Map(Seq("key") -> 12))
      }

      it("should apply class-specific samplesPerSeries") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(classToSamplesPerSeries = Map(classOf[String] -> 2.0))
        QueryUtils.trackSamplesScanned(seriesScanned = 5, rowsScanned = 0, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, config)
        // seriesSamples = 5 * 2.0 = 10
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
      }

      it("should apply class-specific samplesPerPartKeyByte") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val config = SamplesScannedConfig(classToSamplesPerPartKeyByte = Map(classOf[String] -> 0.5))
        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 0, partKeyBytes = 20,
          classOf[String], stats, doubleSchema, config)
        // partKeySamples = 20 * 0.5 = 10
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
      }

      it("should increment Nil counter when only Nil key exists") {
        val stats = QueryStats()
        stats.stat.put(Nil, Stat())

        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 6, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, SamplesScannedConfig())
        // Only the Nil key exists; total = 6; Nil counter should receive the increment
        assertSamplesScanned(stats, Map(Nil -> 6))
      }

      it("should not increment Nil counter when non-Nil keys exist") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())
        stats.stat.put(Nil, Stat())

        QueryUtils.trackSamplesScanned(seriesScanned = 0, rowsScanned = 6, partKeyBytes = 0,
          classOf[String], stats, doubleSchema, SamplesScannedConfig())
        // Nil is filtered out; only "key" receives the increment
        assertSamplesScanned(stats, Map(Seq("key") -> 6))
      }

      it("should count samples for 1 series, all rows, and correct part-key bytes during RV overload") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(3), None, 5)
        QueryUtils.trackSamplesScanned(
          rv, classOf[String], stats, doubleSchema,
          SamplesScannedConfig(
            defaultSamplesPerRow = 1,
            defaultSamplesPerSeries = 10,
            defaultSamplesPerPartKeyByte = 100))
        assertSamplesScanned(stats, Map(Seq("key") -> (3 + 10 + 500)))
      }
    }

    describe("trackChildSamplesScanned tests") {
      it("should be a no-op for empty QueryStats") {
        val stats = QueryStats()
        val rv = makeRV(Some(10), None, 50)
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, SamplesScannedConfig())
        assert(stats.stat.isEmpty)
      }

      it("should count zero child samples with default config") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(10), None, 50)
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, SamplesScannedConfig())
        // rowSamples = 10 * 1.0 * 0.0 = 0; seriesSamples = 0.0; partKeySamples = 50 * 0.0 = 0
        assertSamplesScanned(stats, Map(Seq("key") -> 0))
      }

      it("should apply default samplesPerChildRow") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(10), None, 0)
        val config = SamplesScannedConfig(defaultSamplesPerChildRow = 2)
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // rowSamples = 10 * 1.0 * 2.0 = 20
        assertSamplesScanned(stats, Map(Seq("key") -> 20))
      }

      it("should apply default samplesPerChildSeries") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(0), None, 0)
        val config = SamplesScannedConfig(defaultSamplesPerChildSeries = 7.0)
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // seriesSamples = 7.0
        assertSamplesScanned(stats, Map(Seq("key") -> 7))
      }

      it("should apply default samplesPerChildPartKeyByte") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(None, None, 10)
        val config = SamplesScannedConfig(defaultSamplesPerChildPartKeyByte = 3.0)
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // rowSamples = 1 * 1.0 * 0.0 = 0; partKeySamples = 10 * 3.0 = 30
        assertSamplesScanned(stats, Map(Seq("key") -> 30))
      }

      it("should apply class-specific samplesPerChildRow") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(10), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 2.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // rowSamples = 10 * 1.0 * 2.0 = 20
        assertSamplesScanned(stats, Map(Seq("key") -> 20))
      }

      it("should apply class-specific samplesPerChildSeries") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(0), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildSeries = Map(classOf[String] -> 7.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // seriesSamples = 7.0
        assertSamplesScanned(stats, Map(Seq("key") -> 7))
      }

      it("should apply class-specific samplesPerChildPartKeyByte") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(None, None, 10)
        val config = SamplesScannedConfig(classToSamplesPerChildPartKeyByte = Map(classOf[String] -> 3.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // rowSamples = 1 * 1.0 * 0.0 = 0; partKeySamples = 10 * 3.0 = 30
        assertSamplesScanned(stats, Map(Seq("key") -> 30))
      }

      it("should apply child row multiplier for HistogramColumn") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())

        val rv = makeRV(Some(5), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 1.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, histSchema, config)
        // rowSamples = 5 * 20.0 * 1.0 = 100
        assertSamplesScanned(stats, Map(Seq("key") -> 100))
      }

      it("should increment Nil counter when only Nil key exists") {
        val stats = QueryStats()
        stats.stat.put(Nil, Stat())

        val rv = makeRV(Some(10), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 1.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // Only the Nil key exists; rowSamples = 10 * 1.0 * 1.0 = 10; Nil counter should receive the increment
        assertSamplesScanned(stats, Map(Nil -> 10))
      }

      it("should not increment Nil counter when non-Nil keys exist") {
        val stats = QueryStats()
        stats.stat.put(Seq("key"), Stat())
        stats.stat.put(Nil, Stat())

        val rv = makeRV(Some(10), None, 0)
        val config = SamplesScannedConfig(classToSamplesPerChildRow = Map(classOf[String] -> 1.0))
        QueryUtils.trackChildSamplesScanned(
          rv, classOf[String], stats, doubleSchema, config)
        // Nil is filtered out; only "key" receives the increment
        assertSamplesScanned(stats, Map(Seq("key") -> 10))
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
