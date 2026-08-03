package filodb.core.query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

import filodb.core.metadata.Column.ColumnType
import filodb.core.metrics.{FilodbMetrics, MetricsCounter}
import filodb.core.query.QueryUtils.SamplesScannedScope.SamplesScannedScope
import filodb.core.query.QueryUtils.SamplesScannedType.SamplesScannedType

/**
 * Storage for utility functions.
 */
object QueryUtils {
  object SamplesScannedScope extends Enumeration {
    type SamplesScannedScope = Value
    val Parent, Child = Value
  }
  object SamplesScannedType extends Enumeration {
    type SamplesScannedType = Value
    val Row, Series, PartKeyByte = Value
  }

  val REGEX_CHARS: Array[Char] = Array('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '\\')
  private val COMBO_CACHE_SIZE = 2048
  private val SAMPLES_SCANNED_METRIC_NAME = "num-samples-scanned-by-queries"
  private val SAMPLES_SCANNED_SCOPE_LABEL = "scope"
  private val SAMPLES_SCANNED_TYPE_LABEL = "type"

  private val regexCharsMinusPipe = (REGEX_CHARS.toSet - '|').toArray

  private val comboCache: Cache[Map[String, Seq[String]], Seq[Map[String, String]]] =
    Caffeine.newBuilder()
      .maximumSize(COMBO_CACHE_SIZE)
      .recordStats()
      .build()

  private def makeSamplesScannedCounter(sampleScope: SamplesScannedScope,
                                        sampleType: SamplesScannedType): MetricsCounter = {
    FilodbMetrics.counter(SAMPLES_SCANNED_METRIC_NAME, Map(
      SAMPLES_SCANNED_SCOPE_LABEL -> sampleScope.toString.toLowerCase,
      SAMPLES_SCANNED_TYPE_LABEL -> sampleType.toString.toLowerCase
    ))
  }

  // These metrics will be useful to help tune the samples-scanned config (e.g.
  //   they can be fed into a linear regression to determine how well samples-scanned
  //   values correlate with partition saturation).
  private val rowSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Parent, SamplesScannedType.Row)
  private val seriesSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Parent, SamplesScannedType.Series)
  private val partKeyBytesSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Parent, SamplesScannedType.PartKeyByte)
  private val childRowSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Child, SamplesScannedType.Row)
  private val childSeriesSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Child, SamplesScannedType.Series)
  private val childPartKeyBytesSamplesScanned = makeSamplesScannedCounter(
    SamplesScannedScope.Child, SamplesScannedType.PartKeyByte)

  /**
   * Returns true iff the argument string contains any special regex chars.
   */
  def containsRegexChars(str: String): Boolean = {
    str.exists(REGEX_CHARS.contains(_))
  }

  /**
   * Returns true iff the argument string contains no special regex
   *   characters except the pipe character ('|').
   * True is also returned when the string contains no pipes.
   */
  def containsPipeOnlyRegex(str: String): Boolean = {
    str.forall(!regexCharsMinusPipe.contains(_))
  }

  /**
   * Returns all possible sets of elements where exactly one element is
   *   chosen from each of the argument sequences.
   *
   * @param choices: all sequences should have at least one element.
   * @return ordered sequences; each sequence is ordered such that the element
   *         at index i is chosen from the ith argument sequence.
   */
  def combinations[T](choices: Seq[Seq[T]]): Seq[Seq[T]] = {
    val running = mutable.ArraySeq.make(new Array[Any](choices.size)).asInstanceOf[mutable.ArraySeq[T]]
    val result = new mutable.ArrayBuffer[Seq[T]]
    def helper(iChoice: Int): Unit = {
      if (iChoice == choices.size) {
        result.append(Nil ++ running)
        return
      }
      for (choice <- choices(iChoice)) {
        running(iChoice) = choice
        helper(iChoice + 1)
      }
    }
    helper(0)
    result.toSeq
  }

  /**
   * Splits a string on unescaped pipe characters.
   */
  def splitAtUnescapedPipes(str: String): Seq[String] = {
    // match pipes preceded by any count of backslash pairs and either
    //   some non-backslash character or the beginning of a line.
    val regex = "(?<=(^|[^\\\\]))(\\\\\\\\)*(\\|)".r
    // get pipe indices -- third capture group
    val pipeIndices = regex.findAllMatchIn(str).map(_.start(3)).toSeq

    var offset = 0
    val splits = new ArrayBuffer[String](pipeIndices.size + 1)
    var remaining = str
    for (i <- pipeIndices) {
      // split at the pipe and remove it
      val left = remaining.substring(0, i - offset)
      val right = remaining.substring(i - offset + 1)
      splits.append(left)
      remaining = right
      // count of all characters before the remaining suffix (+1 to account for pipe)
      offset = offset + left.length + 1
    }
    splits.append(remaining)
    splits.toSeq
  }

  /**
   * Given a mapping of individual keys to sets of values, returns all possible combinations
   *   of (key,value) pairs s.t. each combo contains all keys.
   * Example:
   *   foo -> [foo1, foo2]
   *   bar -> [bar1, bar2]
   *   --> [foo=foo1,bar=bar1], [foo=foo1,bar=bar2], [foo=foo2,bar=bar1], [foo=foo2,bar=bar2]
   */
  def makeAllKeyValueCombos(keyToValues: Map[String, Seq[String]]): Seq[Map[String, String]] = {
    // Store the entries with some order, then find all possible value combos s.t. each combo's
    //   ith value is a value of the ith key.
    comboCache.get(keyToValues, _ => {
      val entries = keyToValues.toSeq.sortBy(_._1)
      val keys = entries.map(_._1)
      val vals = entries.map(_._2.toSeq)
      val combos = QueryUtils.combinations(vals)
      // Zip the ordered keys with the ordered values.
      combos.map(keys.zip(_).toMap)
    })
  }

  /**
   * Returns the sample multiplier that should be applied
   *   to a row with the argument column type.
   */
  private def getSamplesScannedRowMultiplier(colInfo: ColumnInfo,
                                             config: SamplesScannedConfig): Double = {
    colInfo.colType match {
      case ColumnType.HistogramColumn =>
        if (colInfo.isExponential) {
          config.exponentialHistogramRowMultiplier
        } else {
          config.histogramRowMultiplier
        }
      case _ => config.defaultRowMultiplier
    }
  }

  /**
   * Efficiently compute the appropriate per-row samples-scanned multiplier.
   */
  private def computeSamplesScannedRowMultiplier(schema: ResultSchema,
                                                 config: SamplesScannedConfig): Double = {
    if (config.fixedRowMultiplier.isDefined) {
      return config.fixedRowMultiplier.get
    }

    // NOTE: avoiding .sum, .map, and `for` to prevent the allocation overhead.
    var rowMultiplier = 0.0
    var i = 0
    while (i < schema.columns.size) {
      rowMultiplier += getSamplesScannedRowMultiplier(schema.columns(i), config)
      i += 1
    }
    rowMultiplier
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument [[QueryStats]].
   *
   * @param clazz The class that produced these samples.
   */
  def trackSamplesScanned(seriesScanned: Long,
                          rowsScanned: Long,
                          partKeyBytes: Long,
                          clazz: Class[_],
                          queryStats: QueryStats,
                          schema: ResultSchema,
                          config: SamplesScannedConfig): Unit = {
    // Exit early if there are no stats to update.
    if (queryStats.unsafeSize() == 0) {
      return
    }

    // Each computation below has an if() short-circuit.
    // This is helpful for leaf samples-scanned counters, where this method is sometimes invoked
    //   specifically to account for rows, and sometimes invoked specifically to account for series.
    val rowSamples = if (rowsScanned > 0) {
      val rowMultiplier = computeSamplesScannedRowMultiplier(schema, config)
      val sampleCount = rowsScanned * rowMultiplier * (
          // NOTE: avoiding getOrElse to avoid lambda allocations.
          if (config.classToSamplesPerRow.contains(clazz)) config.classToSamplesPerRow(clazz)
          else config.defaultSamplesPerRow
        )
      rowSamplesScanned.increment(sampleCount.toLong)
      sampleCount
    } else 0

    val seriesSamples = if (seriesScanned > 0) {
      val sampleCount = seriesScanned * (
          if (config.classToSamplesPerSeries.contains(clazz)) config.classToSamplesPerSeries(clazz)
          else config.defaultSamplesPerSeries
        )
      seriesSamplesScanned.increment(sampleCount.toLong)
      sampleCount
    } else 0

    val partKeySamples = if (partKeyBytes > 0) {
      val sampleCount = partKeyBytes * (
          if (config.classToSamplesPerPartKeyByte.contains(clazz)) config.classToSamplesPerPartKeyByte(clazz)
          else config.defaultSamplesPerPartKeyByte
        )
      partKeyBytesSamplesScanned.increment(sampleCount.toLong)
      sampleCount
    } else 0

    val totalSamples = Math.ceil(
      rowSamples + seriesSamples + partKeySamples
    ).asInstanceOf[Long]

    queryStats.unsafeAddSamplesScanned(totalSamples)
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument [[QueryStats]].
   *
   * @param class The class that produced these samples.
   */
  def trackSamplesScanned(rv: RangeVector,
                          clazz: Class[_],
                          queryStats: QueryStats,
                          schema: ResultSchema,
                          config: SamplesScannedConfig): Unit = {
    val seriesScanned = 1
    val rowsScanned = rv.estimateNumRows()
    val partKeyBytes = rv.key.keySize
    trackSamplesScanned(seriesScanned, rowsScanned, partKeyBytes,
      clazz, queryStats, schema, config)
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument [[QueryStats]].
   *
   * @param childRv The [[RangeVector]] that was scanned by the parent.
   * @param parentClass The class that scanned the child [[RangeVector]].
   */
  def trackChildSamplesScanned(childRv: RangeVector,
                               parentClass: Class[_],
                               queryStats: QueryStats,
                               schema: ResultSchema,
                               config: SamplesScannedConfig): Unit = {
    // Exit early if there are no stats to update.
    if (queryStats.unsafeSize() == 0) {
      return
    }

    // NOTE: avoiding getOrElse below to avoid lambda allocations.

    val rowMultiplier = computeSamplesScannedRowMultiplier(schema, config)
    val rowSamples = childRv.estimateNumRows() * rowMultiplier * (
        if (config.classToSamplesPerChildRow.contains(parentClass)) config.classToSamplesPerChildRow(parentClass)
        else config.defaultSamplesPerChildRow
      )
    childRowSamplesScanned.increment(rowSamples.toLong)

    val seriesSamples =
      if (config.classToSamplesPerChildSeries.contains(parentClass)) config.classToSamplesPerChildSeries(parentClass)
      else config.defaultSamplesPerChildSeries
    childSeriesSamplesScanned.increment(seriesSamples.toLong)

    val partKeySamples = childRv.key.keySize * (
        if (config.classToSamplesPerChildPartKeyByte.contains(parentClass))
          config.classToSamplesPerChildPartKeyByte(parentClass)
        else config.defaultSamplesPerChildPartKeyByte
      )
    childPartKeyBytesSamplesScanned.increment(partKeySamples.toLong)

    val totalSamples = Math.ceil(
      rowSamples + seriesSamples + partKeySamples
    ).asInstanceOf[Long]

    queryStats.unsafeAddSamplesScanned(totalSamples)
  }

  def maxIgnoreNaN(a: Double, b: Double): Double = {
    if (a != a) b // a is NaN
    else if (b != b) a // b is NaN
    else if (a > b) a
    else b
  }

  def minIgnoreNaN(a: Double, b: Double): Double = {
    if (a != a) b // a is NaN
    else if (b != b) a // b is NaN
    else if (a < b) a
    else b
  }

}
