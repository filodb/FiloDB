package filodb.core.query

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

/**
 * Storage for utility functions.
 */
object QueryUtils {
  val REGEX_CHARS: Array[Char] = Array('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '\\')
  private val COMBO_CACHE_SIZE = 2048

  private val regexCharsMinusPipe = (REGEX_CHARS.toSet - '|').toArray

  private val comboCache: Cache[Map[String, Seq[String]], Seq[Map[String, String]]] =
    Caffeine.newBuilder()
      .maximumSize(COMBO_CACHE_SIZE)
      .recordStats()
      .build()


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
    val running = new mutable.ArraySeq[T](choices.size)
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
    result
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
    splits
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
      val entries = keyToValues.toSeq
      val keys = entries.map(_._1)
      val vals = entries.map(_._2.toSeq)
      val combos = QueryUtils.combinations(vals)
      // Zip the ordered keys with the ordered values.
      combos.map(keys.zip(_).toMap)
    })
  }

  /**
   * Estimates the count of rows in a {@link RangeVector}.
   * This may be wildly inaccurate; the implementation as of this writing relies
   *   on either numRows() or outputRange()-- both of which typically do not have values.
   */
  private def estimateNumRows(rv: RangeVector): Long = {
    rv.numRows
      .map(_.asInstanceOf[Long])
      .orElse(
        rv.outputRange
          .filter(range => range.stepMs > 0)
          .map(range => (range.endMs - range.startMs) / range.stepMs)
      )
      // Worst-case: at least count the time-series.
      .getOrElse(1)
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument counters; the total is divided evenly across all counters.
   * This is typically used to increment {@link QueryStats} samples-scanned counters.
   *
   * @param clazz The class that produced these samples.
   */
  def trackSamplesScanned(seriesScanned: Long,
                          rowsScanned: Long,
                          partKeyBytes: Long,
                          clazz: Class[_],
                          samplesScannedCounters: List[AtomicLong],
                          schema: ResultSchema,
                          config: SamplesScannedConfig): Unit = {
    // Exit early if there are no counters to update.
    if (samplesScannedCounters.isEmpty) {
      return
    }

    val valueColumnType = ResultSchema.valueColumnType(schema)
    val rowSamples = rowsScanned *
      config.valueColumnToRowMultiplier.getOrElse(valueColumnType, 1.0) *
      config.classToSamplesPerRow.getOrElse(clazz, config.defaultSamplesPerRow)

    val seriesSamples = seriesScanned *
      config.classToSamplesPerSeries.getOrElse(clazz, config.defaultSamplesPerSeries)

    val partKeySamples = partKeyBytes *
      config.classToSamplesPerPartKeyByte.getOrElse(clazz, config.defaultSamplesPerPartKeyByte)

    val totalSamples = Math.ceil(
      rowSamples + seriesSamples + partKeySamples
    ).asInstanceOf[Long]

    val samplesPerCounter = Math.ceil(
      totalSamples.asInstanceOf[Double] / samplesScannedCounters.size
    ).asInstanceOf[Long]
    samplesScannedCounters.foreach(ctr => ctr.addAndGet(samplesPerCounter))
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument counters; the total is divided evenly across all counters.
   * This is typically used to increment {@link QueryStats} samples-scanned counters.
   *
   * @param class The class that produced these samples.
   */
  def trackSamplesScanned(rv: RangeVector,
                          clazz: Class[_],
                          samplesScannedCounters: List[AtomicLong],
                          schema: ResultSchema,
                          config: SamplesScannedConfig): Unit = {
    val seriesScanned = 1
    val rowsScanned = estimateNumRows(rv)
    val partKeyBytes = rv.key.keySize
    trackSamplesScanned(seriesScanned, rowsScanned, partKeyBytes,
      clazz, samplesScannedCounters, schema, config)
  }

  /**
   * Given the arguments, determines the total count of samples scanned.
   * Adds the total to the argument counters; the total is divided evenly across all counters.
   * This is typically used to increment {@link QueryStats} samples-scanned counters.
   *
   * @param childRv The {@link RangeVector} that was scanned by the parent.
   * @param parentClass The class that scanned the child {@link RangeVector}.
   */
  def trackChildSamplesScanned(childRv: RangeVector,
                               parentClass: Class[_],
                               samplesScannedCounters: List[AtomicLong],
                               schema: ResultSchema,
                               config: SamplesScannedConfig): Unit = {
    // Exit early if there are no counters to update.
    if (samplesScannedCounters.isEmpty) {
      return
    }

    val valueColumnType = ResultSchema.valueColumnType(schema)
    val rowSamples = estimateNumRows(childRv) *
      config.valueColumnToRowMultiplier.getOrElse(valueColumnType, 1.0) *
      config.classToSamplesPerChildRow.getOrElse(parentClass, config.defaultSamplesPerChildRow)

    val seriesSamples = config.classToSamplesPerChildSeries.getOrElse(
      parentClass, config.defaultSamplesPerChildSeries)

    val partKeySamples = childRv.key.keySize *
      config.classToSamplesPerChildPartKeyByte.getOrElse(parentClass, config.defaultSamplesPerChildPartKeyByte)

    val totalSamples = Math.ceil(
      rowSamples + seriesSamples + partKeySamples
    ).asInstanceOf[Long]

    val samplesPerCounter = Math.ceil(
      totalSamples.asInstanceOf[Double] / samplesScannedCounters.size
    ).asInstanceOf[Long]
    samplesScannedCounters.foreach(ctr => ctr.addAndGet(samplesPerCounter))
  }
}
