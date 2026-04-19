package filodb.core.memstore.aggregation

import org.agrona.DirectBuffer

import filodb.memory.format.vectors.{BinaryHistogram, Histogram, HistogramWithBuckets, MutableHistogram}

/**
 * Trait for aggregating values within a time bucket.
 * Each implementation defines how values are combined (sum, avg, min, max, etc.).
 */
trait Aggregator {
  /**
   * Adds a value to the aggregation.
   * @param value the value to add (can be Double, Long, Int, or Float)
   */
  def add(value: Any): Unit

  /** Specialized add for Double values - avoids boxing. Default delegates to add(). */
  def addDouble(value: Double): Unit = add(value)

  /** Specialized add for Long values - avoids boxing. Default delegates to add(). */
  def addLong(value: Long): Unit = add(value)

  /**
   * Adds a value with its timestamp for time-sensitive aggregations (Last, First).
   * Default implementation ignores timestamp.
   * @param value the value to add
   * @param timestamp the timestamp of the value
   */
  def addWithTimestamp(value: Any, timestamp: Long): Unit = add(value)

  /** Specialized addWithTimestamp for Double - avoids boxing. */
  def addDoubleWithTimestamp(value: Double, timestamp: Long): Unit = addDouble(value)

  /** Specialized addWithTimestamp for Long - avoids boxing. */
  def addLongWithTimestamp(value: Long, timestamp: Long): Unit = addLong(value)

  /**
   * Returns the aggregated result.
   * @return the aggregated value, or Double.NaN if no values were added
   */
  def result(): Any

  /**
   * Resets the aggregator state to initial condition.
   */
  def reset(): Unit

  /**
   * Creates a copy of this aggregator with clean state.
   * Used for creating new buckets from templates.
   * @return a new instance of the same aggregator type
   */
  def copy(): Aggregator
}

/**
 * Sum aggregator - adds all numeric values.
 */
class SumAggregator extends Aggregator {
  private var sum: Double = 0.0
  private var count: Int = 0

  def add(value: Any): Unit = {
    value match {
      case d: Double if !d.isNaN && !d.isInfinity => sum += d; count += 1
      case l: Long                                 => sum += l.toDouble; count += 1
      case i: Int                                  => sum += i.toDouble; count += 1
      case f: Float if !f.isNaN && !f.isInfinity   => sum += f.toDouble; count += 1
      case _                                       => // ignore non-numeric or invalid values
    }
  }

  override def addDouble(value: Double): Unit =
    if (!value.isNaN && !value.isInfinity) { sum += value; count += 1 }

  override def addLong(value: Long): Unit = { sum += value.toDouble; count += 1 }

  def result(): Any = if (count > 0) sum else Double.NaN

  def reset(): Unit = {
    sum = 0.0
    count = 0
  }

  def copy(): Aggregator = new SumAggregator
}

/**
 * Average aggregator - computes mean of all numeric values.
 */
class AvgAggregator extends Aggregator {
  private var sum: Double = 0.0
  private var count: Int = 0

  def add(value: Any): Unit = {
    value match {
      case d: Double if !d.isNaN && !d.isInfinity => sum += d; count += 1
      case l: Long                                 => sum += l.toDouble; count += 1
      case i: Int                                  => sum += i.toDouble; count += 1
      case f: Float if !f.isNaN && !f.isInfinity   => sum += f.toDouble; count += 1
      case _                                       => // ignore
    }
  }

  override def addDouble(value: Double): Unit =
    if (!value.isNaN && !value.isInfinity) { sum += value; count += 1 }

  override def addLong(value: Long): Unit = { sum += value.toDouble; count += 1 }

  def result(): Any = if (count > 0) sum / count else Double.NaN

  def reset(): Unit = {
    sum = 0.0
    count = 0
  }

  def copy(): Aggregator = new AvgAggregator
}

/**
 * Minimum aggregator - finds the smallest numeric value.
 */
class MinAggregator extends Aggregator {
  private var min: Double = Double.MaxValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    val d = value match {
      case d: Double if !d.isNaN && !d.isInfinity => d
      case l: Long                                 => l.toDouble
      case i: Int                                  => i.toDouble
      case f: Float if !f.isNaN && !f.isInfinity   => f.toDouble
      case _                                       => return
    }
    if (!initialized || d < min) {
      min = d
      initialized = true
    }
  }

  override def addDouble(value: Double): Unit =
    if (!value.isNaN && !value.isInfinity) {
      if (!initialized || value < min) { min = value; initialized = true }
    }

  override def addLong(value: Long): Unit = {
    val d = value.toDouble
    if (!initialized || d < min) { min = d; initialized = true }
  }

  def result(): Any = if (initialized) min else Double.NaN

  def reset(): Unit = {
    min = Double.MaxValue
    initialized = false
  }

  def copy(): Aggregator = new MinAggregator
}

/**
 * Maximum aggregator - finds the largest numeric value.
 */
class MaxAggregator extends Aggregator {
  private var max: Double = Double.MinValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    val d = value match {
      case d: Double if !d.isNaN && !d.isInfinity => d
      case l: Long                                 => l.toDouble
      case i: Int                                  => i.toDouble
      case f: Float if !f.isNaN && !f.isInfinity   => f.toDouble
      case _                                       => return
    }
    if (!initialized || d > max) {
      max = d
      initialized = true
    }
  }

  override def addDouble(value: Double): Unit =
    if (!value.isNaN && !value.isInfinity) {
      if (!initialized || value > max) { max = value; initialized = true }
    }

  override def addLong(value: Long): Unit = {
    val d = value.toDouble
    if (!initialized || d > max) { max = d; initialized = true }
  }

  def result(): Any = if (initialized) max else Double.NaN

  def reset(): Unit = {
    max = Double.MinValue
    initialized = false
  }

  def copy(): Aggregator = new MaxAggregator
}

/**
 * Last aggregator - keeps the value with the most recent timestamp.
 */
class LastAggregator extends Aggregator {
  private var lastValue: Double = Double.NaN
  private var lastTimestamp: Long = Long.MinValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    // Without timestamp, treat as "last seen in order of addition"
    value match {
      case d: Double => lastValue = d; initialized = true
      case l: Long   => lastValue = l.toDouble; initialized = true
      case i: Int    => lastValue = i.toDouble; initialized = true
      case f: Float  => lastValue = f.toDouble; initialized = true
      case _         => // ignore
    }
  }

  override def addDouble(value: Double): Unit = { lastValue = value; initialized = true }

  override def addLong(value: Long): Unit = { lastValue = value.toDouble; initialized = true }

  override def addWithTimestamp(value: Any, timestamp: Long): Unit = {
    if (timestamp >= lastTimestamp) {
      value match {
        case d: Double => lastValue = d; lastTimestamp = timestamp; initialized = true
        case l: Long   => lastValue = l.toDouble; lastTimestamp = timestamp; initialized = true
        case i: Int    => lastValue = i.toDouble; lastTimestamp = timestamp; initialized = true
        case f: Float  => lastValue = f.toDouble; lastTimestamp = timestamp; initialized = true
        case _         => // ignore
      }
    }
  }

  override def addDoubleWithTimestamp(value: Double, timestamp: Long): Unit =
    if (timestamp >= lastTimestamp) { lastValue = value; lastTimestamp = timestamp; initialized = true }

  override def addLongWithTimestamp(value: Long, timestamp: Long): Unit =
    if (timestamp >= lastTimestamp) { lastValue = value.toDouble; lastTimestamp = timestamp; initialized = true }

  def result(): Any = lastValue

  def reset(): Unit = {
    lastValue = Double.NaN
    lastTimestamp = Long.MinValue
    initialized = false
  }

  def copy(): Aggregator = new LastAggregator
}

/**
 * First aggregator - keeps the value with the earliest timestamp.
 */
class FirstAggregator extends Aggregator {
  private var firstValue: Double = Double.NaN
  private var firstTimestamp: Long = Long.MaxValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    // Without timestamp, treat as "first seen in order of addition"
    if (!initialized) {
      value match {
        case d: Double => firstValue = d; initialized = true
        case l: Long   => firstValue = l.toDouble; initialized = true
        case i: Int    => firstValue = i.toDouble; initialized = true
        case f: Float  => firstValue = f.toDouble; initialized = true
        case _         => // ignore
      }
    }
  }

  override def addDouble(value: Double): Unit =
    if (!initialized) { firstValue = value; initialized = true }

  override def addLong(value: Long): Unit =
    if (!initialized) { firstValue = value.toDouble; initialized = true }

  override def addWithTimestamp(value: Any, timestamp: Long): Unit = {
    if (timestamp < firstTimestamp) {
      value match {
        case d: Double => firstValue = d; firstTimestamp = timestamp; initialized = true
        case l: Long   => firstValue = l.toDouble; firstTimestamp = timestamp; initialized = true
        case i: Int    => firstValue = i.toDouble; firstTimestamp = timestamp; initialized = true
        case f: Float  => firstValue = f.toDouble; firstTimestamp = timestamp; initialized = true
        case _         => // ignore
      }
    }
  }

  override def addDoubleWithTimestamp(value: Double, timestamp: Long): Unit =
    if (timestamp < firstTimestamp) { firstValue = value; firstTimestamp = timestamp; initialized = true }

  override def addLongWithTimestamp(value: Long, timestamp: Long): Unit =
    if (timestamp < firstTimestamp) { firstValue = value.toDouble; firstTimestamp = timestamp; initialized = true }

  def result(): Any = firstValue

  def reset(): Unit = {
    firstValue = Double.NaN
    firstTimestamp = Long.MaxValue
    initialized = false
  }

  def copy(): Aggregator = new FirstAggregator
}

/**
 * Count aggregator - counts the number of samples.
 */
class CountAggregator extends Aggregator {
  private var count: Long = 0

  def add(value: Any): Unit = {
    // Count all non-null values
    if (value != null) {
      count += 1
    }
  }

  override def addDouble(value: Double): Unit = { count += 1 }

  override def addLong(value: Long): Unit = { count += 1 }

  def result(): Any = count

  def reset(): Unit = {
    count = 0
  }

  def copy(): Aggregator = new CountAggregator
}

/**
 * Histogram aggregator - accumulates histogram values using MutableHistogram.
 * For out-of-order samples, histograms are added together within the same time bucket.
 *
 * This aggregator handles:
 * - DirectBuffer (BinaryHistogram format) from ingestion
 * - HistogramWithBuckets for in-memory representations
 * - Automatic bucket schema handling (schemas must match within a bucket)
 *
 * Use for delta temporality histograms where values represent incremental changes.
 */
class HistogramAggregator extends Aggregator {
  private var accumulator: Option[MutableHistogram] = None
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    value match {
      case buf: DirectBuffer =>
        // Convert BinaryHistogram to HistogramWithBuckets
        val binHist = BinaryHistogram.BinHistogram(buf)
        val hist = binHist.toHistogram

        accumulator match {
          case Some(acc) =>
            // Add to existing accumulator
            acc.add(hist)
          case None =>
            // Initialize accumulator with first histogram
            accumulator = Some(MutableHistogram(hist))
            initialized = true
        }

      case h: HistogramWithBuckets =>
        accumulator match {
          case Some(acc) =>
            // Add to existing accumulator
            acc.add(h)
          case None =>
            // Initialize accumulator from histogram
            accumulator = Some(MutableHistogram(h))
            initialized = true
        }

      case _ =>
        // Ignore non-histogram values (including Histogram without buckets)
    }
  }

  def result(): Any = accumulator match {
    case Some(hist) =>
      // Serialize to DirectBuffer for storage
      hist.serialize()
    case None =>
      // Return empty histogram buffer - Histogram.empty.serialize() already returns the buffer
      filodb.memory.format.vectors.Histogram.empty.serialize()
  }

  def reset(): Unit = {
    accumulator = None
    initialized = false
  }

  def copy(): Aggregator = new HistogramAggregator

  /**
   * Gets the current accumulated MutableHistogram.
   * Useful for direct access without serialization.
   */
  def getAccumulator: Option[MutableHistogram] = accumulator
}

/**
 * Histogram last aggregator - keeps the most recent histogram by timestamp.
 * For out-of-order cumulative histograms, only the sample with the latest timestamp is kept.
 *
 * This aggregator handles:
 * - DirectBuffer (BinaryHistogram format) from ingestion
 * - HistogramWithBuckets for in-memory representations
 * - Timestamp-based selection (requires addWithTimestamp)
 *
 * Use for cumulative temporality histograms (like Prometheus) where values are
 * monotonically increasing counters.
 */
class HistogramLastAggregator extends Aggregator {
  private var currentHistogram: Option[MutableHistogram] = None
  private var currentTimestamp: Long = Long.MinValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    // Without timestamp, treat as "last seen in order of addition"
    value match {
      case buf: DirectBuffer =>
        val binHist = BinaryHistogram.BinHistogram(buf)
        val hist = binHist.toHistogram
        currentHistogram = Some(MutableHistogram(hist))
        initialized = true

      case h: HistogramWithBuckets =>
        currentHistogram = Some(MutableHistogram(h))
        initialized = true

      case h: Histogram =>
        h match {
          case hwb: HistogramWithBuckets =>
            currentHistogram = Some(MutableHistogram(hwb))
            initialized = true
          case _ =>
            // Cannot handle non-HistogramWithBuckets types
        }

      case _ =>
        // Ignore non-histogram values
    }
  }

  override def addWithTimestamp(value: Any, timestamp: Long): Unit = {
    // Only keep histogram if it has a later timestamp
    if (timestamp >= currentTimestamp) {
      value match {
        case buf: DirectBuffer =>
          val binHist = BinaryHistogram.BinHistogram(buf)
          val hist = binHist.toHistogram
          currentHistogram = Some(MutableHistogram(hist))
          currentTimestamp = timestamp
          initialized = true

        case h: HistogramWithBuckets =>
          currentHistogram = Some(MutableHistogram(h))
          currentTimestamp = timestamp
          initialized = true

        case h: Histogram =>
          h match {
            case hwb: HistogramWithBuckets =>
              currentHistogram = Some(MutableHistogram(hwb))
              currentTimestamp = timestamp
              initialized = true
            case _ =>
              // Cannot handle non-HistogramWithBuckets types
          }

        case _ =>
          // Ignore non-histogram values
      }
    }
  }

  def result(): Any = currentHistogram match {
    case Some(hist) =>
      // Serialize to DirectBuffer for storage
      hist.serialize()
    case None =>
      // Return empty histogram buffer - Histogram.empty.serialize() already returns the buffer
      filodb.memory.format.vectors.Histogram.empty.serialize()
  }

  def reset(): Unit = {
    currentHistogram = None
    currentTimestamp = Long.MinValue
    initialized = false
  }

  def copy(): Aggregator = new HistogramLastAggregator

  /**
   * Gets the current histogram.
   * Useful for direct access without serialization.
   */
  def getCurrentHistogram: Option[MutableHistogram] = currentHistogram
}

/**
 * Factory for creating aggregators by type.
 */
object Aggregator {
  /**
   * Creates an aggregator instance for the given aggregation type.
   * @param aggType the aggregation type
   * @return a new aggregator instance
   */
  def create(aggType: AggregationType): Aggregator = aggType match {
    case AggregationType.Sum          => new SumAggregator
    case AggregationType.Avg          => new AvgAggregator
    case AggregationType.Min          => new MinAggregator
    case AggregationType.Max          => new MaxAggregator
    case AggregationType.Last         => new LastAggregator
    case AggregationType.First        => new FirstAggregator
    case AggregationType.Count        => new CountAggregator
    case AggregationType.HistogramSum => new HistogramAggregator
    case AggregationType.HistogramLast => new HistogramLastAggregator
  }
}
