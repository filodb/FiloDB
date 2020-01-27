package filodb.gateway.conversion

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Schema, Schemas}
import filodb.memory.BinaryRegion
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

/**
 * Base trait for common shard calculation and debug logic for all Influx Line Protocol based records for FiloDB
 */
trait InfluxRecord extends InputRecord {
  def bytes: Array[Byte]
  def kpiLen: Int
  def tagDelims: Buffer[Int]
  def fieldDelims: Buffer[Int]
  def fieldEnd: Int
  def schema: Schema
  def ts: Long

  import InfluxProtocolParser._
  protected def endOfTags: Int = keyOffset(fieldDelims(0)) - 1

  // Iterate through the tags for shard keys, extract values and calculate shard hash
  private var tagsShardHash = 7
  val nonMetricShardValues = new collection.mutable.ArrayBuffer[String]

  {
    var nonMetricIndex = 0
    parseKeyValues(bytes, tagDelims, endOfTags, new KVVisitor {
      def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit = {
        if (nonMetricIndex < schema.options.nonMetricShardColumns.length) {
          val keyToCompare = schema.options.nonMetricShardKeyBytes(nonMetricIndex)
          if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, keyToCompare)) {
            // key match.  Add value to nonMetricShardValues
            nonMetricShardValues += new String(bytes, valueIndex, valueLen)

            // calculate hash too
            nonMetricIndex += 1
            val valueHash = BinaryRegion.hasher32.hash(bytes, valueIndex, valueLen, BinaryRegion.Seed)
            tagsShardHash = RecordBuilder.combineHash(tagsShardHash, valueHash)
          }
        }
      }
    })
  }

  // WARNING: lots of allocation happening here
  override def toString: String = {
    s"""{
        |  measurement: ${new String(bytes, 0, kpiLen)}
        |  ${tagDelims.length} tags:
        |${debugKeyValues(bytes, tagDelims, endOfTags)}
        |  fields:
        |${debugKeyValues(bytes, fieldDelims, fieldEnd)}
        |  time: $ts (${new org.joda.time.DateTime(ts)})
        |}""".stripMargin
  }

  final def getMetric: String = new String(bytes, 0, kpiLen)

  final def shardKeyHash: Int = {
    val kpiHash = BinaryRegion.hasher32.hash(bytes, 0, kpiLen, BinaryRegion.Seed)
    RecordBuilder.combineHash(tagsShardHash, kpiHash)
  }

  // since they are sorted, just hash the entire tags together including delimiters
  val partitionKeyHash = {
    val firstTagIndex = keyOffset(tagDelims(0))
    BinaryRegion.hasher32.hash(bytes, firstTagIndex, endOfTags - firstTagIndex, BinaryRegion.Seed)
  }
}

// Adds the parsed double, ignoring the field name, to the RecordBuilder, or NaN if it is not a double
class SimpleDoubleAdder(builder: RecordBuilder) extends InfluxFieldVisitor {
  def doubleValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, value: Double): Unit =
    builder.addDouble(value)
  def stringValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueOffset: Int, valueLen: Int): Unit =
    builder.addDouble(Double.NaN)
}

/**
 * A Prom counter or gauge outputted using Telegraf Influx format with just one field
 * NOTE: Telegraf always sorts the tags so we don't need to do this
 *
 * We deduce counter or gauge based on the field name.  Counters will have "counter" as the field name.
 *
 * @param bytes unescaped(parsed) bytes from raw text bytes
 * @param kpiLen the number of bytes taken by the KPI or Influx "measurement" field
 * @param tagDelims Buffer of tag delimiter offsets, one per tag value
 * @param fieldOffset byte array index of field key=value pair
 * @param fieldEnd the end offset of the fields
 * @param ts the UNIX epoch Long timestamp from the Influx record
 */
final case class InfluxPromSingleRecord(bytes: Array[Byte],
                                        kpiLen: Int,
                                        tagDelims: Buffer[Int],
                                        fieldDelims: Buffer[Int],
                                        fieldEnd: Int,
                                        ts: Long) extends InfluxRecord {
  require(fieldDelims.length == 1, s"Cannot use ${getClass.getName} with fieldDelims of length ${fieldDelims.length}")
  final def addToBuilder(builder: RecordBuilder): Unit = {
    // Add the timestamp and value first
    builder.startNewRecord(schema)
    builder.addLong(ts)
    InfluxProtocolParser.parseKeyValues(bytes, fieldDelims, fieldEnd, new SimpleDoubleAdder(builder))

    // Add metric name, then the map/tags
    builder.addBlob(bytes, UnsafeUtils.arayOffset, kpiLen)
    builder.startMap()
    InfluxProtocolParser.parseKeyValues(bytes, tagDelims, endOfTags, new MapBuilderVisitor(builder))
    builder.updatePartitionHash(partitionKeyHash)

    builder.endMap(false)
    builder.endRecord()
  }

  lazy val schema = {
    val counter = InfluxProtocolParser.firstKeyEquals(bytes, fieldDelims, InfluxProtocolParser.CounterKey)
    if (counter) Schemas.promCounter else Schemas.gauge
  }
}

object InfluxHistogramRecord extends StrictLogging {
  val sumLabel = "sum".getBytes
  val countLabel = "count".getBytes
  val infLabel = "+Inf".getBytes
  val leKey = "le".getBytes
  val leHash = BinaryRegion.hash32(leKey)
  val bucketSuffix = "bucket".getBytes
  val Underscore = '_'.toByte

  def copyMetricToBuffer(sourceBytes: Array[Byte], metricLen: Int): Unit = {
    // Only copy enough of metric to fit in underscore and longest suffix into metric buffer
    val bytesToCopy = Math.min(metricBufferSize - 1 - bucketSuffix.size, metricLen)
    System.arraycopy(sourceBytes, 0, metricBuffer, 0, bytesToCopy)
    metricBuffer(bytesToCopy) = Underscore
  }

  val _log = logger

  def addSuffixToMetricAndBuild(builder: RecordBuilder,
                                baseMetricLen: Int,
                                suffix: Array[Byte]): Unit = {
    BinaryRegion.copyArray(suffix, metricBuffer, baseMetricLen + 1)
    builder.addBlob(metricBuffer, UnsafeUtils.arayOffset, baseMetricLen + 1 + suffix.size)
  }

  // Per-thread buffer for metric name mangling
  val metricBufferLocal = new ThreadLocal[Array[Byte]]()
  val metricBufferSize = 256

  def metricBuffer: Array[Byte] = {
    //scalastyle:off
    metricBufferLocal.get match {
      case null =>
        val newBuf = new Array[Byte](metricBufferSize)
        metricBufferLocal.set(newBuf)
        newBuf
      case buffer: Array[Byte] => buffer
    }
    //scalastyle:on
  }
}

// Parses and sorts fields assuming they are buckets for histograms, to prepare for histogram
// encoding and writing to BinaryRecord.  The sum and count are also extracted.
// To conserve memory, we keep arrays and do sorted insertion in place
class HistogramFieldVisitor(numFields: Int) extends InfluxFieldVisitor {
  import InfluxHistogramRecord._

  require(numFields >= 3, s"Not enough fields ($numFields) for histogram schema")

  var gotInf = false
  var sum = Double.NaN
  var count = Double.NaN
  val bucketTops = new Array[Double](numFields - 2)
  val bucketVals = new Array[Long](numFields - 2)
  var numBuckets = 0

  def doubleValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, value: Double): Unit = {
    if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, sumLabel)) {
      sum = value
    } else if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, countLabel)) {
      count = value
    } else {
      // Assume it is a bucket.  Convert the field bytes to a number
      val top = if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, infLabel)) {
                  gotInf = true
                  Double.PositiveInfinity
                } else { InfluxProtocolParser.parseDouble(bytes, keyIndex, keyLen) }
      // Find position to insert top and value in bucket.  Buckets must be sorted
      val pos = if (numBuckets == 0) 0
                else {
                  val binSearchRes = java.util.Arrays.binarySearch(bucketTops, 0, numBuckets, top)
                  if (binSearchRes < 0) (-binSearchRes - 1) else binSearchRes
                }
      // insert/shift over array elements and insert
      assert(numBuckets < (numFields - 2))
      if (numBuckets > pos) {
        System.arraycopy(bucketTops, pos, bucketTops, pos + 1, numBuckets - pos)
        System.arraycopy(bucketVals, pos, bucketVals, pos + 1, numBuckets - pos)
      }
      bucketTops(pos) = top
      bucketVals(pos) = value.toLong
      numBuckets += 1
    }
  }

  def stringValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueOffset: Int, valueLen: Int): Unit = {
    _log.warn(s"Got non numeric field in histogram record: key=${new String(bytes, keyIndex, keyLen)} " +
      s"value=[${new String(bytes, valueOffset, valueLen)}]\nline=[${new String(bytes, 0, valueOffset + valueLen)}]")
  }
}

/**
 * A histogram outputted using Influx Line Protocol.  One record contains multiple fields, one for each bucket
 * plus sum and count.
 * Much more efficient than Prom WriteRequest since it only has to compute shard hashes once.
 * Will be encoded as a single efficient BinaryRecord using FiloDB histogram schema.
 */
final case class InfluxHistogramRecord(bytes: Array[Byte],
                                       kpiLen: Int,
                                       tagDelims: Buffer[Int],
                                       fieldDelims: Buffer[Int],
                                       fieldEnd: Int,
                                       ts: Long) extends InfluxRecord {
  final def addToBuilder(builder: RecordBuilder): Unit = {
    // do some preprocessing: copy metric name to the thread local buffer
    // TODO: this would be needed in case it is not a histogram and we need to write a summary
    // InfluxPromHistogramRecord.copyMetricToBuffer(bytes, kpiLen)

    // Parse sorted buckets, sum, count from fields
    val visitor = new HistogramFieldVisitor(fieldDelims.length)
    InfluxProtocolParser.parseKeyValues(bytes, fieldDelims, fieldEnd, visitor)

    // Only create histogram record if we are able to parse above and it contains +Inf bucket
    // This also ensures that it's not a blank histogram, which cannot be ingested
    if (visitor.gotInf) {
      val buckets = CustomBuckets(visitor.bucketTops)
      val hist = LongHistogram(buckets, visitor.bucketVals)

      // Now, write out histogram
      builder.startNewRecord(Schemas.promHistogram)
      builder.addLong(ts)
      builder.addDouble(visitor.sum)
      builder.addDouble(visitor.count)
      builder.addBlob(hist.serialize())

      // Add metric name, then the map/tags
      builder.addBlob(bytes, UnsafeUtils.arayOffset, kpiLen)
      builder.startMap()
      InfluxProtocolParser.parseKeyValues(bytes, tagDelims, endOfTags, new MapBuilderVisitor(builder))
      builder.updatePartitionHash(partitionKeyHash)

      builder.endMap(false)
      builder.endRecord()
    }
  }

  def schema: Schema = Schemas.promHistogram
}
