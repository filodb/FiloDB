package filodb.gateway.conversion

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.DatasetOptions
import filodb.memory.BinaryRegion

/**
 * Base trait for common shard calculation and debug logic for all Influx Line Protocol based records for FiloDB
 */
trait InfluxRecord extends InputRecord {
  def bytes: Array[Byte]
  def kpiLen: Int
  def tagDelims: Buffer[Int]
  def fieldDelims: Buffer[Int]
  def fieldEnd: Int
  def dsOptions: DatasetOptions
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
        if (nonMetricIndex < dsOptions.nonMetricShardColumns.length) {
          val keyToCompare = dsOptions.nonMetricShardKeyBytes(nonMetricIndex)
          if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, keyToCompare)) {
            // key match.  Add value to nonMetricShardValues
            nonMetricShardValues += new String(bytes, valueIndex, valueLen)

            // calculate hash too
            tagsShardHash = RecordBuilder.combineHash(tagsShardHash, dsOptions.nonMetricShardKeyHash(nonMetricIndex))
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
 * These are not histograms/summaries so no stripping of metric name and separate calculation
 * of shard hash is needed.
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
                                        ts: Long,
                                        dsOptions: DatasetOptions) extends InfluxRecord {
  require(fieldDelims.length == 1, s"Cannot use ${getClass.getName} with fieldDelims of length ${fieldDelims.length}")
  final def addToBuilder(builder: RecordBuilder): Unit = {
    // Add the timestamp and value first
    builder.startNewRecord()
    builder.addLong(ts)
    InfluxProtocolParser.parseKeyValues(bytes, fieldDelims, fieldEnd, new SimpleDoubleAdder(builder))

    // Now start the map and add the metric name / __name__
    builder.startMap()
    builder.addMapKeyValueHash(dsOptions.metricBytes, dsOptions.metricHash,
                               bytes, 0, kpiLen)

    // Now add all the tags, and don't forget the hash
    InfluxProtocolParser.parseKeyValues(bytes, tagDelims, endOfTags, new MapBuilderVisitor(builder))
    builder.updatePartitionHash(partitionKeyHash)

    builder.endMap()
    builder.endRecord()
  }
}

object InfluxPromHistogramRecord extends StrictLogging {
  val sumSuffix = "sum".getBytes
  val countSuffix = "count".getBytes
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
                                suffix: Array[Byte],
                                dsOptions: DatasetOptions): Unit = {
    BinaryRegion.copyArray(suffix, metricBuffer, baseMetricLen + 1)
    builder.addMapKeyValueHash(dsOptions.metricBytes, dsOptions.metricHash,
                               metricBuffer, 0, baseMetricLen + 1 + suffix.size)
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

// For each field in a histogram / multi field Influx record, create a separate BinaryRecord
// Assumes the base metric name has been copied to the thread local metricBuffer
// Add prometheus style time series with le= the bucket name.
class HistogramPromFieldVisitor(builder: RecordBuilder,
                                ts: Long,
                                baseMetricLen: Int,
                                tagDelims: Buffer[Int],
                                endOfTags: Int,
                                partKeyHash: Int,
                                dsOptions: DatasetOptions) extends InfluxFieldVisitor {
  import InfluxPromHistogramRecord._

  val tagsBuilderVisitor = new MapBuilderVisitor(builder)

  def doubleValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, value: Double): Unit = {
    builder.startNewRecord()
    builder.addLong(ts)
    builder.addDouble(value)
    builder.startMap()

    // Add original tags....  TODO: somehow make this more efficient
    InfluxProtocolParser.parseKeyValues(bytes, tagDelims, endOfTags, tagsBuilderVisitor)
    builder.updatePartitionHash(partKeyHash)

    // Is the key sum or count?  If so, append that to metric name
    if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, sumSuffix)) {
      addSuffixToMetricAndBuild(builder, baseMetricLen, sumSuffix, dsOptions)
    } else if (BinaryRegion.equalBytes(bytes, keyIndex, keyLen, countSuffix)) {
      addSuffixToMetricAndBuild(builder, baseMetricLen, countSuffix, dsOptions)
    // Otherwise, add _bucket to metric name and add le= with the key
    } else {
      addSuffixToMetricAndBuild(builder, baseMetricLen, bucketSuffix, dsOptions)
      builder.addMapKeyValueHash(leKey, leHash, bytes, keyIndex, keyLen)
    }

    builder.endMap()
    builder.endRecord()
  }

  def stringValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueOffset: Int, valueLen: Int): Unit = {
    _log.warn(s"Got non numeric field in histogram record: key=${new String(bytes, keyIndex, keyLen)} " +
                s"value=[${new String(bytes, valueOffset, valueLen)}]")
  }
}

/**
 * A Prom histogram outputted using Influx Line Protocol.  One record contains multiple fileds, one for each bucket
 * plus sum and count.
 * Much more efficient than Prom WriteRequest since it only has to compute shard hashes once.
 * For now we have to translate each field to its own BinaryRecord.
 * In the future make this more efficient by encoding as just one record.
 */
final case class InfluxPromHistogramRecord(bytes: Array[Byte],
                                           kpiLen: Int,
                                           tagDelims: Buffer[Int],
                                           fieldDelims: Buffer[Int],
                                           fieldEnd: Int,
                                           ts: Long,
                                           dsOptions: DatasetOptions) extends InfluxRecord {
  final def addToBuilder(builder: RecordBuilder): Unit = {
    // do some preprocessing: copy metric name to the thread local buffer
    InfluxPromHistogramRecord.copyMetricToBuffer(bytes, kpiLen)

    // For each field, append one BinaryRecord
    val visitor = new HistogramPromFieldVisitor(builder, ts, kpiLen, tagDelims, endOfTags, partitionKeyHash, dsOptions)
    InfluxProtocolParser.parseKeyValues(bytes, fieldDelims, fieldEnd, visitor)
  }
}
