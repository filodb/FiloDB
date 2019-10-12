package filodb.core.query

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import org.joda.time.DateTime

import filodb.core.binaryrecord2.{MapItemConsumer, RecordBuilder, RecordContainer, RecordSchema}
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType._
import filodb.core.store._
import filodb.memory.{MemFactory, UTF8StringMedium, UTF8StringShort}
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => UTF8Str}

/**
  * Identifier for a single RangeVector.
  * Sub-classes must be a case class or override equals/hashcode since this class is used in a
  * hash table.
  */
trait RangeVectorKey extends java.io.Serializable {
  def labelValues: Map[UTF8Str, UTF8Str]
  def sourceShards: Seq[Int]
  def partIds: Seq[Int]
  override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
}

class SeqMapConsumer extends MapItemConsumer {
  val pairs = new collection.mutable.ArrayBuffer[(UTF8Str, UTF8Str)]
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    val keyUtf8 = new UTF8Str(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
    val valUtf8 = new UTF8Str(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset))
    pairs += (keyUtf8 -> valUtf8)
  }
}

/**
  * Range Vector Key backed by a BinaryRecord v2 partition key, which is basically a pointer to memory on or offheap.
  */
final case class PartitionRangeVectorKey(partBase: Array[Byte],
                                         partOffset: Long,
                                         partSchema: RecordSchema,
                                         partKeyCols: Seq[ColumnInfo],
                                         sourceShard: Int,
                                         groupNum: Int,
                                         partId: Int) extends RangeVectorKey {
  override def sourceShards: Seq[Int] = Seq(sourceShard)
  override def partIds: Seq[Int] = Seq(partId)
  def labelValues: Map[UTF8Str, UTF8Str] = {
    partKeyCols.zipWithIndex.flatMap { case (c, pos) =>
      c.colType match {
        case StringColumn => Seq(UTF8Str(c.name) -> partSchema.asZCUTF8Str(partBase, partOffset, pos))
        case IntColumn    => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getInt(partBase, partOffset, pos).toString))
        case LongColumn   => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getLong(partBase, partOffset, pos).toString))
        case MapColumn    => val consumer = new SeqMapConsumer
          partSchema.consumeMapItems(partBase, partOffset, pos, consumer)
          consumer.pairs
        case _            => throw new UnsupportedOperationException("Not supported yet")
      }
    }.toMap
  }
  override def toString: String = s"/shard:$sourceShard/${partSchema.stringify(partBase, partOffset)} [grp$groupNum]"
}

final case class CustomRangeVectorKey(labelValues: Map[UTF8Str, UTF8Str],
                                      sourceShards: Seq[Int] = Nil,
                                      partIds: Seq[Int] = Nil)
  extends RangeVectorKey {
}

object CustomRangeVectorKey {
  def fromZcUtf8(str: UTF8Str): CustomRangeVectorKey = {
    CustomRangeVectorKey(str.asNewString.split("\u03BC").map(_.split("\u03C0")).filter(_.length == 2).map { lv =>
      UTF8Str(lv(0)) -> UTF8Str(lv(1))
    }.toMap)
  }

  def toZcUtf8(rvk: RangeVectorKey): UTF8Str = {
    // TODO can we optimize this further? Can we use a binary field in the row-reader ?
    val str = rvk.labelValues.toSeq.map(lv=>s"${lv._1.asNewString}\u03C0${lv._2.asNewString}").sorted.mkString("\u03BC")
    UTF8Str(str)
  }

  val empty = CustomRangeVectorKey(Map.empty)
  val emptyAsZcUtf8 = toZcUtf8(empty)
}


object RangeVector {
  val timestampColID = 0
}
/**
  * Represents a single result of any FiloDB Query.
  */
trait RangeVector {
  def key: RangeVectorKey
  def rows: Iterator[RowReader]
  def numRows: Option[Int] = None
  def prettyPrint(formatTime: Boolean = true): String = "RV String Not supported"
}

// First column of columnIDs should be the timestamp column
final case class RawDataRangeVector(key: RangeVectorKey,
                                    val partition: ReadablePartition,
                                    chunkMethod: ChunkScanMethod,
                                    columnIDs: Array[Int]) extends RangeVector {
  // Iterators are stateful, for correct reuse make this a def
  def rows: Iterator[RowReader] = partition.timeRangeRows(chunkMethod, columnIDs)

  // Obtain ChunkSetInfos from specific window of time from partition
  def chunkInfos(windowStart: Long, windowEnd: Long): ChunkInfoIterator = partition.infos(windowStart, windowEnd)

  // the query engine is based around one main data column to query, so it will always be the second column passed in
  def valueColID: Int = columnIDs(1)
}

/**
  * A RangeVector designed to return one row per ChunkSetInfo, with the following schema:
  * ID (Long), NumRows (Int), startTime (Long), endTime (Long), numBytes(I) of chunk, readerclass of chunk
  * @param column the Column to return detailed chunk info about, must be a DataColumn
  */
final case class ChunkInfoRangeVector(key: RangeVectorKey,
                                      partition: ReadablePartition,
                                      chunkMethod: ChunkScanMethod,
                                      column: Column) extends RangeVector {
  val reader = new ChunkInfoRowReader(column)
  // Iterators are stateful, for correct reuse make this a def
  def rows: Iterator[RowReader] = partition.infos(chunkMethod).map { info =>
    reader.setInfo(info)
    reader
  }
}

/**
  * SerializableRangeVector represents a RangeVector that can be serialized over the wire.
  * RecordContainers may be shared amongst all the SRV's from a single Result to minimize space and heap usage --
  *   this is the reason for the startRecordNo, the row # of the first container.
  * PLEASE PLEASE use Kryo to serialize this as it will make sure the single shared RecordContainer is
  * only serialized once as a single instance.
  */
final class SerializableRangeVector(val key: RangeVectorKey,
                                    val numRowsInt: Int,
                                    containers: Seq[RecordContainer],
                                    val schema: RecordSchema,
                                    startRecordNo: Int) extends RangeVector with java.io.Serializable {

  override val numRows = Some(numRowsInt)

  // Possible for records to spill across containers, so we read from all containers
  override def rows: Iterator[RowReader] =
    containers.toIterator.flatMap(_.iterate(schema)).drop(startRecordNo).take(numRowsInt)

  /**
    * Pretty prints all the elements into strings using record schema
    */
  override def prettyPrint(formatTime: Boolean = true): String = {
    val curTime = System.currentTimeMillis
    key.toString + "\n\t" +
      rows.map {
        case reader =>
          val firstCol = if (formatTime && schema.isTimeSeries) {
            val timeStamp = reader.getLong(0)
            s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago) $timeStamp"
          } else {
            schema.columnTypes(0) match {
              case BinaryRecordColumn => schema.stringify(reader.getBlobBase(0), reader.getBlobOffset(0))
              case _ => reader.getAny(0).toString
            }
          }
          (firstCol +: (1 until schema.numColumns).map(reader.getAny(_).toString)).mkString("\t")
      }.mkString("\n\t") + "\n"
  }
}

object SerializableRangeVector extends StrictLogging {
  import filodb.core._

  val queryResultBytes = Kamon.histogram("query-engine-result-bytes")

  /**
    * Creates a SerializableRangeVector out of another RangeVector by sharing a previously used RecordBuilder.
    * The most efficient option when you need to create multiple SRVs as the containers are automatically
    * shared correctly.
    * The containers are sent whole as most likely more than one would be sent, so they should mostly be packed.
    */
  def apply(rv: RangeVector,
            builder: RecordBuilder,
            schema: RecordSchema,
            execPlan: String): SerializableRangeVector = {
    var numRows = 0
    val oldContainerOpt = builder.currentContainer
    val startRecordNo = oldContainerOpt.map(_.numRecords).getOrElse(0)
    // Important TODO / TechDebt: We need to replace Iterators with cursors to better control
    // the chunk iteration, lock acquisition and release. This is much needed for safe memory access.
    try {
      ChunkMap.validateNoSharedLocks(execPlan)
      val rows = rv.rows
      while (rows.hasNext) {
        numRows += 1
        builder.addFromReader(rows.next, schema, 0)
      }
    } finally {
      // clear exec plan
      // When the query is done, clean up lingering shared locks caused by iterator limit.
      ChunkMap.releaseAllSharedLocks()
    }
    // If there weren't containers before, then take all of them.  If there were, discard earlier ones, just
    // start with the most recent one we started adding to
    val containers = oldContainerOpt match {
      case None                 => builder.allContainers
      case Some(firstContainer) => builder.allContainers.dropWhile(_ != firstContainer)
    }
    new SerializableRangeVector(rv.key, numRows, containers, schema, startRecordNo)
  }

  /**
    * Creates a SerializableRangeVector out of another RV and ColumnInfo schema.  Convenient but no sharing.
    * Since it wastes space when working with multiple RVs, should be used mostly for testing.
    */
  def apply(rv: RangeVector, cols: Seq[ColumnInfo]): SerializableRangeVector = {
    val schema = toSchema(cols)
    apply(rv, newBuilder(), schema, "Test-Only-Plan")
  }

  // TODO: make this configurable....
  val MaxContainerSize = 4096    // 4KB allows for roughly 200 time/value samples

  // Reuse RecordSchemas as there aren't too many schemas
  val SchemaCacheSize = 100
  val schemaCache = concurrentCache[Seq[ColumnInfo], RecordSchema](SchemaCacheSize)

  def toSchema(colSchema: Seq[ColumnInfo], brSchemas: Map[Int, RecordSchema] = Map.empty): RecordSchema =
    schemaCache.getOrElseUpdate(colSchema, { cols => new RecordSchema(cols, brSchema = brSchemas) })

  def newBuilder(): RecordBuilder =
    new RecordBuilder(MemFactory.onHeapFactory, MaxContainerSize)
}

final case class IteratorBackedRangeVector(key: RangeVectorKey,
                                           rows: Iterator[RowReader]) extends RangeVector