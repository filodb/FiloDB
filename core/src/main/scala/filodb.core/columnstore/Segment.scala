package filodb.core.columnstore

import java.nio.ByteBuffer
import org.velvia.filo.{IngestColumn, RowIngestSupport, RowToColumnBuilder}
import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.Column

/**
 * A Segment represents columnar chunks for a given dataset, partition, range of keys, and columns.
 * It also contains an index to help read data out in sorted order.
 * For more details see [[doc/sorted_chunk_merge.md]].
 */
trait Segment[K] {
  val keyRange: KeyRange[K]
  val index: ChunkRowMap

  def segmentId: ByteBuffer = keyRange.binaryStart
  def dataset: TableName    = keyRange.dataset
  def partition: PartitionKey = keyRange.partition

  override def toString: String = s"Segment($dataset : $partition / ${keyRange.start}) columns($getColumns)"

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit
  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)]
  def getColumns: collection.Set[ColumnId]

  def addChunks(id: ChunkID, chunks: Map[ColumnId, Chunk]): Unit = {
    for { (col, chunk) <- chunks } { addChunk(id, col, chunk) }
  }

  def isEmpty: Boolean = getColumns.isEmpty || index.isEmpty
}

/**
 * A generic segment implementation
 */
class GenericSegment[K](val keyRange: KeyRange[K],
                        val index: ChunkRowMap) extends Segment[K] {
  val chunkIds = ArrayBuffer[ChunkID]()
  val chunks = new HashMap[ColumnId, HashMap[ChunkID, Chunk]]

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit = {
    if (!(chunkIds contains id)) chunkIds += id
    val columnChunks = chunks.getOrElseUpdate(column, new HashMap[ChunkID, Chunk])
    columnChunks(id) = bytes
  }

  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)] =
    for { column <- chunks.keysIterator
          chunkId <- chunks(column).keysIterator }
    yield { (column, chunkId, chunks(column)(chunkId)) }

  def getColumns: collection.Set[ColumnId] = chunks.keySet
}

/**
 * A Segment class for easily adding bunches of rows as new columnar chunks, based upon
 * a fixed schema and RowIngestSupport typeclass.  Automatically increments the chunkId
 * properly.
 *
 * @param getSortKey a function to extract the sort key K out of the row type R.  Note that
 *                   the sort key must not be optional.
 */
class RowWriterSegment[K: SortKeyHelper, R](override val keyRange: KeyRange[K],
                                            schema: Seq[Column],
                                            ingestSupport: RowIngestSupport[R],
                                            getSortKey: R => K)
//scalastyle:off
//sorry for this ugly hack but only way to get scalac to pass in the SortKeyHelper
// properly it seems :(
extends GenericSegment(keyRange, null) {
  //scalastyle:on
  override val index = new UpdatableChunkRowMap[K]
  val filoSchema = schema.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      IngestColumn(name, colType.clazz)
  }

  val updatingIndex = index.asInstanceOf[UpdatableChunkRowMap[K]]

  /**
   * Adds a bunch of rows as a new set of chunks with the same chunkId.  The nextChunkId from
   * a ChunkRowMap will be used.
   */
  def addRowsAsChunk(rows: Seq[R]): Unit = {
    val newChunkId = index.nextChunkId
    rows.zipWithIndex.foreach { case (r, i) => updatingIndex.update(getSortKey(r), newChunkId, i) }
    val chunkMap = RowToColumnBuilder.buildFromRows(rows, filoSchema, ingestSupport)
    chunkMap.foreach { case (col, bytes) => addChunk(newChunkId, col, bytes) }
  }
}

/**
 * A segment optimized for reading and iterating rows of data out of the chunks.
 * You cannot modify the chunkRowMap.... addChunks is only intended for filling in chunk data
 * as it is read from the ColumnStore.  Then, you call rowIterator.
 * It assumes that the ChunkID is a counter with low cardinality, and stores everything
 * as arrays for extremely fast access.
 */
class RowReaderSegment[K](val keyRange: KeyRange[K],
                          val index: BinaryChunkRowMap,
                          columns: Seq[Column]) extends Segment[K] {
  import RowReaderSegment._

  // chunks(chunkId)(columnNum)
  val chunks = Array.fill(index.nextChunkId)(new Array[ByteBuffer](columns.length))
  val clazzes = columns.map(_.columnType.clazz).toArray

  val colIdToNumber = columns.zipWithIndex.map { case (col, idx) => (col.name, idx) }.toMap

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit =
    chunks(id)(colIdToNumber(column)) = bytes

  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)] = {
    for { chunkId <- (0 until index.nextChunkId).toIterator
          columnNo <- (0 until columns.length).toIterator }
    yield { (columns(columnNo).name, chunkId, chunks(chunkId)(columnNo)) }
  }

  def getColumns: collection.Set[ColumnId] = columns.map(_.name).toSet

  private def getReaders(readerFactory: RowReaderFactory): Array[RowReader] =
    (0 until index.nextChunkId).map { chunkId =>
      readerFactory(chunks(chunkId), clazzes)
    }.toArray

  /**
   * Iterates over rows in this segment in the sort order defined by the ChunkRowMap.
   * Creates new RowReaders every time, so that multiple calls could be made and original
   * state in the Segment is not mutated.
   */
  def rowIterator(readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
    val readers = getReaders(readerFactory)
    new Iterator[RowReader] {
      val chunkIdIter = index.chunkIdIterator
      val rowNumIter = index.rowNumIterator

      def hasNext: Boolean = chunkIdIter.hasNext
      def next: RowReader = {
        val nextChunk = chunkIdIter.next
        readers(nextChunk).rowNo = rowNumIter.next
        readers(nextChunk)
      }
    }
  }

  /**
   * Returns an Iterator over (reader, chunkId, rowNum).  Intended for efficient ChunkRowMap
   * merging operations.
   */
  def rowChunkIterator(readerFactory: RowReaderFactory = DefaultReaderFactory):
      Iterator[(RowReader, ChunkID, Int)] = {
    val readers = getReaders(readerFactory)
    new Iterator[(RowReader, ChunkID, Int)] {
      val chunkIdIter = index.chunkIdIterator
      val rowNumIter = index.rowNumIterator

      def hasNext: Boolean = chunkIdIter.hasNext
      def next: (RowReader, ChunkID, Int) = {
        val nextChunk = chunkIdIter.next
        val nextRowNo = rowNumIter.next
        readers(nextChunk).rowNo = nextRowNo
        (readers(nextChunk), nextChunk, nextRowNo)
      }
    }
  }
}

object RowReaderSegment {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => RowReader

  private val DefaultReaderFactory: RowReaderFactory =
    (bytes, clazzes) => new MutableRowReader(bytes, clazzes)

  def apply[K](genSeg: GenericSegment[K], schema: Seq[Column]): RowReaderSegment[K] = {
    val (chunkIdBuf, rowNumBuf) = genSeg.index.serialize()
    val binChunkMap = new BinaryChunkRowMap(chunkIdBuf, rowNumBuf, genSeg.index.nextChunkId)
    val readSeg = new RowReaderSegment(genSeg.keyRange, binChunkMap, schema)
    genSeg.getChunks.foreach { case (col, id, bytes) => readSeg.addChunk(id, col, bytes) }
    readSeg
  }
}

// Move these to filo project
import org.velvia.filo.ColumnWrapper

/**
 * A RowReader is designed for fast iteration over rows of multiple Filo vectors, ideally all
 * with the same length.  An Iterator[RowReader] sets the rowNo and returns this RowReader, and
 * the application is responsible for calling the right method to extract each value.
 * The advantage of RowReader over RowExtractor is that one does not need to set values in some
 * intermediate Row object; the get* methods read directly from Filo vectors.
 * For example, a Spark Row can inherit from RowReader.
 */
abstract class RowReader {
  def parsers: Array[ColumnWrapper[_]]
  var rowNo: Int = -1

  final def notNull(columnNo: Int): Boolean = parsers(columnNo).isAvailable(rowNo)
  final def getInt(columnNo: Int): Int = parsers(columnNo).asInstanceOf[ColumnWrapper[Int]](rowNo)
  final def getLong(columnNo: Int): Long = parsers(columnNo).asInstanceOf[ColumnWrapper[Long]](rowNo)
  final def getDouble(columnNo: Int): Double = parsers(columnNo).asInstanceOf[ColumnWrapper[Double]](rowNo)
  final def getString(columnNo: Int): String = parsers(columnNo).asInstanceOf[ColumnWrapper[String]](rowNo)
}

object RowReader {
  trait TypedFieldExtractor[F] {
    def getField(reader: RowReader, columnNo: Int): F
  }

  implicit object LongFieldExtractor extends TypedFieldExtractor[Long] {
    final def getField(reader: RowReader, columnNo: Int): Long = reader.getLong(columnNo)
  }
}

private object Classes {
  val Byte = java.lang.Byte.TYPE
  val Short = java.lang.Short.TYPE
  val Int = java.lang.Integer.TYPE
  val Long = java.lang.Long.TYPE
  val Float = java.lang.Float.TYPE
  val Double = java.lang.Double.TYPE
  val String = classOf[String]
}

class MutableRowReader(chunks: Array[ByteBuffer], classes: Array[Class[_]]) extends RowReader {
  import org.velvia.filo.ColumnParser._

  val parsers: Array[ColumnWrapper[_]] = chunks.zip(classes).map {
    case (chunk, Classes.String) => parse[String](chunk)
    case (chunk, Classes.Int) => parse[Int](chunk)
    case (chunk, Classes.Long) => parse[Long](chunk)
    case (chunk, Classes.Double) => parse[Double](chunk)
  }
}

// TODO: add a Segment class that helps take in a bunch of rows and serializes them into chunks,
// also correctly updating the rowIndex and chunkID.