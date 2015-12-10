package filodb.core.query

import java.nio.ByteBuffer

import filodb.core.Types._
import filodb.core.metadata._
import filodb.core.query.Dataflow.RowReaderFactory
import org.velvia.filo.{FastFiloRowReader, FiloRowReader, FiloVector, RowReader}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object Dataflow {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader

  val DefaultReaderFactory: RowReaderFactory =
    (bytes, classes) => new FastFiloRowReader(bytes, classes)

}

trait Dataflow extends Iterator[RowReader] {

  def classes: Array[Class[_]]

  def getMoreRows(count: Int): Array[RowReader]

}


class SegmentScan(val segment: Segment,
                  columns: Seq[ColumnId])
                 (implicit val rowReaderFactory: RowReaderFactory)
  extends Dataflow {

  val chunkAccessTable: Array[(ChunkId, FiloRowReader)] = buildAccessTable()
  val overrideIndex: mutable.HashMap[Int, mutable.Set[Int]] with mutable.MultiMap[Int, Int] = buildOverrideIndex()

  private def chunks = segment.chunks

  def classes: Array[Class[_]] = columns.map { col =>
    segment.projection.schemaMap.get(col).get.columnType.clazz
  }.toArray

  private def numChunks = chunks.length


  private def buildAccessTable() = {
    val chunkAccessTable: Array[(ChunkId, FiloRowReader)] = new Array(numChunks)
    chunks.zipWithIndex.foreach { case (chunk, j) =>
      chunkAccessTable(j) = (chunk.chunkId,
        rowReaderFactory(chunk.columnVectors, classes))
    }
    chunkAccessTable
  }

  private def buildOverrideIndex() = {
    val overrideIndex = new mutable.HashMap[Int, mutable.Set[Int]]() with mutable.MultiMap[Int, Int]
    val allOverrides = chunks.map(_.chunkOverrides).collect { case Some(x) => x }.flatten.zipWithIndex
    allOverrides.foreach { case ((chunkId, keys), i) => keys.foreach(overrideIndex.addBinding(i, _)) }
    overrideIndex
  }

  var currentLocation = (0, -1)

  private def isInvalid(loc: (Int, Int)) = {
    loc._1 < 0 && loc._2 < 0
  }

  private def getNextLocation: Option[(Int, Int)] = {
    var newLocation = currentLocation
    do {
      newLocation = move(newLocation)
    } while (isSkip(newLocation))
    val totalChunks = numChunks
    val valid = !isInvalid(newLocation)
    val isValidChunk = newLocation._1 < totalChunks
    if (valid && isValidChunk && newLocation._2 < chunks(newLocation._1).numRows) {
      Some(newLocation)
    } else {
      None
    }
  }

  private def move(loc: (Int, Int)) = {
    var (chunkNum, rowOffset) = loc
    if (chunkNum < numChunks) {
      if (rowOffset + 1 < chunks(chunkNum).numRows) {
        rowOffset = rowOffset + 1
      } else {
        rowOffset = -1
      }
    }
    if (rowOffset == -1) {
      if (chunkNum + 1 < numChunks) {
        chunkNum = chunkNum + 1
        rowOffset = 0
      }
      else {
        chunkNum = -1
      }
    }
    (chunkNum, rowOffset)
  }

  private def isSkip(location: (Int, Int)): Boolean = {
    if (isInvalid(location)) {
      false
    } else {
      overrideIndex.get(location._1).exists { overrides =>
        overrides.contains(location._2)
      }
    }
  }

  override def hasNext: Boolean = {
    val l = getNextLocation
    l match {
      case Some(location) => true
      case None => false
    }
  }

  override def next(): RowReader = {
    val (chunkNum, rowOffset) = getNextLocation.get
    chunkAccessTable(chunkNum)._2.rowNo = rowOffset
    chunkAccessTable(chunkNum)._2
  }

  override def getMoreRows(count: Int): Array[RowReader] = {
    var fetched = 0
    val rows = ArrayBuffer[RowReader]()
    while (hasNext && fetched < count) {
      currentLocation = getNextLocation.get
      rows.+=:(SingleRowReader(currentLocation._2, chunkAccessTable(currentLocation._1)._2.parsers))
      fetched += 1
    }
    rows.toArray
  }

}

case class SingleRowReader(rowNum: Int, parsers: Array[FiloVector[_]]) extends FiloRowReader {
  rowNo = rowNum
}
