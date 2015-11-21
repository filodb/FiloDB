package filodb.core.query

import java.nio.ByteBuffer

import filodb.core.Types._
import filodb.core.metadata._
import org.velvia.filo.{FastFiloRowReader, FiloRowReader, FiloVector}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class SingleRowReader(rowNum: Int, parsers: Array[FiloVector[_]]) extends FiloRowReader {
  rowNo = rowNum
}

trait Dataflow {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader
  val readerFactory: RowReaderFactory = (bytes, classes) => new FastFiloRowReader(bytes, classes)

  def hasMoreRows: Boolean

  def getMoreRows(batchSize: Int): Array[FiloRowReader]
}


class UnorderedSegmentScan(segment: Segment) extends Dataflow {

  val chunkAccessTable: Array[(ChunkId, Array[FiloVector[_]])] = buildAccessTable()
  val overrideIndex: mutable.HashMap[Int, mutable.Set[Int]] with mutable.MultiMap[Int, Int] = buildOverrideIndex()

  private def chunks = segment.chunks

  private def classes = segment.columns.map(_.columnType.clazz).toArray

  private def numChunks = chunks.length


  private def buildAccessTable() = {
    val chunkAccessTable: Array[(ChunkId, Array[FiloVector[_]])] = new Array(numChunks)
    chunks.zipWithIndex.foreach { case (chunk, j) =>
      chunkAccessTable(j) = (chunk.chunkId, readerFactory(chunk.columnVectors, classes).parsers)
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

  private def getNextLocation: Option[(Int, Int)] = {
    var newLocation = move(currentLocation)
    while (isSkip(newLocation)) {
      newLocation = move(newLocation)
    }
    if (newLocation._1 < numChunks && newLocation._2 < chunks(newLocation._1).numRows) Some(newLocation) else None
  }

  private def move(loc: (Int, Int)) = {
    var (chunkNum, rowOffset) = loc
    if (rowOffset < chunks(chunkNum).numRows) {
      rowOffset = rowOffset + 1
    } else {
      chunkNum = chunkNum + 1
      rowOffset = 0
    }
    (chunkNum, rowOffset)
  }

  private def isSkip(location: (Int, Int)): Boolean = {
    overrideIndex.get(location._1).exists { overrides =>
      overrides.contains(location._2)
    }
  }

  override def hasMoreRows: Boolean = {
    val l = getNextLocation
    l match {
      case Some(location) => true
      case None => false
    }
  }

  override def getMoreRows(batchSize: Int): Array[FiloRowReader] = {
    val rows = ArrayBuffer[FiloRowReader]()
    while (hasMoreRows) {
      currentLocation = getNextLocation.get
      rows.+=:(SingleRowReader(currentLocation._2, chunkAccessTable(currentLocation._1)._2))
    }
    rows.toArray
  }
}
