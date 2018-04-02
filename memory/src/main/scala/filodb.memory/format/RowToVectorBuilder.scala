package filodb.memory.format

import java.nio.ByteBuffer

import scala.language.existentials

import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format.Encodings.{AutoDetect, EncodingHint}
import filodb.memory.format.vectors._


case class VectorInfo(name: String, dataType: Class[_])

// To help matching against the ClassTag in the VectorBuilder
object Classes {
  val Boolean = classOf[Boolean]
  val Byte = java.lang.Byte.TYPE
  val Short = java.lang.Short.TYPE
  val Int = java.lang.Integer.TYPE
  val Long = java.lang.Long.TYPE
  val Float = java.lang.Float.TYPE
  val Double = java.lang.Double.TYPE
  val String = classOf[String]
  val UTF8 = classOf[ZeroCopyUTF8String]
}
object RowToVectorBuilder {
  /**
    * A convenience method to turn a bunch of rows R to Filo serialized columnar chunks.
    * @param rows the rows to convert to columnar chunks
    * @param schema a Seq of VectorInfo describing the Vector used for each column
    * @param hint an EncodingHint for the encoder
    * @return a Map of column name to the byte chunks
    */
  def buildFromRows(rows: Iterator[RowReader],
                    schema: Seq[VectorInfo],
                    hint: EncodingHint = AutoDetect): Map[String, ByteBuffer] = {
    val builder = new RowToVectorBuilder(schema)
    rows.foreach(builder.addRow)
    builder.convertToBytes(hint)
  }
}

/**
  * Class to help transpose a set of rows to Filo binary vectors.
  * @param schema a Seq of VectorInfo describing the data type used for each vector
  * @param builderMap pass in a custom BuilderMap to extend the supported vector types
  *
  * TODO: Add stats about # of rows, chunks/buffers encoded, bytes encoded, # NA's etc.
  */
class RowToVectorBuilder(schema: Seq[VectorInfo], memFactory: MemFactory = MemFactory.onHeapFactory) {
  val maxElements = 1000
  val builders = schema.zipWithIndex.map {
    case (VectorInfo(_, dataType),index)=> dataType match {
      case Classes.Int    => IntBinaryVector.appendingVector(memFactory, maxElements)
      case Classes.Long   => LongBinaryVector.appendingVector(memFactory, maxElements)
      case Classes.Double => DoubleVector.appendingVector(memFactory, maxElements)
      case Classes.UTF8   => UTF8Vector.appendingVector(memFactory, maxElements)
      case Classes.String => UTF8Vector.appendingVector(memFactory, maxElements)
    }
  }
  val numColumns = schema.length


  /**
    * Adds a single row of data to each of the VectorBuilders.
    * @param row the row of data to transpose.  Each column will be added to the right Builders.
    */
  def addRow(row: RowReader): Unit = {
    for { i <- 0 until numColumns optimized } {
      builders(i).addFromReader(row, i)
    }
  }


  def convertToBytes(hint: EncodingHint = AutoDetect): Map[String, ByteBuffer] = {
    val chunks = builders.map(_.optimize(memFactory, hint).toFiloBuffer)
    schema.zip(chunks).map { case (VectorInfo(colName, _), bytes) => (colName, bytes) }.toMap
  }

  private def unsupportedInput(typ: Any) =
    throw new RuntimeException("Unsupported input type " + typ)
}