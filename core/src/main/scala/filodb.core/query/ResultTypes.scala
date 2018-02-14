package filodb.core.query

import scala.reflect.runtime.universe._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.joda.time.DateTime

import filodb.core.CompositeReaderOrdering
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Column
import filodb.core.store.{ChunkScanMethod, RowKeyChunkScan}
import filodb.memory.format.RowReader

/**
 * Some basic info about a single Partition
 */
final case class PartitionInfo(partKey: BinaryRecord, shardNo: Int) {
  override def toString: String = s"/shard:$shardNo/$partKey"
}

/**
 * Multiple data elements from one or more columns.
 * Could be raw data or intermediate values which fundamentally, given a schema, must be represented
 * by an array of multiple elements, each one with a distinct key.
 * Think of it as a logical array of Tuples, physically represented by multiple BinaryVectors, one for each column.
 */
final case class PartitionVector(info: Option[PartitionInfo], readers: Seq[ChunkSetReader]) {
  /**
   * Returns an Iterator of RowReader over all the chunksets from the key range (startKey, endKey).
   * Assumes chunks are sorted in row key order, and first few chunks in every reader are the row key vectors.
   * @param startKey the start of the range desired, inclusive
   * @param endKey the end of the range desired, inclusive
   * @param ordering an Ordering that compares RowReaders made from the vectors, consistent with row key vectors
   *                 could be dataset.rowKeyOrdering for example, but must correspond to schema of readers
   */
  def rangedIterator(startKey: BinaryRecord,
                     endKey: BinaryRecord,
                     ordering: Ordering[RowReader]): Iterator[RowReader] = {
    readers.toIterator.flatMap { reader =>
      val (startRow, endRow) = reader.rowKeyRange(startKey, endKey, ordering)
      if (endRow < 0 || startRow >= reader.length) { Iterator.empty }
      else if (startRow == 0 && endRow == (reader.length - 1)) {
        reader.rowIterator()
      } else {
        reader.rowIterator().take(endRow + 1).drop(startRow)
      }
    }
  }

  /**
   * Returns an Iterator of RowReader over all the rows in each chunkset in order
   */
  def allRowsIterator: Iterator[RowReader] = readers.toIterator.flatMap(_.rowIterator())
}

object PartitionVector {
  def apply(reader: ChunkSetReader): PartitionVector = PartitionVector(None, Seq(reader))
}

/**
 * A single element of data described by a fixed schema of types, corresponding to a single key or timestamp.
 * Could be either raw or intermediate data.
 * Ex: a (timestamp, value) tuple.  Or a (Double) scalar.  (count, total) for an average.
 */
final case class Tuple(info: Option[PartitionInfo], data: BinaryRecord)

/**
 * Describes column/field name and type
 */
final case class ColumnInfo(name: String, colType: Column.ColumnType)

/**
 * Describes the full schema of Vectors and Tuples, including how many initial columns are for row keys.
 * The first ColumnInfo in the schema describes the first vector in Vectors and first field in Tuples, etc.
 */
final case class ResultSchema(columns: Seq[ColumnInfo], numRowKeyColumns: Int) {
  import Column.ColumnType._

  def length: Int = columns.length
  def isTimeSeries: Boolean = columns.length >= 1 && numRowKeyColumns == 1 &&
                              (columns.head.colType == LongColumn || columns.head.colType == TimestampColumn)
}

/**
 * There are three types of final query results.
 * - a list of raw (or via function, transformed) time series samples, with an optional key range
 * - a list of aggregates
 * - a final aggregate
 */
// NOTE: the Serializable is needed for Akka to choose a more specific serializer (eg Kryo)
sealed trait Result extends java.io.Serializable {
  def schema: ResultSchema

  /**
   * Returns an Iterator of (Option[PartitionInfo], Seq[RowReader]) which helps with serialization. Basically each
   * element of the returned Seq contains partition info (optional), plus a Seq of RowReaders.  Each RowReader
   * can then be converted to pretty print text, JSON, etc. etc.
   */
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])]

  /**
   * Pretty prints all the elements into strings.  Returns an iterator to avoid memory bloat.
   */
  def prettyPrint(formatTime: Boolean = true, partitionRowLimit: Int = 50): Iterator[String] = {
    val curTime = System.currentTimeMillis
    toRowReaders.map { case (partInfoOpt, rowReaders) =>
      partInfoOpt.map(_.toString).getOrElse("") + "\n\t" +
        rowReaders.take(partitionRowLimit).map {
          case br: BinaryRecord if br.isEmpty =>  "\t<empty>"
          case reader =>
            val firstCol = if (formatTime && schema.isTimeSeries) {
              val timeStamp = reader.getLong(0)
              s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago)"
            } else {
              reader.getAny(0).toString
            }
            (firstCol +: (1 until schema.length).map(reader.getAny(_).toString)).mkString("\t")
        }.mkString("\n\t") + "\n"
    }
  }
}

final case class VectorListResult(keyRange: Option[(BinaryRecord, BinaryRecord)],
                                  schema: ResultSchema,
                                  vectorList: Seq[PartitionVector]) extends Result {
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])] = {
    val keyTypes = schema.columns.take(schema.numRowKeyColumns).map(_.colType.keyType)
    val ordering = CompositeReaderOrdering(keyTypes)
    vectorList.toIterator.map { pv =>
      val rows = keyRange match {
        case Some((startKey, endKey)) => pv.rangedIterator(startKey, endKey, ordering).toSeq
        case None                     => pv.allRowsIterator.toSeq
      }
      (pv.info, rows)
    }
  }
}

final case class VectorResult(schema: ResultSchema, vector: PartitionVector) extends Result {
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])] =
    Iterator.single((vector.info, vector.allRowsIterator.toBuffer))
}

final case class TupleListResult(schema: ResultSchema, tuples: Seq[Tuple]) extends Result {
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])] =
    tuples.toIterator.map { case Tuple(info, data) => (info, Seq(data)) }
}

final case class TupleResult(schema: ResultSchema, tuple: Tuple) extends Result {
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])] =
    Iterator.single((tuple.info, Seq(tuple.data)))
}

/**
 * Converts various types to result types
 * TODO: consider collapsing into Result
 */
abstract class ResultMaker[A: TypeTag] {
  /**
   * Converts a source type like a Vector or Tuple to a result, with the given schema.
   * @param schema the schema of the result
   * @param chunkMethod used only for the VectorListResult to filter rows from the vectors
   * @param limit for Observables, limits the number of items to take
   */
  def toResult(input: A, schema: ResultSchema, chunkMethod: ChunkScanMethod, limit: Int): Task[Result]

  def fromResult(res: Result): A

  /**
   * Full type info including erased inner class info.  Needed to discern inner type of Observables.
   * Converted to a string and shortened to leave out the package namespaces
   */
  def typeInfo: String = {
    val typ = typeOf[A]
    s"${typ.typeSymbol.name}[${typ.typeArgs.map(_.typeSymbol.name).mkString(",")}]"
  }
}

object ResultMaker extends StrictLogging {
  implicit object UnitMaker extends ResultMaker[Unit] {
    // Unit should NEVER be the output of an ExecPlan.  Create an empty result if we ever desire that.
    def toResult(u: Unit,
                 schema: ResultSchema,
                 chunkMethod: ChunkScanMethod,
                 limit: Int = 1000): Task[Result] = ???
    def fromResult(res: Result): Unit = {}
  }

  implicit object PartitionVectorObservableMaker extends ResultMaker[Observable[PartitionVector]] {
    def toResult(partVectors: Observable[PartitionVector],
                 schema: ResultSchema,
                 chunkMethod: ChunkScanMethod,
                 limit: Int = 1000): Task[Result] = {
      partVectors.take(limit).toListL.map { parts =>
        // calculate the proper range
        val keyRange = chunkMethod match {
          case r: RowKeyChunkScan => Some((r.startkey, r.endkey))
          case _                  => None
        }
        VectorListResult(keyRange, schema, parts)
      }
    }

    def fromResult(res: Result): Observable[PartitionVector] = res match {
      case VectorListResult(_, _, vectors) => Observable.fromIterable(vectors)
      case other: Result => throw new RuntimeException(s"Unexpected result $other... possible type/plan error")
    }
  }

  implicit object TupleObservableMaker extends ResultMaker[Observable[Tuple]] {
    def toResult(tuples: Observable[Tuple],
                 schema: ResultSchema,
                 chunkMethod: ChunkScanMethod,
                 limit: Int = 1000): Task[Result] = {
      tuples.take(limit).toListL.map { tupleList =>
        TupleListResult(schema, tupleList)
      }
    }

    def fromResult(res: Result): Observable[Tuple] = res match {
      case TupleListResult(_, tuples) => Observable.fromIterable(tuples)
      case other: Result => throw new RuntimeException(s"Unexpected result $other... possible type/plan error")
    }
  }
}
