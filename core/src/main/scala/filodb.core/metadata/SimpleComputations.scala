package filodb.core.metadata

import com.typesafe.config.ConfigFactory
import java.sql.Timestamp
import org.scalactic._
import org.velvia.filo.{RowReader, ZeroCopyUTF8String => UTF8Str}

import filodb.core._
import filodb.core.Types._


object SimpleComputations {
  import SingleKeyTypes._
  import Column.ColumnType._
  import RowReader.TypedFieldExtractor

  /**
   * Syntax: :string <constStringValue>
   * Produces a StringColumn with a single constant value
   */
  object ConstStringComputation extends ColumnComputation {
    def funcName: String = "string"

    def analyze(expr: String,
                dataset: String,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { args <- fixedNumArgs(expr, 1) }
      yield {
        ComputedColumn(0, expr, dataset, StringColumn, Nil,
                       new TypedFieldExtractor[UTF8Str] {
                         def getField(reader: RowReader, columnNo: Int): UTF8Str = UTF8Str(args.head)
                         def compare(reader: RowReader, other: RowReader, columnNo: Int): Int = ???
                       })
      }
    }
  }

  /**
   * Syntax: :getOrElse <colName> <defaultValue>
   * returns <defaultValue> if <colName> is null
   */
  object GetOrElseComputation extends SingleColumnComputation {
    def funcName: String = "getOrElse"

    def analyze(expr: String,
                dataset: String,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { info <- parse(expr, schema)
            defaultValue <- parseParam(info.keyType, info.param) }
      yield {
        val origExtractor = info.keyType.extractor
        val extractor = new TypedFieldExtractor[Any] {
          def getField(reader: RowReader, columnNo: Int): Any = {
            if (reader.notNull(columnNo)) { origExtractor.getField(reader, columnNo) }
            else { defaultValue }
          }
          def compare(reader: RowReader, other: RowReader, columnNo: Int): Int = ???
        }
        computedColumn(expr, dataset, info, extractor)
      }
    }
  }

  /**
   * Syntax: :round <colName> <roundingValue>
   * Valid for: IntColumn, LongColumn, DoubleColumn
   * Rounds the numeric value to the nearest multiple of <roundingValue>.
   * Examples:
   *
   *   :round intCol 10000 where  intCol=12345 -> 10000,  19999 -> 10000
   *   :rount doubleCol 1000.0 where doubleCol=1999.9->1000
   *
   * NOTE: the rounding value is used as the default value if the source column is null.
   */
  object RoundComputation extends SingleColumnComputation {
    def funcName: String = "round"

    def analyze(expr: String,
                dataset: String,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { info <- parse(expr, schema, Set(IntColumn, LongColumn, DoubleColumn))
            roundingValue <- parseParam(info.keyType, info.param) }
      yield {
        val extractor = info.colType match {
          case IntColumn =>
            val round = roundingValue.asInstanceOf[Int]
            wrap((i: Int) => i / round * round)
          case LongColumn =>
            val round = roundingValue.asInstanceOf[Long]
            wrap((i: Long) => i / round * round)
          case DoubleColumn =>
            val round = roundingValue.asInstanceOf[Double]
            wrap((i: Double) => Math.floor(i / round) * round)
          case o: Column.ColumnType => ???
        }
        computedColumn(expr, dataset, info, extractor)
      }
    }
  }

  /**
   * :stringPrefix stringCol numChars
   * Defaults to "" if null string column
   */
  object StringPrefixComputation extends SingleColumnComputation {
    def funcName: String = "stringPrefix"

    def analyze(expr: String,
                dataset: String,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { info <- parse(expr, schema, Set(StringColumn))
            numChars <- parseParam(SingleKeyTypes.IntKeyType, info.param) }
      yield {
        computedColumn(expr, dataset, info, wrap((s: UTF8Str) => s.substring(0, numChars)))
      }
    }
  }

  /**
   * :hash columnName numBuckets - for an int, long, or string column
   * returns an int between 0 and (numBuckets - 1)
   */
  object HashComputation extends SingleColumnComputation {
    def funcName: String = "hash"

    def analyze(expr: String,
                dataset: String,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { info <- parse(expr, schema, Set(IntColumn, LongColumn, StringColumn))
            numBuckets <- parseParam(SingleKeyTypes.IntKeyType, info.param) }
      yield {
        val extractor = info.colType match {
          case IntColumn  => wrap((i: Int) => Math.abs(i % numBuckets))
          case LongColumn => wrap((l: Long) => Math.abs(l % numBuckets).toInt)
          case DoubleColumn => wrap((d: Double) => Math.abs(d % numBuckets).toInt)
          case StringColumn => wrap((s: UTF8Str) => Math.abs(s.hashCode % numBuckets))
          case BitmapColumn => wrap((b: Boolean) => (if (b) 1 else 0) % numBuckets)
          case TimestampColumn => wrap((l: Long) => Math.abs(l % numBuckets).toInt)
        }
        computedColumn(expr, dataset, info, IntColumn, extractor)
      }
    }
  }
}