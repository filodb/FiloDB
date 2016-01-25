package filodb.core.metadata

import org.velvia.filo.RowReader
import scala.util.{Failure, Success, Try}

import filodb.core.{KeyType, SingleKeyTypeBase}
import filodb.core.Types._

/**
 * Represents a computed or generated column.
 * @param keyType a custom KeyType which returns the computation via the getKeyFunc function.
 */
case class ComputedColumn(id: Int,
                          expr: String,   // The original computation expression
                          dataset: String,
                          columnType: Column.ColumnType,
                          keyType: KeyType) extends Column {
  def name: String = expr
}

object ComputedColumn {
  import SimpleComputations._

  val AllComputations = Seq(ConstStringComputation, GetOrElseComputation)
  val nameToComputation = AllComputations.map { comp => comp.funcName -> comp }.toMap

  def isComputedColumn(expr: String): Boolean = expr.startsWith(":")

  /**
   * Analyzes a computed column expression, matching it with the correct ColumnComputation.
   * @returns NoSuchFunction if a ColumnComputation is not found; if found,
   *          then the return value from the analyze() method of the computation
   */
  def analyze(expr: String, dataset: TableName, schema: Seq[Column]): Try[ComputedColumn] = {
    if (!isComputedColumn(expr)) return Failure(NotComputedColumn)

    val exprFunction = expr.split(" ").head.drop(1)
    nameToComputation.get(exprFunction).map { computation =>
      computation.analyze(expr, dataset, schema)
    }.getOrElse(Failure(NoSuchFunction(exprFunction)))
  }
}

trait InvalidComputedColumnSpec
case class BadArgument(reason: String) extends Exception("BadArgument" + reason)
  with InvalidComputedColumnSpec
case class WrongNumberArguments(given: Int, expected: Int) extends Exception(
  s"Wrong # of args: expected $expected, but given $given") with InvalidComputedColumnSpec
case class NoSuchFunction(func: String) extends Exception(s"No such function $func")
  with InvalidComputedColumnSpec
case object NotComputedColumn extends Exception("Not a computed column") with InvalidComputedColumnSpec

/**
 * A ColumnComputation analyzes the user computed column input (eg ":getOrElse someCol 100")
 * and attempts to return a ComputedColumn with keyType with the computation function.
 */
trait ColumnComputation {
  // The name of the computation function, without the leading ":"
  def funcName: String

  /**
   * Attempt to analyze the user arguments and produce a ComputedColumn.
   * @returns either a ComputedColumn with valid ComputedKeyType, or one of the InvalidComputedColumnSpec
   *          exceptions.  NOTE: does not need to fill in id, which will be generated/replaced later.
   */
  def analyze(expr: String, dataset: TableName, schema: Seq[Column]): Try[ComputedColumn]

  def userArgs(expr: String): Seq[String] = expr.split(" ").toSeq.drop(1)
}

object SimpleComputations {
  import ComputedKeyTypes._
  import Column.ColumnType._

  object ConstStringComputation extends ColumnComputation {
    def funcName: String = "string"

    def analyze(expr: String, dataset: TableName, schema: Seq[Column]): Try[ComputedColumn] = {
      val args = userArgs(expr)
      if (args.length == 1) {
        Success(ComputedColumn(0, expr, dataset, StringColumn,
                               new ComputedStringKeyType((x: RowReader) => args.head)))
      } else {
        Failure(WrongNumberArguments(args.length, 1))
      }
    }
  }

  /**
   * Syntax: :getOrElse <colName> <defaultValue>
   * returns <defaultValue> if <colName> is null
   */
  object GetOrElseComputation extends ColumnComputation {
    def funcName: String = "getOrElse"
    def analyze(expr: String, dataset: TableName, schema: Seq[Column]): Try[ComputedColumn] = {
      val args = userArgs(expr)
      if (args.length != 2) return Failure(WrongNumberArguments(args.length, 2))

      // Look for column name argument
      val sourceColIndex = schema.indexWhere(_.name == args(0))
      if (sourceColIndex < 0) return Failure(BadArgument(s"Could not find source column ${args(0)}"))
      val sourceColType = schema(sourceColIndex).columnType

      val keyType = sourceColType.keyType
      val extractor = keyType.asInstanceOf[SingleKeyTypeBase[keyType.T]].extractor
      val defaultValue = Try(keyType.fromString(args(1))).recover { case t: Throwable =>
        return Failure(BadArgument(s"Could not parse [${args(1)}]: ${t.getMessage}"))
      }.get

      val computedKeyType = getComputedType(keyType)((r: RowReader) =>
                                if (r.notNull(sourceColIndex)) { extractor.getField(r, sourceColIndex) }
                                else { defaultValue }
                              )
      Success(ComputedColumn(0, expr, dataset, sourceColType, computedKeyType))
    }
  }
}