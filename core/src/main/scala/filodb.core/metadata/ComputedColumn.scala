package filodb.core.metadata

import org.scalactic._
import org.velvia.filo.RowReader

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
  def analyze(expr: String,
              dataset: TableName,
              schema: Seq[Column]): ComputedColumn Or One[InvalidComputedColumnSpec] = {
    if (isComputedColumn(expr)) {
      val exprFunction = expr.split(' ').head.drop(1)
      nameToComputation.get(exprFunction).map { computation =>
        computation.analyze(expr, dataset, schema).accumulating
      }.getOrElse(Bad(One(NoSuchFunction(exprFunction))))
    } else {
      Bad(One(NotComputedColumn))
    }
  }
}

trait InvalidComputedColumnSpec
case class BadArgument(reason: String) extends InvalidComputedColumnSpec
case class WrongNumberArguments(given: Int, expected: Int) extends InvalidComputedColumnSpec
case class NoSuchFunction(func: String) extends InvalidComputedColumnSpec
case object NotComputedColumn extends InvalidComputedColumnSpec

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
   *          values.  NOTE: does not need to fill in id, which will be generated/replaced later.
   */
  def analyze(expr: String,
              dataset: TableName,
              schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec

  def userArgs(expr: String): Seq[String] = expr.split(" ").toSeq.drop(1)

  /**
   * Breaks up expr into space-separated arguments, validating a fixed number of arguments
   * @param expr string expression
   * @param numArgs expected number of arguments
   * @return Good(Seq[String]) if number of args matches numArgs, or Bad(WrongNumberArguments)
   */
  def getFixedNumArgs(expr: String, numArgs: Int): Seq[String] Or InvalidComputedColumnSpec = {
    val args = userArgs(expr)
    if (args.length == numArgs) Good(args) else Bad(WrongNumberArguments(args.length, numArgs))
  }
}

object SimpleComputations {
  import ComputedKeyTypes._
  import Column.ColumnType._
  import TypeCheckedTripleEquals._

  object ConstStringComputation extends ColumnComputation {
    def funcName: String = "string"

    def analyze(expr: String,
                dataset: TableName,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {
      for { args <- getFixedNumArgs(expr, 1) }
      yield {
        ComputedColumn(0, expr, dataset, StringColumn,
                       new ComputedStringKeyType((x: RowReader) => args.head))
      }
    }
  }

  /**
   * Syntax: :getOrElse <colName> <defaultValue>
   * returns <defaultValue> if <colName> is null
   */
  object GetOrElseComputation extends ColumnComputation {

    def funcName: String = "getOrElse"

    private def getColIndex(schema: Seq[Column], colName: String): Int Or InvalidComputedColumnSpec = {
      val sourceColIndex = schema.indexWhere(_.name === colName)
      if (sourceColIndex < 0) { Bad(BadArgument(s"Could not find source column $colName")) }
      else                    { Good(sourceColIndex) }
    }

    def analyze(expr: String,
                dataset: TableName,
                schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec = {

      def getDefaultValue(keyType: KeyType, arg: String): keyType.T Or InvalidComputedColumnSpec = {
        try { Good(keyType.fromString(arg)) }
        catch {
          case t: Throwable => Bad(BadArgument(s"Could not parse [$arg]: ${t.getMessage}"))
        }
      }

      for { args <- getFixedNumArgs(expr, 2)
            sourceColIndex <- getColIndex(schema, args(0))
            sourceColType = schema(sourceColIndex).columnType
            keyType = sourceColType.keyType
            extractor = keyType.asInstanceOf[SingleKeyTypeBase[keyType.T]].extractor
            defaultValue <- getDefaultValue(keyType, args(1)) }
      yield {
        val computedKeyType = getComputedType(keyType)((r: RowReader) =>
                                  if (r.notNull(sourceColIndex)) {
                                    extractor.getField(r, sourceColIndex).asInstanceOf[keyType.T]
                                  } else { defaultValue }
                                )
        ComputedColumn(0, expr, dataset, sourceColType, computedKeyType)
      }
    }
  }
}