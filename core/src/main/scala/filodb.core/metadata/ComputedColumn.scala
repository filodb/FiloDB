package filodb.core.metadata

import com.typesafe.config.Config
import org.scalactic._

import filodb.core.KeyType
import filodb.memory.format.RowReader._

/**
 * Represents a computed or generated column.
 */
case class ComputedColumn(id: Int,
                          expr: String,   // The original computation expression
                          dataset: String,
                          columnType: Column.ColumnType,
                          sourceIndices: Seq[Int],    // index into schema of source column
                          val extractor: TypedFieldExtractor[_]) extends Column {
  def name: String = expr
  def params: Config = ???
}

object ComputedColumn {
  import SimpleComputations._
  import TimeComputations._

  val AllComputations = Seq(ConstStringComputation,
                            GetOrElseComputation,
                            RoundComputation,
                            TimesliceComputation,
                            MonthOfYearComputation,
                            StringPrefixComputation,
                            HashComputation)
  val nameToComputation = AllComputations.map { comp => comp.funcName -> comp }.toMap

  def isComputedColumn(expr: String): Boolean = expr.startsWith(":")

  /**
   * Analyzes a computed column expression, matching it with the correct ColumnComputation.
   * @return NoSuchFunction if a ColumnComputation is not found; if found,
   *         then the return value from the analyze() method of the computation
   */
  def analyze(expr: String,
              dataset: String,
              schema: Seq[Column]): ComputedColumn Or One[InvalidFunctionSpec] = {
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

trait InvalidFunctionSpec
case class BadArgument(reason: String) extends InvalidFunctionSpec
case class WrongNumberArguments(given: Int, expected: Int) extends InvalidFunctionSpec
case class NoSuchFunction(func: String) extends InvalidFunctionSpec
case object NotComputedColumn extends InvalidFunctionSpec

/**
 * A ColumnComputation analyzes the user computed column input (eg ":getOrElse someCol 100")
 * and attempts to return a ComputedColumn with keyType with the computation function.
 */
trait ColumnComputation extends FunctionValidationHelpers {
  // The name of the computation function, without the leading ":"
  def funcName: String

  /**
   * Attempt to analyze the user arguments and produce a ComputedColumn.
   * @return either a ComputedColumn with valid ComputedKeyType, or one of the InvalidFunctionSpec
   *         values.  NOTE: does not need to fill in id, which will be generated/replaced later.
   */
  def analyze(expr: String,
              dataset: String,
              schema: Seq[Column]): ComputedColumn Or InvalidFunctionSpec

  def userArgs(expr: String): Seq[String] = expr.split(" ").toSeq.drop(1)

  /**
   * Breaks up expr into space-separated arguments, validating a fixed number of arguments
   * @param expr string expression
   * @param numArgs expected number of arguments
   * @return Good(Seq[String]) if number of args matches numArgs, or Bad(WrongNumberArguments)
   */
  def fixedNumArgs(expr: String, numArgs: Int): Seq[String] Or InvalidFunctionSpec =
    validateNumArgs(userArgs(expr), numArgs)
}

trait FunctionValidationHelpers {
  import TypeCheckedTripleEquals._

  def validateNumArgs(args: Seq[String], numArgs: Int): Seq[String] Or InvalidFunctionSpec =
    if (args.length == numArgs) Good(args) else Bad(WrongNumberArguments(args.length, numArgs))

  def columnIndex(schema: Seq[Column], colName: String): Int Or InvalidFunctionSpec = {
    val sourceColIndex = schema.indexWhere(_.name === colName)
    if (sourceColIndex < 0) { Bad(BadArgument(s"Could not find source column $colName")) }
    else                    { Good(sourceColIndex) }
  }

  def validatedColumnType(schema: Seq[Column], colIndex: Int, allowedTypes: Set[Column.ColumnType]):
      Column.ColumnType Or InvalidFunctionSpec = {
    val colType = schema(colIndex).columnType
    if (allowedTypes.contains(colType)) { Good(colType) }
    else { Bad(BadArgument(s"$colType not in allowed set $allowedTypes")) }
  }

  def parseParam(keyType: KeyType, arg: String): keyType.T Or InvalidFunctionSpec = {
    try { Good(keyType.fromString(arg)) }
    catch {
      case t: Throwable => Bad(BadArgument(s"Could not parse [$arg]: ${t.getMessage}"))
    }
  }
}

/**
 * A case class and trait to facilitate single column computations
 */
case class SingleColumnInfo(sourceColumn: String,
                            param: String,
                            colIndex: Int,
                            colType: Column.ColumnType) {
  val keyType = colType.keyType
}

trait SingleColumnComputation extends ColumnComputation {
  def parse(expr: String, schema: Seq[Column]): SingleColumnInfo Or InvalidFunctionSpec = {
    for { args <- fixedNumArgs(expr, 2)
          sourceColIndex <- columnIndex(schema, args(0))
          sourceColType = schema(sourceColIndex).columnType }
    yield { SingleColumnInfo(args(0), args(1), sourceColIndex, sourceColType) }
  }

  def parse(expr: String, schema: Seq[Column], allowedTypes: Set[Column.ColumnType]):
      SingleColumnInfo Or InvalidFunctionSpec = {
    for { args <- fixedNumArgs(expr, 2)
          sourceColIndex <- columnIndex(schema, args(0))
          sourceColType <- validatedColumnType(schema, sourceColIndex, allowedTypes) }
    yield { SingleColumnInfo(args(0), args(1), sourceColIndex, sourceColType) }
  }

  def computedColumn(expr: String,
                     dataset: String,
                     sourceIndices: Seq[Int],
                     colType: Column.ColumnType,
                     extractor: TypedFieldExtractor[_]): ComputedColumn = {
    ComputedColumn(0, expr, dataset, colType, sourceIndices, extractor)
  }

  def wrap[F: TypedFieldExtractor, T](func: F => T): WrappedExtractor[T, F] = new WrappedExtractor(func)

  def computedColumn(expr: String, dataset: String, c: SingleColumnInfo,
                     extractor: TypedFieldExtractor[_]): ComputedColumn =
    computedColumn(expr, dataset, Seq(c.colIndex), c.colType, extractor)

  def computedColumn(expr: String,
                     dataset: String,
                     c: SingleColumnInfo,
                     colType: Column.ColumnType,
                     extractor: TypedFieldExtractor[_]): ComputedColumn =
    computedColumn(expr, dataset, Seq(c.colIndex), colType, extractor)
}
