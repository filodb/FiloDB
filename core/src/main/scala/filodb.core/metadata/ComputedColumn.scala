package filodb.core.metadata

import org.scalactic._
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._

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
                          sourceColumns: Seq[String],    // names of source columns
                          keyType: KeyType) extends Column {
  def name: String = expr
}

object ComputedColumn {
  import SimpleComputations._

  val AllComputations = Seq(ConstStringComputation,
                            GetOrElseComputation,
                            RoundComputation,
                            TimesliceComputation,
                            StringPrefixComputation)
  val nameToComputation = AllComputations.map { comp => comp.funcName -> comp }.toMap

  def isComputedColumn(expr: String): Boolean = expr.startsWith(":")

  /**
   * Analyzes a computed column expression, matching it with the correct ColumnComputation.
   * @returns NoSuchFunction if a ColumnComputation is not found; if found,
   *          then the return value from the analyze() method of the computation
   */
  def analyze(expr: String,
              dataset: String,
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
  import TypeCheckedTripleEquals._

  // The name of the computation function, without the leading ":"
  def funcName: String

  /**
   * Attempt to analyze the user arguments and produce a ComputedColumn.
   * @returns either a ComputedColumn with valid ComputedKeyType, or one of the InvalidComputedColumnSpec
   *          values.  NOTE: does not need to fill in id, which will be generated/replaced later.
   */
  def analyze(expr: String,
              dataset: String,
              schema: Seq[Column]): ComputedColumn Or InvalidComputedColumnSpec

  def userArgs(expr: String): Seq[String] = expr.split(" ").toSeq.drop(1)

  /**
   * Breaks up expr into space-separated arguments, validating a fixed number of arguments
   * @param expr string expression
   * @param numArgs expected number of arguments
   * @return Good(Seq[String]) if number of args matches numArgs, or Bad(WrongNumberArguments)
   */
  def fixedNumArgs(expr: String, numArgs: Int): Seq[String] Or InvalidComputedColumnSpec = {
    val args = userArgs(expr)
    if (args.length == numArgs) Good(args) else Bad(WrongNumberArguments(args.length, numArgs))
  }

  def columnIndex(schema: Seq[Column], colName: String): Int Or InvalidComputedColumnSpec = {
    val sourceColIndex = schema.indexWhere(_.name === colName)
    if (sourceColIndex < 0) { Bad(BadArgument(s"Could not find source column $colName")) }
    else                    { Good(sourceColIndex) }
  }

  def validatedColumnType(schema: Seq[Column], colIndex: Int, allowedTypes: Set[Column.ColumnType]):
      Column.ColumnType Or InvalidComputedColumnSpec = {
    val colType = schema(colIndex).columnType
    if (allowedTypes.contains(colType)) { Good(colType) }
    else { Bad(BadArgument(s"$colType not in allowed set $allowedTypes")) }
  }

  def parseParam(keyType: KeyType, arg: String): keyType.T Or InvalidComputedColumnSpec = {
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
  def parse(expr: String, schema: Seq[Column]): SingleColumnInfo Or InvalidComputedColumnSpec = {
    for { args <- fixedNumArgs(expr, 2)
          sourceColIndex <- columnIndex(schema, args(0))
          sourceColType = schema(sourceColIndex).columnType }
    yield { SingleColumnInfo(args(0), args(1), sourceColIndex, sourceColType) }
  }

  def parse(expr: String, schema: Seq[Column], allowedTypes: Set[Column.ColumnType]):
      SingleColumnInfo Or InvalidComputedColumnSpec = {
    for { args <- fixedNumArgs(expr, 2)
          sourceColIndex <- columnIndex(schema, args(0))
          sourceColType <- validatedColumnType(schema, sourceColIndex, allowedTypes) }
    yield { SingleColumnInfo(args(0), args(1), sourceColIndex, sourceColType) }
  }

  def computedColumn(expr: String,
                     dataset: String,
                     sourceColumns: Seq[String],
                     colType: Column.ColumnType,
                     keyType: KeyType)
                    (readerFunc: RowReader => keyType.T): ComputedColumn = {
    val computedKeyType = ComputedKeyTypes.getComputedType(keyType)(readerFunc)
    ComputedColumn(0, expr, dataset, colType, sourceColumns, computedKeyType)
  }

  def computedColumnWithDefault(expr: String, dataset: String, c: SingleColumnInfo)
                    (default: c.keyType.T)(valueFunc: c.keyType.T => c.keyType.T): ComputedColumn =
    computedColumnWithDefault(expr, dataset, c, c.colType, c.keyType)(default)(valueFunc)

  def computedColumnWithDefault(expr: String,
                                dataset: String,
                                sourceColInfo: SingleColumnInfo,
                                colType: Column.ColumnType,
                                destKeyType: KeyType)
                               (default: destKeyType.T)
                               (valueFunc: sourceColInfo.keyType.T => destKeyType.T): ComputedColumn = {
    val extractor = sourceColInfo.keyType.asInstanceOf[SingleKeyTypeBase[sourceColInfo.keyType.T]].extractor
    val colIndex = sourceColInfo.colIndex
    computedColumn(expr, dataset, Seq(sourceColInfo.sourceColumn), colType, destKeyType) {
    (r: RowReader) =>
      if (r.notNull(colIndex)) {
        valueFunc(extractor.getField(r, colIndex))
      } else { default }
    }
  }
}