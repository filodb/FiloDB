package filodb.coordinator.parse

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers, RegexParsers}

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query
import filodb.core.query.ColumnFilter
import filodb.query._


//noinspection ScalaStyle
// scalastyle:off

trait AST {

  sealed trait PromToken

  sealed trait Expression

  case class QueryParams(start: Long, step: Long, end: Long)

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression

  /**
    * The following label matching operators exist:
    * = Select labels that are exactly equal to the provided string.
    * =: Select labels that are exactly equal to the provided string.
    * !=: Select labels that are not equal to the provided string.
    * =~: Select labels that regex-match the provided string (or substring).
    * !~: Select labels that do not regex-match the provided string (or substring).
    *
    * The following binary comparison operators exist in Prometheus:
    * == (equal)
    * != (not-equal)
    * > (greater-than)
    * < (less-than)
    * >= (greater-or-equal)
    * <= (less-or-equal)
    **/
  sealed trait Operator extends PromToken

  case object EqualMatch extends Operator

  case object ExactMatch extends Operator

  case object RegexMatch extends Operator

  case object NotRegexMatch extends Operator

  sealed trait Comparision extends Operator {
    def isBool: Boolean
  }

  case class NotEqual(isBool: Boolean) extends Comparision

  case class Eq(isBool: Boolean) extends Comparision

  case class Gt(isBool: Boolean) extends Comparision

  case class Gte(isBool: Boolean) extends Comparision

  case class Lt(isBool: Boolean) extends Comparision

  case class Lte(isBool: Boolean) extends Comparision

  case class LabelMatch(label: String, labelMatchOp: Operator, value: String) extends PromToken

  sealed trait ArithmeticOp extends Operator

  case object Add extends ArithmeticOp

  case object Sub extends ArithmeticOp

  case object Mul extends ArithmeticOp

  case object Div extends ArithmeticOp

  case object Mod extends ArithmeticOp

  case object Pow extends ArithmeticOp

  sealed trait SetOp extends Operator

  case object And extends SetOp

  case object Or extends SetOp

  case object Unless extends SetOp


  /**
    * Time durations are specified as a number
    * followed immediately by one of the following units:
    * s - seconds
    * m - minutes
    * h - hours
    * d - days
    * w - weeks
    * y - years
    */

  sealed trait TimeUnit {
    def millis: Long
  }

  case object Second extends TimeUnit {
    override def millis: Long = 1000L
  }

  case object Minute extends TimeUnit {
    override def millis: Long = Second.millis * 60
  }

  case object Hour extends TimeUnit {
    override def millis: Long = Minute.millis * 60
  }

  case object Day extends TimeUnit {
    override def millis: Long = Hour.millis * 24
  }

  case object Week extends TimeUnit {
    override def millis: Long = Day.millis * 7
  }

  case object Year extends TimeUnit {
    override def millis: Long = Week.millis * 52
  }

  case class Duration(scale: Int, timeUnit: TimeUnit) {
    if (scale <= 0) throw new IllegalArgumentException("Duration should be greater than zero")
    val millis = scale * timeUnit.millis
  }

  case class Offset(duration: Duration)

  sealed trait NumericalExpression extends Expression

  case class Scalar(float: Double) extends NumericalExpression

  case class ArithmeticExpression(lhs: Scalar, op: ArithmeticOp, rhs: Scalar)
    extends NumericalExpression

  sealed trait JoinMatching {
    def labels: Seq[String]
  }

  case class Ignoring(labels: Seq[String]) extends JoinMatching

  case class On(labels: Seq[String]) extends JoinMatching

  sealed trait JoinGrouping {
    def labels: Seq[String]
  }

  case class GroupLeft(labels: Seq[String]) extends JoinGrouping

  case class GroupRight(labels: Seq[String]) extends JoinGrouping

  sealed trait Cardinality

  case object OneToOne extends Cardinality

  case object OneToMany extends Cardinality

  case object ManyToOne extends Cardinality

  case object ManyToMany extends Cardinality

  case class VectorMatch(matching: Option[JoinMatching],
                         grouping: Option[JoinGrouping]) {
    lazy val cardinality: Cardinality = grouping match {
      case Some(GroupLeft(_)) => OneToMany
      case Some(GroupRight(_)) => ManyToOne
      case None => OneToOne
    }

    def notEmpty: Boolean = matching.isDefined || grouping.isDefined

    def validate(operator: Operator, lhs: Expression, rhs: Expression): Unit = {
      if (notEmpty && (lhs.isInstanceOf[Scalar] || rhs.isInstanceOf[Scalar])) {
        throw new IllegalArgumentException("vector matching only allowed between instant vectors")
      }
      if (grouping.isDefined && operator.isInstanceOf[SetOp]) {
        throw new IllegalArgumentException("no grouping allowed for and, or, unless operations")
      }
      validateGroupAndMatch()
    }

    private def validateGroupAndMatch(): Unit = if (grouping.isDefined && matching.isDefined) {
      val group = grouping.get
      val matcher = matching.get
      val matchLabels = matcher.labels
      val groupLabels = group.labels
      groupLabels.foreach { label =>
        if (matchLabels.contains(label) && matcher.isInstanceOf[On]) {
          throw new IllegalArgumentException("Labels must not occur in ON and GROUP clause at once")
        }
      }
    }
  }


  sealed trait SeriesExpression extends Expression {
    def toLogicalPlanNode(dataset: Dataset,
                          queryParams: QueryParams): LogicalPlan
  }

  /**
    * Instant vector selectors allow the selection of a set of time series
    * and a single sample value for each at a given timestamp (instant):
    * in the simplest form, only a metric name is specified.
    * This results in an instant vector containing elements
    * for all time series that have this metric name.
    * It is possible to filter these time series further by
    * appending a set of labels to match in curly braces ({}).
    */

  case class InstantExpression(metricName: String,
                               labelSelection: Seq[LabelMatch],
                               offset: Option[Duration]) extends SeriesExpression {

    private val nameLabels = labelSelection.filter(_.label == "__name__")

    if (nameLabels.nonEmpty && !nameLabels.head.label.equals(metricName)) {
      throw new IllegalArgumentException("Metric name should not be set twice")
    }

    private val columnFilters = labelSelection.map { labelMatch =>
      //TODO All operators need to be taken care
      ColumnFilter(labelMatch.label, query.Filter.Equals(labelMatch.value))
    }

    private val nameFilter = ColumnFilter(metricName, query.Filter.Equals("__name__"))

    def toLogicalPlanNode(dataset: Dataset,
                          queryParams: QueryParams): LogicalPlan = {
      PeriodicSeries(
        RawSeries(
          interval(dataset, queryParams),
          columnFilters :+ nameFilter,
          dataset.dataColumns.map(_.name)),
        queryParams.start, queryParams.step, queryParams.end
      )
    }


    def interval(dataset: Dataset,
                 queryParams: QueryParams): RangeSelector = {
      //val endTimestamp = queryEvaluationTimestamp - offset.map(_.millis).getOrElse(0)
      val startTimestamp = queryParams.start - Duration(5, Minute).millis
      val endTimestamp = queryParams.end
      val start: BinaryRecord = BinaryRecord(dataset, Seq(startTimestamp))
      val end: BinaryRecord = BinaryRecord(dataset, Seq(endTimestamp))
      IntervalSelector(start, end)
    }
  }

  /**
    * Range vector literals work like instant vector literals,
    * except that they select a range of samples back from the current instant.
    * Syntactically, a range duration is appended in square brackets ([])
    * at the end of a vector selector to specify how far back in time values
    * should be fetched for each resulting range vector element.
    */
  case class RangeExpression(metricName: String,
                             labelSelection: Seq[LabelMatch],
                             window: Duration,
                             offset: Option[Duration]) extends SeriesExpression {
    private val nameLabels = labelSelection.filter(_.label == "__name__")

    if (nameLabels.nonEmpty && !nameLabels.head.label.equals(metricName)) {
      throw new IllegalArgumentException("Metric name should not be set twice")
    }

    private val columnFilters = labelSelection.map { labelMatch =>
      //TODO All operators need to be taken care
      ColumnFilter(labelMatch.label, query.Filter.Equals(labelMatch.value))
    }

    private val nameFilter = ColumnFilter(metricName, query.Filter.Equals("__name__"))

    def toLogicalPlanNode(dataset: Dataset,
                          queryParams: QueryParams): LogicalPlan = {
      if (queryParams.start != queryParams.end) {
        //TODO wrapped functions will change this
        throw new UnsupportedOperationException("Range expression is not allowed in query_range")
      }
      RawSeries(
        interval(dataset, queryParams),
        columnFilters :+ nameFilter,
        dataset.dataColumns.map(_.name)
      )
    }

    def interval(dataset: Dataset,
                 queryParams: QueryParams): RangeSelector = {

      val start: BinaryRecord = BinaryRecord(dataset, Seq(queryParams.start - window.millis))
      val end: BinaryRecord = BinaryRecord(dataset, Seq(queryParams.end))
      IntervalSelector(start, end)
    }
  }


  case class UnaryExpression(operator: Operator, operand: Expression) extends Expression

  case class BinaryExpression(lhs: Expression,
                              operator: Operator,
                              vectorMatch: Option[VectorMatch],
                              rhs: Expression) extends Expression {

    operator match {
      case setOp: SetOp =>
        if (lhs.isInstanceOf[Scalar] || rhs.isInstanceOf[Scalar])
          throw new IllegalArgumentException("set operators not allowed in binary scalar expression")

      case comparision: Comparision if !comparision.isBool =>
        if (lhs.isInstanceOf[Scalar] && rhs.isInstanceOf[Scalar])
          throw new IllegalArgumentException("comparisons between scalars must use BOOL modifier")
      case _ =>
    }
    if (vectorMatch.isDefined) {
      vectorMatch.get.validate(operator, lhs, rhs)
    }

  }

  case class Function(name: String, params: Seq[Expression]) extends Expression

  sealed trait AggregateGrouping

  case class Without(labels: Seq[String]) extends AggregateGrouping

  case class By(labels: Seq[String]) extends AggregateGrouping

  case class AggregateExpression(function: Function,
                                 aggregateGrouping: AggregateGrouping,
                                 altFunctionParams: Seq[Expression]) extends Expression {
    if (function.params.nonEmpty && altFunctionParams.nonEmpty) {
      throw new IllegalArgumentException("Can define function params only once")
    }
    if (function.params.isEmpty && altFunctionParams.isEmpty) {
      throw new IllegalArgumentException("Need to define function params once")
    }

  }


}

trait BaseParser extends AST with JavaTokenParsers with RegexParsers with PackratParsers {


  lazy val identifier: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => Identifier(str) }
  }

  lazy val labelIdentifier: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_:]*".r ^^ { str => Identifier(str) }
  }

  lazy val string: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_:|`~!@$#%^&*()-+=?><:;{}]*".r ^^ { str => Identifier(str) }
  }

  protected lazy val quotedStr: PackratParser[Identifier] =
    ("\"" | "'") ~ string ~ ("\"" | "'") ^^ {
      case ignore1 ~ id ~ ignore2 => id
    }

  protected val OFFSET = Keyword("OFFSET")
  protected val IGNORING = Keyword("IGNORING")
  protected val GROUP_LEFT = Keyword("GROUP_LEFT")
  protected val GROUP_RIGHT = Keyword("GROUP_RIGHT")
  protected val ON = Keyword("ON")
  protected val WITHOUT = Keyword("WITHOUT")
  protected val BY = Keyword("BY")
  protected val AND = Keyword("AND")
  protected val OR = Keyword("OR")
  protected val UNLESS = Keyword("UNLESS")

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }
}

//  }////////////////////    OPERATORS ///////////////////////////////////////////
trait Operators extends BaseParser {


  lazy val equalMatch = "=" ^^ (_ => EqualMatch)

  lazy val exactMatch = "=:" ^^ (_ => EqualMatch)

  lazy val regexMatchOp = "=~" ^^ (_ => RegexMatch)

  lazy val notRegexMatchOp = "!~" ^^ (_ => NotRegexMatch)

  lazy val notEqual = "!=" ~ "bool".? ^^ { case ignore ~ op => NotEqual(op.isDefined) }

  lazy val equal = "==" ~ "bool".? ^^ { case ignore ~ op => Eq(op.isDefined) }

  lazy val gt = ">" ~ "bool".? ^^ { case ignore ~ op => Gt(op.isDefined) }

  lazy val gte = ">=" ~ "bool".? ^^ { case ignore ~ op => Gte(op.isDefined) }

  lazy val lt = "<" ~ "bool".? ^^ { case ignore ~ op => Lt(op.isDefined) }

  lazy val lte = "<=" ~ "bool".? ^^ { case ignore ~ op => Lte(op.isDefined) }

  lazy val labelMatchOp = notRegexMatchOp | regexMatchOp | exactMatch | notEqual | equalMatch

  lazy val comparisionOp = gte | lte | notEqual | gt | lt | equal

  lazy val labelMatch: PackratParser[LabelMatch] = identifier ~ labelMatchOp ~ quotedStr ^^ {
    case label ~ op ~ value => LabelMatch(label.str, op, value.str)
  }

  lazy val labelSelection: PackratParser[Seq[LabelMatch]] =
    "{" ~> repsep(labelMatch, ",") <~ "}" ^^ {
      Seq() ++ _
    }


  lazy val add = "+" ^^ (_ => Add)

  lazy val sub = "-" ^^ (_ => Sub)

  lazy val mul = "*" ^^ (_ => Mul)

  lazy val div = "/" ^^ (_ => Div)

  lazy val mod = "%" ^^ (_ => Mod)

  lazy val pow = "^" ^^ (_ => Pow)

  lazy val arithmeticOp = pow | add | sub | mul | div | mod


  lazy val and = AND ^^ (_ => And)

  lazy val or = OR ^^ (_ => Or)

  lazy val unless = UNLESS ^^ (_ => Unless)

  lazy val setOp: PackratParser[SetOp] = and | or | unless

  lazy val binaryOp: PackratParser[Operator] = comparisionOp | setOp | arithmeticOp


  ////////////////////// END OPERATORS ///////////////////////////////////////////
}

////////////////////// UNITS ///////////////////////////////////////////

trait Units extends BaseParser {

  lazy val second = "s" ^^ (_ => Second)

  lazy val minute = "m" ^^ (_ => Minute)

  lazy val hour = "h" ^^ (_ => Hour)

  lazy val day = "d" ^^ (_ => Day)

  lazy val week = "w" ^^ (_ => Week)

  lazy val year = "y" ^^ (_ => Year)

  lazy val timeUnit = second | minute | hour | day | week | year


  lazy val timeDuration: PackratParser[Duration] = wholeNumber ~ timeUnit ^^ {
    case d ~ tu => Duration(Integer.parseInt(d), tu)
  }
  lazy val offset: PackratParser[Offset] = OFFSET ~ timeDuration ^^ {
    case ignore ~ t => Offset(t)
  }
}

////////////////////// END UNITS ///////////////////////////////////////////
////////////////////// NUMERICALS ///////////////////////////////////////////
trait Numericals extends Units with Operators {


  lazy val scalar: PackratParser[Scalar] = floatingPointNumber ^^ {
    case s => Scalar(java.lang.Double.parseDouble(s))
  }


  lazy val arithmeticExpression: PackratParser[ArithmeticExpression] =
    "(".? ~ scalar ~ arithmeticOp ~ scalar ~ ")".? ^^ {
      case p1 ~ lhs ~ op ~ rhs ~ p2 => ArithmeticExpression(lhs, op, rhs)
    }

  lazy val numericalExpression: PackratParser[NumericalExpression] = arithmeticExpression | scalar

}

////////////////////// END NUMERICALS///////////////////////////////////////////
////////////////////// JOINS ///////////////////////////////////////////
trait Joins extends Numericals {


  lazy val ignoring: PackratParser[Ignoring] = IGNORING ~ labels ^^ {
    case unused0 ~ seq => Ignoring(seq.map(_.str))
  }

  lazy val on: PackratParser[On] = ON ~ labels ^^ {
    case unused0 ~ seq => On(seq.map(_.str))
  }

  lazy val joinMatcher: PackratParser[JoinMatching] = ignoring | on

  lazy val labels: PackratParser[Seq[Identifier]] = "(" ~> repsep(identifier, ",") <~ ")" ^^ {
    Seq() ++ _
  }

  lazy val groupLeft: PackratParser[GroupLeft] = GROUP_LEFT ~ labels.? ^^ {
    case unused0 ~ seq => GroupLeft(seq.getOrElse(Seq.empty).map(_.str))
  }

  lazy val groupRight: PackratParser[GroupRight] = GROUP_RIGHT ~ labels.? ^^ {
    case unused0 ~ seq => GroupRight(seq.getOrElse(Seq.empty).map(_.str))
  }

  lazy val grouping: PackratParser[JoinGrouping] = groupLeft | groupRight

  lazy val vectorMatch: PackratParser[VectorMatch] = joinMatcher.? ~ grouping.? ^^ {
    case m ~ g => VectorMatch(m, g)
  }


}

////////////////////// END JOINS ///////////////////////////////////////////
////////////////////// SELECTORS ///////////////////////////////////////////
trait Selectors extends Joins {


  lazy val instantVectorSelector: PackratParser[InstantExpression]
  = labelIdentifier ~ labelSelection.? ~ offset.? ^^ {
    case metricName ~ ls ~ opt =>
      InstantExpression(metricName.str, ls.getOrElse(Seq.empty), opt.map(_.duration))
  }


  lazy val rangeVectorSelector: PackratParser[RangeExpression] =
    labelIdentifier ~ labelSelection.? ~ "[" ~ timeDuration ~ "]" ~ offset.? ^^ {
      case metricName ~ ls ~ leftBracket ~ td ~ rightBracket ~ opt =>
        RangeExpression(metricName.str, ls.getOrElse(Seq.empty), td, opt.map(_.duration))
    }

  lazy val selector: PackratParser[SeriesExpression] = rangeVectorSelector | instantVectorSelector


}

////////////////////// END SELECTORS ///////////////////////////////////////////
////////////////////// AGGREGATES ///////////////////////////////////////////
trait Aggregates extends Selectors {


  lazy val without: PackratParser[Without] = WITHOUT ~ labels ^^ {
    case unused0 ~ seq => Without(seq.map(_.str))
  }

  lazy val by: PackratParser[By] = BY ~ labels ^^ {
    case unused0 ~ seq => By(seq.map(_.str))
  }

  lazy val aggregateGrouping: PackratParser[AggregateGrouping] = without | by

}

////////////////////// END AGGREGATES ///////////////////////////////////////////
////////////////////// EXPRESSIONS ///////////////////////////////////////////
trait Expressions extends Aggregates {

  lazy val unaryExpression: PackratParser[UnaryExpression] =
    (add | sub) ~ (numericalExpression | instantVectorSelector | rangeVectorSelector) ^^ {
      case op ~ exp => UnaryExpression(op, exp)
    }


  lazy val binaryExpression: PackratParser[BinaryExpression] =
    "(".? ~ expression ~ binaryOp ~ vectorMatch.? ~ expression ~ ")".? ^^ {
      case p1 ~ lhs ~ op ~ vm ~ rhs ~ p2 => BinaryExpression(lhs, op, vm, rhs)
    }


  lazy val functionParams: PackratParser[Seq[Expression]] =
    "(" ~> repsep(expression, ",") <~ ")" ^^ {
      Seq() ++ _
    }

  lazy val function: PackratParser[Function] = identifier ~ functionParams ^^ {
    case name ~ params => Function(name.str, params)
  }

  lazy val aggregateExpression: PackratParser[AggregateExpression] =
    identifier ~ functionParams.? ~ aggregateGrouping ~ functionParams.? ^^ {
      case fn ~ params ~ ag ~ ls => AggregateExpression(
        Function(fn.str, params.getOrElse(Seq.empty)), ag, ls.getOrElse(Seq.empty)
      )
    }

  lazy val expression: PackratParser[Expression] =
    binaryExpression | aggregateExpression | function | unaryExpression | selector | numericalExpression | quotedStr

}

////////////////////// END EXPRESSIONS ///////////////////////////////////////////

object PrometheusQLParser extends Expressions {
  /**
    * Parser is not whitespace sensitive
    */
  override lazy val skipWhitespace: Boolean = true

  override val whiteSpace = "[ \t\r\f]+".r

  val FiveMinutes = Duration(5, Minute).millis

  def parseQuery(query: String): Expression = {
    parseAll(expression, query) match {
      case s: Success[_] => s.get.asInstanceOf[Expression]
      case e: Error => handleError(e, query)
      case f: Failure => handleFailure(f, query)
    }
  }

  def queryToLogicalPlan(query: String,
                         dataset: Dataset,
                         queryTimestamp: Long): LogicalPlan = {
    val defaultQueryParams = QueryParams(queryTimestamp, 1, queryTimestamp)
    queryRangeToLogicalPlan(query, dataset, defaultQueryParams)
  }

  private def queryRangeToLogicalPlan(query: String,
                                      dataset: Dataset,
                                      queryParams: PrometheusQLParser.QueryParams) = {
    val expression = parseQuery(query)
    expression match {
      case s: SeriesExpression => s.toLogicalPlanNode(dataset, queryParams)
      case _ => throw new UnsupportedOperationException("Only instant and range expressions are supported for now")
    }
  }

  private def handleError(e: Error, input: String) = {
    val msg = "Cannot parse [" + input + "] because " + e.msg
    throw new IllegalArgumentException(msg)
  }

  private def handleFailure(f: Failure, input: String) = {
    val msg = "Cannot parse [" + input + "] because " + f.msg
    throw new IllegalArgumentException(msg)
  }

}

// scalastyle:on