package filodb.prometheus.parse

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers, RegexParsers}

import filodb.prometheus.ast.{Expressions, TimeRangeParams, TimeStepParams}
import filodb.query._

trait BaseParser extends Expressions with JavaTokenParsers with RegexParsers with PackratParsers {

  lazy val identifier: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => Identifier(str) }
  }

  lazy val labelIdentifier: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_:\\-\\.]*".r ^^ { str => Identifier(str) }
  }

  lazy val string: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_:|`~!@$#%^&*()s+=?><:;{}-]*".r ^^ { str => Identifier(str) }
  }

  protected lazy val quotedSeries: PackratParser[Identifier] =
    "([\"'])(?:\\\\\\1|.)*?\\1".r ^^ { str =>  Identifier(str.substring(1, str.size-1)) } //remove quotes

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
    ("""(?i)""" + kw.key + """(?!\w)""").r
  }
}

//  }////////////////////    OPERATORS ///////////////////////////////////////////
trait Operator extends BaseParser {


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

  lazy val labelMatch: PackratParser[LabelMatch] = identifier ~ labelMatchOp ~ quotedSeries ^^ {
    case label ~ op ~ value => LabelMatch(label.str, op, value.str)
  }

  lazy val labelSelection: PackratParser[Seq[LabelMatch]] =
    "{" ~> repsep(labelMatch, ",") <~ "}" ^^ {
      Seq() ++ _
    }

  lazy val labels: PackratParser[Seq[Identifier]] = "(" ~> repsep(identifier, ",") <~ ")" ^^ {
    Seq() ++ _
  }

  lazy val add = "+" ^^ (_ => Add)

  lazy val sub = "-" ^^ (_ => Sub)

  lazy val mul = "*" ^^ (_ => Mul)

  lazy val div = "/" ^^ (_ => Div)

  lazy val mod = "%" ^^ (_ => Mod)

  lazy val pow = "^" ^^ (_ => Pow)

  lazy val arithmeticOp = pow | mul | div | mod | add | sub


  lazy val and = AND ^^ (_ => And)

  lazy val or = OR ^^ (_ => Or)

  lazy val unless = UNLESS ^^ (_ => Unless)

  lazy val setOp: PackratParser[SetOp] = and | unless | or

  lazy val binaryOp: PackratParser[Operator] = arithmeticOp | comparisionOp | setOp


  ////////////////////// END OPERATORS ///////////////////////////////////////////
}

////////////////////// UNITS ///////////////////////////////////////////

trait Unit extends BaseParser {

  lazy val second = "s" ^^ (_ => Second)

  lazy val minute = "m" ^^ (_ => Minute)

  lazy val hour = "h" ^^ (_ => Hour)

  lazy val day = "d" ^^ (_ => Day)

  lazy val week = "w" ^^ (_ => Week)

  lazy val year = "y" ^^ (_ => Year)

  lazy val timeUnit = second | minute | hour | day | week | year


  lazy val duration: PackratParser[Duration] = wholeNumber ~ timeUnit ^^ {
    case d ~ tu => Duration(Integer.parseInt(d), tu)
  }
  lazy val offset: PackratParser[Offset] = OFFSET ~ duration ^^ {
    case ignore ~ t => Offset(t)
  }
}

////////////////////// END UNITS ///////////////////////////////////////////
////////////////////// NUMERICALS ///////////////////////////////////////////
trait Numeric extends Unit with Operator {


  lazy val scalar: PackratParser[Scalar] = floatingPointNumber ^^ {
    case s => Scalar(java.lang.Double.parseDouble(s))
  }


  lazy val arithmeticExpression: PackratParser[ArithmeticExpression] =
    "(".? ~ scalar ~ arithmeticOp ~ scalar ~ ")".? ^^ {
      case p1 ~ lhs ~ op ~ rhs ~ p2 => ArithmeticExpression(lhs, op, rhs)
    }

  lazy val numericalExpression: PackratParser[ScalarExpression] = arithmeticExpression | scalar

}

////////////////////// END NUMERICALS///////////////////////////////////////////
////////////////////// JOINS ///////////////////////////////////////////
trait Join extends Numeric {


  lazy val ignoring: PackratParser[Ignoring] = IGNORING ~ labels ^^ {
    case unused0 ~ seq => Ignoring(seq.map(_.str))
  }

  lazy val on: PackratParser[On] = ON ~ labels ^^ {
    case unused0 ~ seq => On(seq.map(_.str))
  }

  lazy val joinMatcher: PackratParser[JoinMatching] = ignoring | on


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
trait Selector extends Operator with Unit with BaseParser {
  protected lazy val simpleSeries: PackratParser[InstantExpression] =
    "([\"'])(?:\\\\\\1|.)*?\\1".r ^^ { str => InstantExpression(str, Seq.empty, None) }


  lazy val instantVectorSelector: PackratParser[InstantExpression]
  = labelIdentifier ~ labelSelection.? ~ offset.? ^^ {
    case metricName ~ ls ~ opt =>
      InstantExpression(metricName.str, ls.getOrElse(Seq.empty), opt.map(_.duration))
  }

  lazy val rangeVectorSelector: PackratParser[RangeExpression] =
    labelIdentifier ~ labelSelection.? ~ "[" ~ duration ~ "]" ~ offset.? ^^ {
      case metricName ~ ls ~ leftBracket ~ td ~ rightBracket ~ opt =>
        RangeExpression(metricName.str, ls.getOrElse(Seq.empty), td, opt.map(_.duration))
    }

  lazy val vector: PackratParser[Vector] = rangeVectorSelector | instantVectorSelector
}

////////////////////// END SELECTORS ///////////////////////////////////////////
////////////////////// AGGREGATES ///////////////////////////////////////////
trait Aggregates extends Operator with BaseParser {

  protected val SUM = Keyword("SUM")
  protected val AVG = Keyword("AVG")
  protected val MIN = Keyword("MIN")
  protected val MAX = Keyword("MAX")
  protected val STD_DEV = Keyword("STDDEV")
  protected val STD_VAR = Keyword("STDVAR")
  protected val COUNT = Keyword("COUNT")
  protected val COUNT_VALUES = Keyword("COUNT_VALUES")
  protected val BOTTOMK = Keyword("BOTTOMK")
  protected val TOPK = Keyword("TOPK")
  protected val QUANTILE = Keyword("QUANTILE")

  protected val SUM_OVER_TIME = Keyword("SUM_OVER_TIME")
  protected val AVG_OVER_TIME = Keyword("AVG_OVER_TIME")
  protected val MIN_OVER_TIME = Keyword("MIN_OVER_TIME")
  protected val MAX_OVER_TIME = Keyword("MAX_OVER_TIME")
  protected val STDDEV_OVER_TIME = Keyword("STDDEV_OVER_TIME")
  protected val STDVAR_OVER_TIME = Keyword("STDVAR_OVER_TIME")
  protected val COUNT_OVER_TIME = Keyword("COUNT_OVER_TIME")
  protected val QUANTILE_OVER_TIME = Keyword("QUANTILE_OVER_TIME")

  lazy val aggregateOperator: PackratParser[String] =
    SUM | AVG | MIN | MAX | STD_DEV | STD_VAR | COUNT_VALUES | COUNT | BOTTOMK | TOPK | QUANTILE

  lazy val aggregateRangeOperator: PackratParser[String] =
    SUM_OVER_TIME | AVG_OVER_TIME | MIN_OVER_TIME | MAX_OVER_TIME | STDDEV_OVER_TIME |
      STDVAR_OVER_TIME | COUNT_OVER_TIME | QUANTILE_OVER_TIME


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
trait Expression extends Aggregates with Selector with Numeric with Join {

  lazy val unaryExpression: PackratParser[UnaryExpression] =
    (add | sub) ~ (numericalExpression | instantVectorSelector | rangeVectorSelector) ^^ {
      case op ~ exp => UnaryExpression(op, exp)
    }


  lazy val binaryExpression: PackratParser[BinaryExpression] =
    expression ~ binaryOp ~ vectorMatch.? ~ expression ^^ {
      case lhs ~ op ~ vm ~ rhs => BinaryExpression(lhs, op, vm, rhs)
    }


  lazy val functionParams: PackratParser[Seq[Expression]] =
    "(" ~> repsep(expression, ",") <~ ")" ^^ {
      Seq() ++ _
    }

  lazy val function: PackratParser[Function] = identifier ~ functionParams ^^ {
    case name ~ params => Function(name.str, params)
  }

  lazy val aggregateExpression: PackratParser[AggregateExpression] =
    aggregateOperator ~ functionParams.? ~ aggregateGrouping.? ~ functionParams.? ^^ {
      case fn ~ params ~ ag ~ ls => AggregateExpression(
        fn, params.getOrElse(Seq.empty), ag, ls.getOrElse(Seq.empty)
      )
    }

  lazy val expression: PackratParser[Expression] =
    binaryExpression | aggregateExpression |
      function | unaryExpression | vector | numericalExpression | simpleSeries | "(" ~> expression <~ ")"

}

////////////////////// END EXPRESSIONS ///////////////////////////////////////////

object Parser extends Expression {
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

  def metadataQueryToLogicalPlan(query: String, timeParams: TimeRangeParams): LogicalPlan = {
    val expression = parseQuery(query)
    expression match {
      case p: InstantExpression => MetadataExpression(p).toMetadataQueryPlan(timeParams)
      case _ => throw new UnsupportedOperationException()
    }
  }

  def queryToLogicalPlan(query: String, queryTimestamp: Long): LogicalPlan = {
    val defaultQueryParams = TimeStepParams(queryTimestamp, 1, queryTimestamp)
    queryRangeToLogicalPlan(query, defaultQueryParams)
  }

  def queryRangeToLogicalPlan(query: String, timeParams: TimeRangeParams): LogicalPlan = {
    val expression = parseQuery(query)
    expression match {
      case p: PeriodicSeries => p.toPeriodicSeriesPlan(timeParams)
      case r: SimpleSeries => r.toRawSeriesPlan(timeParams, isRoot = true)
      case _ => throw new UnsupportedOperationException()
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