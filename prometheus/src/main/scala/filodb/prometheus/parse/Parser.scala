package filodb.prometheus.parse

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers, RegexParsers}

import filodb.core.query.{ColumnFilter, Filter}
import filodb.prometheus.ast.{Expressions, TimeRangeParams, TimeStepParams}
import filodb.query._

object BaseParser {
  val whiteSpace = "[ \t\r\f\n]+".r
}

trait BaseParser extends Expressions with JavaTokenParsers with RegexParsers with PackratParsers {

  lazy val labelNameIdentifier: PackratParser[Identifier] = {
    "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => Identifier(str) }
  }

  lazy val metricNameIdentifier: PackratParser[Identifier] = {
    "[a-zA-Z_:][a-zA-Z0-9_:\\-\\.]*".r ^^ { str => Identifier(str) }
  }

  protected lazy val labelValueIdentifier: PackratParser[Identifier] = {
    // Parse a quoted identifier, supporting escapes, with quotes removed. Note that this
    // originally relied on a complex regex with capturing groups. The way capturing groups are
    // processed by the Java regex class results in deep recursion and a stack overflow error
    // for long identifiers. In addition, the regex could only detect escape patterns, but it
    // couldn't replace them. As a result, an additional step was required to parse the string
    // again, searching and replacing the escapes. Parsers for "real" programming languages
    // never use regular expressions, because they are limited in capability. Custom code is
    // certainly "bigger", but it's much more flexible overall. This also makes it easier to
    // support additional types of promql strings that aren't supported as of yet. For example,
    // additional escapes, and backtick quotes which don't do escape processing.

    new PackratParser[Identifier]() {
      def apply(in: Input): ParseResult[Identifier] = {
        val source = in.source
        var offset = in.offset

        (whiteSpace findPrefixMatchOf (source.subSequence(offset, source.length))) match {
          case Some(matched) => offset += matched.end
          case None =>
        }

        val quote = source.charAt(offset); offset += 1

        if (quote != '\'' && quote != '"') {
          return Failure("quote character expected", in)
        }

        val bob = new StringBuilder()

        while (offset < source.length) {
          var c = source.charAt(offset); offset += 1

          if (c == quote) {
            return Success(Identifier(bob.toString()), in.drop(offset - in.offset))
          }

          if (c == '\\') {
            val next = source.charAt(offset); offset += 1
            c = next match {
              case '\\' | '\'' | '"' => next
              case 'f' => '\f'
              case 'n' => '\n'
              case 'r' => '\r'
              case 't' => '\t'
              case _ => return Error("illegal string escape: " + next, in)
            }
          }

          bob.append(c)
        }

        return Error("unfinished quoted identifier", in)
      }
    }
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

  lazy val labelMatch: PackratParser[LabelMatch] = labelNameIdentifier ~ labelMatchOp ~ labelValueIdentifier ^^ {
    case label ~ op ~ value => LabelMatch(label.str, op, value.str)
  }

  lazy val labelSelection: PackratParser[Seq[LabelMatch]] =
    "{" ~> repsep(labelMatch, ",") <~ "}" ^^ {
      Seq() ++ _
    }

  lazy val labels: PackratParser[Seq[Identifier]] = "(" ~> repsep(labelNameIdentifier, ",") <~ ")" ^^ {
    Seq() ++ _
  }

  lazy val labelValues: PackratParser[Seq[LabelMatch]] =
    repsep(labelMatch, ",") ^^ {
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

  lazy val interval = "i" ^^ (_ => IntervalMultiple)

  lazy val timeUnit = second | minute | hour | day | week | year | interval

  lazy val duration: PackratParser[Duration] = decimalNumber ~ timeUnit ^^ {
    case d ~ tu => Duration(d.toDouble, tu)
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

  lazy val numericalExpression: PackratParser[ScalarExpression] = scalar

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
    "([\"'])(?:\\\\\\1|.)*?\\1".r ^^ { str => InstantExpression(Some(str), Seq.empty, None) }


  lazy val instantVectorSelector: PackratParser[InstantExpression]
  = metricNameIdentifier ~ labelSelection.? ~ offset.? ^^ {
    case metricName ~ ls ~ opt =>
      InstantExpression(Some(metricName.str), ls.getOrElse(Seq.empty), opt.map(_.duration))
  }

  lazy val instantVectorSelector2: PackratParser[InstantExpression]
  = labelSelection ~ offset.? ^^ {
    case ls ~ opt =>
      InstantExpression(None, ls, opt.map(_.duration))
  }

  lazy val rangeVectorSelector: PackratParser[RangeExpression] =
    metricNameIdentifier ~ labelSelection.? ~ "[" ~ duration ~ "]" ~ offset.? ^^ {
      case metricName ~ ls ~ leftBracket ~ td ~ rightBracket ~ opt =>
        RangeExpression(Some(metricName.str), ls.getOrElse(Seq.empty), td, opt.map(_.duration))
    }

  lazy val rangeVectorSelector2: PackratParser[RangeExpression] =
    labelSelection ~ "[" ~ duration ~ "]" ~ offset.? ^^ {
      case ls ~ leftBracket ~ td ~ rightBracket ~ opt =>
        RangeExpression(None, ls, td, opt.map(_.duration))
    }

  lazy val vector: PackratParser[Vector] =
    rangeVectorSelector2 | rangeVectorSelector | instantVectorSelector2 | instantVectorSelector

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

  lazy val function: PackratParser[Function] = labelNameIdentifier ~ functionParams ^^ {
    case name ~ params => Function(name.str, params)
  }

  // For queries with aggregateGrouping before metric name
  // Example: sum without (sum_label) (some_metric)
  lazy val aggregateExpression1: PackratParser[AggregateExpression] =
    aggregateOperator ~ aggregateGrouping.?  ~ functionParams ~ functionParams.? ^^ {
      case fn ~ ag ~ params ~  ls => AggregateExpression(
        fn, params, ag, ls.getOrElse(Seq.empty)
      )
    }

  // For queries with aggregateGrouping after metric name
  // Example: sum (some_metric) without (some_label)
  lazy val aggregateExpression2: PackratParser[AggregateExpression] =
    aggregateOperator ~ functionParams ~ aggregateGrouping.? ~ functionParams.? ^^ {
      case fn ~ params ~ ag  ~  ls => AggregateExpression(
        fn, params, ag, ls.getOrElse(Seq.empty)
      )
    }

  lazy val expression: PackratParser[Expression] =
    binaryExpression | aggregateExpression2 | aggregateExpression1 |
      function | unaryExpression | vector | numericalExpression | simpleSeries | "(" ~> expression <~ ")"

}

////////////////////// END EXPRESSIONS ///////////////////////////////////////////

object Parser extends Expression {
  /**
    * Parser is not whitespace sensitive
    */
  override lazy val skipWhitespace: Boolean = true

  override val whiteSpace = BaseParser.whiteSpace

  def parseQuery(query: String): Expression = {
    parseAll(expression, query) match {
      case s: Success[_] => s.get.asInstanceOf[Expression]
      case e: Error => handleError(e, query)
      case f: Failure => handleFailure(f, query)
    }
  }

  def parseFilter(query: String): InstantExpression = {
    parseAll(expression, query) match {
      case s: Success[_] => s.get match {
        case ie: InstantExpression => ie
        case _ => throw new IllegalArgumentException(s"Expression $query is not a simple filter")
      }
      case e: Error => handleError(e, query)
      case f: Failure => handleFailure(f, query)
    }
  }

  def parseLabelValueFilter(query: String): Seq[LabelMatch] = {
    parseAll(labelValues, query) match {
      case s: Success[_] => s.get.asInstanceOf[Seq[LabelMatch]]
      case e: Error => handleError(e, query)
      case f: Failure => handleFailure(f, query)
    }
  }

  def metadataQueryToLogicalPlan(query: String, timeParams: TimeRangeParams,
                                 fetchFirstLastSampleTimes: Boolean = false): LogicalPlan = {
    val expression = parseQuery(query)
    expression match {
      case p: InstantExpression => p.toMetadataPlan(timeParams, fetchFirstLastSampleTimes)
      case _ => throw new UnsupportedOperationException()
    }
  }

  def labelValuesQueryToLogicalPlan(labelNames: Seq[String], filterQuery: Option[String],
                                    timeParams: TimeRangeParams): LogicalPlan = {
    filterQuery match {
      case Some(filter) =>
        val columnFilters = parseLabelValueFilter(filter).map { l =>
          l.labelMatchOp match {
            case EqualMatch => ColumnFilter(l.label, Filter.Equals(l.value))
            case NotRegexMatch => ColumnFilter(l.label, Filter.NotEqualsRegex(l.value))
            case RegexMatch => ColumnFilter(l.label, Filter.EqualsRegex(l.value))
            case NotEqual(false) => ColumnFilter(l.label, Filter.NotEquals(l.value))
            case other: Any => throw new IllegalArgumentException(s"Unknown match operator $other")
          }
        }
        LabelValues(labelNames, columnFilters, timeParams.start * 1000, timeParams.end * 1000)
      case _ =>
        LabelValues(labelNames, Seq.empty, timeParams.start * 1000, timeParams.end * 1000)
    }
  }

  def queryToLogicalPlan(query: String, queryTimestamp: Long, step: Long): LogicalPlan = {
    // Remember step matters here in instant query, when lookback is provided in step factor notation as in [5i]
    val defaultQueryParams = TimeStepParams(queryTimestamp, step, queryTimestamp)
    queryRangeToLogicalPlan(query, defaultQueryParams)
  }

  def queryRangeToLogicalPlan(query: String, timeParams: TimeRangeParams): LogicalPlan = {
    val expression = parseQuery(query)

    assignPrecedence(expression) match {
      case p: PeriodicSeries => p.toSeriesPlan(timeParams)
      case r: SimpleSeries   => r.toSeriesPlan(timeParams, isRoot = true)
      case _ => throw new UnsupportedOperationException()
    }
  }

  def assignPrecedence(expression: Expression): Expression = {
   expression match {
      case f: Function            => f.copy(allParams = f.allParams.map(assignPrecedence(_)))
      case a: AggregateExpression => a.copy(params = a.params.map(assignPrecedence(_)), altFunctionParams = a.
                                     altFunctionParams.map(assignPrecedence(_)))
      case b: BinaryExpression    => assignPrecedence(b.lhs, b.operator, b.vectorMatch, b.rhs)
      case _                      => expression
    }
  }

  /**
    * Recursively assign precedence to BinaryExpression by creating new BinaryExpression with inner expressions
    *  rearranged based on precedence
    */
  def assignPrecedence(lhs: Expression,
                       operator: Operator,
                       vectorMatch: Option[VectorMatch],
                       rhs: Expression): Expression = {
    rhs match {
      case rhsBE: BinaryExpression => val rhsWithPrecedence = assignPrecedence(rhsBE.lhs, rhsBE.operator,
        rhsBE.vectorMatch, rhsBE.rhs) // Assign Precedence to RHS Expression
        rhsWithPrecedence match {
          case rhsWithPrecBE: BinaryExpression => val rhsOp = rhsWithPrecBE.operator.
            getPlanOperator
            val precd = rhsOp.precedence -
              operator.getPlanOperator.precedence
            if ((precd < 0) || (precd == 0 &&
              !rhsOp.isRightAssociative)) {
              // Assign Precedence to LHS Expression
              val lhsWithPrecedence =
                assignPrecedence(lhs, operator,
                  vectorMatch, rhsWithPrecBE.lhs)
              // Create new BinaryExpression as existing precedence is not correct
              // New expression will have "lhs operator rhs.lhs" first as operator.precedence > rhsOp.precedence
              BinaryExpression(lhsWithPrecedence, rhsWithPrecBE.operator,
                rhsWithPrecBE.vectorMatch, rhsWithPrecBE.rhs)
            } else {
              BinaryExpression(lhs, operator, vectorMatch, rhsWithPrecedence)
            }
          case _ => BinaryExpression(lhs, operator,
            vectorMatch, rhsWithPrecedence)
        }
      case _ => BinaryExpression(lhs, operator, vectorMatch, rhs)
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
