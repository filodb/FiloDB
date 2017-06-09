package filodb.coordinator.client

import org.parboiled2._
import scala.util.{Try, Success, Failure}

import filodb.coordinator.QueryCommands._
import filodb.core.query.{ColumnFilter, Filter}

object PromQLParser {
  // We don't really want users to be able to scan every metric. Make this obscure.
  val ScanEverythingMetric = ":_really_scan_everything"

  val SecsInMinute = 60
  val SecsInHour   = 3600
  val SecsInDay    = SecsInHour * 24
  val SecsInWeek   = SecsInDay * 7

  val DefaultRange = SecsInMinute * 30
  val Quote = "\""
  val Space = " "
}

sealed trait Expr
final case class PartitionSpec(metricName: String, column: String, filters: Seq[ColumnFilter], range: Int)
    extends Expr

sealed trait PromQuery
final case class VectorExprOnlyQuery(spec: PartitionSpec) extends PromQuery
final case class FunctionQuery(functionName: String, param: Option[String], expr: Expr)
    extends PromQuery with Expr

final case class ArgsAndPartSpec(queryArgs: QueryArgs, partSpec: PartitionSpec) {
  def withMetricFilter(metricCol: String): ArgsAndPartSpec =
    this.copy(partSpec = partSpec.copy(filters =
                partSpec.filters :+ ColumnFilter(metricCol, Filter.Equals(partSpec.metricName))))

  /**
   * Replaces ONE of the arguments of the main function.
   * May throw an exception if the index number is outside of range of arguments.
   */
  def withNewArg(argIndex: Int, newArg: String): ArgsAndPartSpec =
    this.copy(queryArgs = queryArgs.copy(args = queryArgs.args.updated(argIndex, newArg)))
}

/**
 * A Prometheus PromQL-like query syntax parser
 * To be able to parse expressions like {{ sum(http-requests-total#avg{method="GET"}[5m]) }}
 *
 * Differences from PromQL:
 * - FiloDB has flexible schemas, so #columnName needs to be appended to the metrics name
 * - What column does the metric name map to?  TODO: allow filtering on metric name too
 * - For now very strict subset supported
 *   - nested functions can be parsed now
 */
class PromQLParser(val input: ParserInput) extends Parser {
  import Filter._
  import PromQLParser._

  // scalastyle:off method.name
  // scalastyle:off public.methods.have.type
  def AlphaNumSymbols = CharPredicate.AlphaNum ++ CharPredicate("_-:/")
  def NameString = rule { capture(oneOrMore(AlphaNumSymbols)) }
  def QuotedString = rule { Quote ~ capture(oneOrMore(noneOf(Quote))) ~ Quote }

  def TimeRange = rule { '[' ~ Duration ~ ']' }

  def Number = rule { capture(oneOrMore(CharPredicate.Digit)) ~> (_.toInt) }

  def Duration: Rule1[Int] = rule { DurationSec | DurationMin | DurationHour | DurationDay | DurationWeek }
  def DurationSec = rule { Number ~ Space.? ~ "s" }
  def DurationMin = rule { Number ~ Space.? ~ "m" ~> (_ * SecsInMinute) }
  def DurationHour = rule { Number ~ Space.? ~ "h" ~> (_ * SecsInHour) }
  def DurationDay = rule { Number ~ Space.? ~ "d" ~> (_ * SecsInDay) }
  def DurationWeek = rule { Number ~ Space.? ~ "w" ~> (_ * SecsInWeek) }

  def TagSelector = rule { '{' ~ oneOrMore(TagFilter).separatedBy(ch(',')) ~ '}' }

  def EqualsFilter =
    rule { Space.? ~ NameString ~ "=" ~ QuotedString ~> ((tag, value) => ColumnFilter(tag, Equals(value))) }

  def TagFilter: Rule1[ColumnFilter] = rule { EqualsFilter }

  def VectorSelector =
    rule { NameString ~ "#" ~ NameString ~ TagSelector.? ~ TimeRange.? ~>
           ((metric: String, column: String, filters: Option[Seq[ColumnFilter]], duration: Option[Int]) =>
            PartitionSpec(metric, column, filters.getOrElse(Nil), duration.getOrElse(DefaultRange))) }

  def FunctionParam = rule { NameString ~ ',' ~ Space.? }
  def FunctionExpr = rule { NameString ~ '(' ~ FunctionParam.? ~ FunctionOrSelector ~ ')' ~> FunctionQuery }

  def FunctionOrSelector: Rule1[Expr] = rule { VectorSelector | FunctionExpr }

  def VectorExpr = rule { VectorSelector ~> VectorExprOnlyQuery }
  def Query: Rule1[PromQuery] = rule { (FunctionExpr | VectorExpr) ~ EOI }

  /**
   * Method to parse a PromQL query and return important parameters for the FilODB client query call
   */
  def parseAndGetArgs(): Try[ArgsAndPartSpec] = {
    Query.run().flatMap {
      // Single level of function
      case FunctionQuery(funcName, paramOpt, partSpec: PartitionSpec) =>
        evalArgs(funcName, paramOpt, partSpec).map { args =>
          val spec = QueryArgs(funcName, args)
          Success(ArgsAndPartSpec(spec, partSpec))
        }.getOrElse {
          Failure(new IllegalArgumentException(s"Unable to parse function $funcName with option $paramOpt"))
        }

      // Two levels of functions.  Assume outer one is combiner.
      // For now, only support a single combiner parameter.
      case FunctionQuery(combName, combParam,
             FunctionQuery(funcName, paramOpt, partSpec: PartitionSpec)) =>
        evalArgs(funcName, paramOpt, partSpec).map { args =>
          val spec = QueryArgs(funcName, args, combName, combParam.toSeq)
          Success(ArgsAndPartSpec(spec, partSpec))
        }.getOrElse {
          Failure(new IllegalArgumentException(s"Unable to parse function $funcName with option $paramOpt"))
        }

      case VectorExprOnlyQuery(partSpec) =>
        // For now just return the last data point and list all series
        val spec = QueryArgs("last", Seq("timestamp", partSpec.column))
        Success(ArgsAndPartSpec(spec, partSpec))

      case other: PromQuery =>
        Failure(new IllegalArgumentException(s"Sorry, query with AST $other cannot be supported right now."))
    }
  }

  private def evalArgs(func: String, paramOpt: Option[String], spec: PartitionSpec): Option[Seq[String]] = {
    // For now, only parse the time aggregate functions
    if (func.startsWith("time_group")) {
      // arguments:  <timeColumn> <valueColumn> <startTs> <endTs> <numBuckets>
      val numBuckets = paramOpt.map(_.toInt).getOrElse(50)
      val endTs = System.currentTimeMillis
      val startTs = endTs - (spec.range * 1000)
      Some(Seq("timestamp", spec.column, startTs.toString, endTs.toString, numBuckets.toString))
    } else {
      // Assume a single-column function
      Some(Seq(spec.column))
    }
  }
}