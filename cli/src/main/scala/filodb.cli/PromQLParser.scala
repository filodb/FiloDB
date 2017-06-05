package filodb.cli

import org.parboiled2._

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
}