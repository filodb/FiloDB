// scalastyle:off
package filodb.prometheus.parse

import scala.collection.JavaConverters._

import com.typesafe.scalalogging.StrictLogging
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}

import filodb.prometheus.antlr.{PromQLBaseVisitor, PromQLLexer, PromQLParser}
import filodb.prometheus.ast._

/**
  * Bridges the gap between the auto-generated Antlr classes the FiloDB AST classes.
  * Auto-generated classes shouldn't leak past here. When the grammar changes, the
  * auto-generated classes need to be rebuilt, and then additional changes are required here.
  */
object AntlrParser extends StrictLogging {
  /**
    * Main entry point.
    */
  def parseQuery(query: String): Expression = {
    logger.debug(s"AntlrParser query: $query")
    parseQuery(query, p => p.expression())
  }

  def parseLabelValueFilter(query: String): Seq[LabelMatch] = {
    parseQuery(query, p => p.labelMatcherList())
  }

  /**
    * @param entry callback which invokes the parser entry point
    */
  def parseQuery[T](query: String, entry: PromQLParser => ParseTree): T = {
    val errors = new StringBuilder()

    val listener = new BaseErrorListener() {
	  override def syntaxError(recognizer: Recognizer[_,_],
                               offendingSymbol: Object,
                               line: Int,
                               charPositionInLine: Int,
                               msg: String,
                               e: RecognitionException): scala.Unit =
      {
        if (errors.length() != 0) {
          errors.append(", and ")
        }
        errors.append("at " + line + ":" + charPositionInLine + " " + msg)
      }
    }

    val lexer = new PromQLLexer(CharStreams.fromString(query))
    lexer.removeErrorListeners()
    lexer.addErrorListener(listener)

    val parser = new PromQLParser(new CommonTokenStream(lexer))
    parser.removeErrorListeners()
    parser.setErrorHandler(new BailErrorStrategy())
    parser.addErrorListener(listener)

    try {
      val expr = cast[T](new AntlrParser().visit(entry(parser)))
      if (expr != null && errors.length() == 0) {
        return expr
      }
    } catch {
      case e: ParseCancellationException => {}
      case exc: Throwable => exc.printStackTrace
    }

    var msg = "Cannot parse [" + query + "]"
    if (errors.length() != 0) {
      msg = msg + " because " + errors.toString
    }
    throw new IllegalArgumentException(msg)
  }

  // TODO: Need a better way of capturing cast failures and converting to IllegalArgumentException.
  def cast[T](obj: Object): T = {
    return obj.asInstanceOf[T]
  }
}

/**
  * The real work happens here, by extending the auto-generated visitor.
  */
class AntlrParser extends PromQLBaseVisitor[Object] {
  override def visitSubqueryOperation(ctx: PromQLParser.SubqueryOperationContext): SubqueryExpression = {
    val lhs = build[Expression](ctx.vectorExpression())
    if (!lhs.isInstanceOf[PeriodicSeries] || lhs.isInstanceOf[SubqueryExpression]) {
      throw new IllegalArgumentException("Subquery can only be applied to instant queries")
    }
    val sqcl = build[SubqueryClause](ctx.subquery())
    val offset: Option[Duration] = if (ctx.offset == null) {
      None
    } else {
      Some(build[Duration](ctx.offset))
    }
    val limit: Option[Scalar] = None
    SubqueryExpression(lhs.asInstanceOf[PeriodicSeries], sqcl, offset, limit.map(_.toScalar.toInt))
  }

  override def visitSubquery(ctx: PromQLParser.SubqueryContext): SubqueryClause = {
    val list = ctx.DURATION
    val window = parseDuration(list.get(0))
    val step = if (list.size() > 1) Some(parseDuration(list.get(1))) else None
    SubqueryClause(window, step)
  }

  override def visitBinaryOperation(ctx: PromQLParser.BinaryOperationContext): BinaryExpression = {
    val lhs = build[Expression](ctx.getChild(0))
    val op = build[Operator](ctx.getChild(1))
    val grouping = ctx.grouping()
    val rhs = build[Expression](ctx.getChild(if (grouping == null) 2 else 3))

    val vectorMatch = if (grouping == null) {
      VectorMatch(None, None)
    } else {
      build[VectorMatch](grouping)
    }

    BinaryExpression(lhs, op, Some(vectorMatch), rhs)
  }

  override def visitUnaryOperation(ctx: PromQLParser.UnaryOperationContext): UnaryExpression = {
    val op = build[Operator](ctx.getChild(0))
    val expr = build[Expression](ctx.getChild(1))
    UnaryExpression(op, expr)
  }

  override def visitUnaryOp(ctx: PromQLParser.UnaryOpContext): ArithmeticOp = {
    ctx.getStart().getType() match {
      case PromQLParser.ADD => Add
      case PromQLParser.SUB => Sub
    }
  }

  override def visitPowOp(ctx: PromQLParser.PowOpContext): ArithmeticOp = {
    Pow
  }

  override def visitMultOp(ctx: PromQLParser.MultOpContext): ArithmeticOp = {
    ctx.getStart().getType() match {
      case PromQLParser.MUL => Mul
      case PromQLParser.DIV => Div
      case PromQLParser.MOD => Mod
    }
  }

  override def visitAddOp(ctx: PromQLParser.AddOpContext): ArithmeticOp = {
    ctx.getStart().getType() match {
      case PromQLParser.ADD => Add
      case PromQLParser.SUB => Sub
    }
  }

  override def visitCompareOp(ctx: PromQLParser.CompareOpContext): Comparision = {
    val bool = ctx.BOOL != null
    ctx.getStart().getType() match {
      case PromQLParser.DEQ => Eq(bool)
      case PromQLParser.NE  => NotEqual(bool)
      case PromQLParser.GT  => Gt(bool)
      case PromQLParser.LT  => Lt(bool)
      case PromQLParser.GE  => Gte(bool)
      case PromQLParser.LE  => Lte(bool)
    }
  }

  override def visitAndUnlessOp(ctx: PromQLParser.AndUnlessOpContext): SetOp = {
    ctx.getStart().getType() match {
      case PromQLParser.AND => And
      case PromQLParser.UNLESS => Unless
    }
  }

  override def visitOrOp(ctx: PromQLParser.OrOpContext): SetOp = {
    ctx.getStart().getType() match {
      case PromQLParser.OR => Or
    }
  }

  override def visitInstantOrRangeSelector(ctx: PromQLParser.InstantOrRangeSelectorContext): Vector = {
    val instantSelector = ctx.instantSelector()
    val mn = instantSelector.metricName()
    val metricName: Option[String] = if (mn == null) None else Some(mn.getText())

    val matcherList = instantSelector.labelMatcherList()
    val labelSelection: Seq[LabelMatch] = if (matcherList == null) {
      Seq.empty
    } else {
      build[Seq[LabelMatch]](matcherList)
    }

    val offset: Option[Duration] = if (ctx.offset == null) {
      None
    } else {
      Some(build[Duration](ctx.offset))
    }

    val limit: Option[Scalar] = None

    if (ctx.window == null) {
      InstantExpression(metricName, labelSelection, offset)
    } else {
      val window = build[Duration](ctx.window)
      RangeExpression(metricName, labelSelection, window, offset)
    }
  }

  override def visitWindow(ctx: PromQLParser.WindowContext): Duration = {
    parseDuration(ctx.DURATION)
  }

  override def visitOffset(ctx: PromQLParser.OffsetContext): Duration = {
    parseDuration(ctx.DURATION)
  }

  override def visitLimitOperation(ctx: PromQLParser.LimitOperationContext) = {
    val name = ctx.limit().LIMIT().getSymbol().getText()
    val limit = Scalar(java.lang.Double.parseDouble(ctx.limit().NUMBER().getSymbol().getText()))
    val params = Seq(limit) :+ build[Expression](ctx.vectorExpression())
    Function(name, params)
  }

  override def visitLabelMatcher(ctx: PromQLParser.LabelMatcherContext): LabelMatch = {
    val label = ctx.labelName().getText()
    val op = build[Operator](ctx.labelMatcherOp())
    val value = dequote(ctx.STRING())
    LabelMatch(label, op, value)
  }

  override def visitLabelMatcherOp(ctx: PromQLParser.LabelMatcherOpContext): Operator = {
    ctx.getStart().getType() match {
      case PromQLParser.EQ  => EqualMatch
      case PromQLParser.NE  => NotEqual(false)
      case PromQLParser.RE  => RegexMatch
      case PromQLParser.NRE => NotRegexMatch
    }
  }

  override def visitLabelMatcherList(ctx: PromQLParser.LabelMatcherListContext): Seq[LabelMatch] = {
    val list = ctx.labelMatcher()
    if (list.isEmpty()) {
      Seq.empty
    } else {
      list.asScala.map { matcher => build[LabelMatch](matcher) }.toList
    }
  }

  override def visitFunction(ctx: PromQLParser.FunctionContext): Function = {
    val name = ctx.IDENTIFIER.getSymbol().getText()
    val params = build[Seq[Expression]](ctx.parameterList())
    Function(name, params)
  }

  override def visitParameterList(ctx: PromQLParser.ParameterListContext): Seq[Expression] = {
    val params = ctx.parameter()
    if (params.isEmpty()) {
      Seq.empty
    } else {
      params.asScala.map { param => build[Expression](param) }.toList
    }
  }

  override def visitAggregation(ctx: PromQLParser.AggregationContext): Expression = {
    val name = ctx.AGGREGATION_OP.getSymbol().getText()
    val params = build[Seq[Expression]](ctx.parameterList())

    val by = ctx.by()
    val without = ctx.without()

    val grouping: Option[AggregateGrouping] = if (by != null) {
      Some(build[By](by))
    } else if (without != null) {
      Some(build[Without](without))
    } else {
      None
    }

    AggregateExpression(name, params, grouping, Seq.empty)
  }

  override def visitBy(ctx: PromQLParser.ByContext): By = {
    By(build[Seq[String]](ctx.labelNameList()))
  }

  override def visitWithout(ctx: PromQLParser.WithoutContext): Without = {
    Without(build[Seq[String]](ctx.labelNameList()))
  }

  override def visitGrouping(ctx: PromQLParser.GroupingContext): VectorMatch = {
    val on = ctx.on()
    val ignoring = ctx.ignoring()

    val matching: Option[JoinMatching] = if (on != null) {
      Some(build[On](on))
    } else if (ignoring != null) {
      Some(build[Ignoring](ignoring))
    } else {
      None
    }

    val left = ctx.groupLeft()
    val right = ctx.groupRight()

    val grouping: Option[JoinGrouping] = if (left != null) {
      Some(build[GroupLeft](left))
    } else if (right != null) {
      Some(build[GroupRight](right))
    } else {
      None
    }

    VectorMatch(matching, grouping)
  }

  override def visitOn(ctx: PromQLParser.OnContext): On = {
    On(build[Seq[String]](ctx.labelNameList()))
  }

  override def visitIgnoring(ctx: PromQLParser.IgnoringContext): Ignoring = {
    Ignoring(build[Seq[String]](ctx.labelNameList()))
  }

  override def visitGroupLeft(ctx: PromQLParser.GroupLeftContext): GroupLeft = {
    val list = ctx.labelNameList()
    GroupLeft(if (list == null) Seq.empty else build[Seq[String]](list))
  }

  override def visitGroupRight(ctx: PromQLParser.GroupRightContext): GroupRight = {
    val list = ctx.labelNameList()
    GroupRight(if (list == null) Seq.empty else build[Seq[String]](list))
  }

  override def visitLabelNameList(ctx: PromQLParser.LabelNameListContext): Seq[String] = {
    ctx.labelName().asScala.map { name => name.getText() }.toList
  }

  override def visitLiteral(ctx: PromQLParser.LiteralContext): Expression = {
    val num = ctx.NUMBER()
    if (num != null) {
      Scalar(java.lang.Double.parseDouble(num.getSymbol().getText()))
    } else {
      StringLiteral(dequote(ctx.STRING()))
    }
  }

  /**
    * This method is called by the inherited visitChildren method. The default implementation
    * simply returns nextResult, tossing away any left child nodes. Instead, throw an
    * IllegalStateException, and the caller should build the children manually.
    * 
    * This default method is appropriate for nodes that can only have one child, or if among
    * the possible child node types that it can have, only one type will exist. Think of
    * nodes that have a bunch of 'or' rules, defined using the '|' character.
    */
  override def aggregateResult(aggregate: Object, nextResult: Object): Object = {
    if (nextResult == null) {
      aggregate
    } else if (aggregate == null) {
      nextResult
    } else {
      // This is a bug in this class, not the query itself.
      throw new IllegalStateException(
        "Cannot aggregate child nodes: " + aggregate + ", " + nextResult)
    }
  }

  /**
    * Visits a child node (recursively) and casts the result to the expected type. A "clean"
    * exception is thrown if the type doesn't match.
    */
  private def build[T](ctx: ParseTree): T = AntlrParser.cast[T](visit(ctx))

  /**
    * Strip quotes and process escape codes a string terminal node.
    */
  private def dequote(str: TerminalNode): String = ParserUtil.dequote(str.getSymbol().getText())

  /**
   * Given a string and an index to begin parsing, returns the first Duration parsed.
   * Additionally returns the index to begin parsing for the next Duration.
   */
  private def parseSingleDurationFromString(str: String, istart: Int): (Duration, Int)  = {
    var istop = istart + 1
    while (istop < str.length && !str(istop).isLetter) {
      istop += 1
    }
    val value = java.lang.Double.parseDouble(str.substring(istart, istop))
    val unit = str(istop) match {
      case 's' => Second
      case 'm' => Minute
      case 'h' => Hour
      case 'd' => Day
      case 'w' => Week
      case 'y' => Year
      case 'i' => IntervalMultiple
    }
    val nextIstart = istop + 1
    (Duration(value, unit), nextIstart)
  }

  /**
   * Returns the total Duration of all duration time/unit pairs in the string.
   * Example:
   *     "1m30s" -> Duration(90.0, Second)
   * Cannot be used with IntervalMultiple "i" notation.
   */
  private def parseDurationNoInterval(str: String): Duration = {
    var i = 0
    var totalMillis = 0d
    while (i < str.length) {
      val (dur, inext) = parseSingleDurationFromString(str, i)
      totalMillis += dur.millis(0)
      i = inext
    }
    // Feels wrong to add a Milliseconds Duration type, so just using seconds.
    Duration(totalMillis / 1000, Second)
  }

  private def parseDuration(node: TerminalNode): Duration = {
    val str = node.getSymbol.getText
    if (str.last == 'i') {
      val (dur, _) = parseSingleDurationFromString(str, 0)
      dur
    } else {
      parseDurationNoInterval(str)
    }
  }
}
