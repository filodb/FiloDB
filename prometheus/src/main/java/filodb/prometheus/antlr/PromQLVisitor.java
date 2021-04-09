// Generated from PromQL.g4 by ANTLR 4.9.1
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PromQLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PromQLVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link PromQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(PromQLParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code binaryOperation}
	 * labeled alternative in {@link PromQLParser#vectorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinaryOperation(PromQLParser.BinaryOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unaryOperation}
	 * labeled alternative in {@link PromQLParser#vectorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnaryOperation(PromQLParser.UnaryOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code vectorOperation}
	 * labeled alternative in {@link PromQLParser#vectorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVectorOperation(PromQLParser.VectorOperationContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#unaryOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnaryOp(PromQLParser.UnaryOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#powOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPowOp(PromQLParser.PowOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#multOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultOp(PromQLParser.MultOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#addOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddOp(PromQLParser.AddOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#compareOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompareOp(PromQLParser.CompareOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#andUnlessOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAndUnlessOp(PromQLParser.AndUnlessOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#orOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrOp(PromQLParser.OrOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVector(PromQLParser.VectorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#parens}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParens(PromQLParser.ParensContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#instantOrRangeSelector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInstantOrRangeSelector(PromQLParser.InstantOrRangeSelectorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#instantSelector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInstantSelector(PromQLParser.InstantSelectorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcher}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelMatcher(PromQLParser.LabelMatcherContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcherOp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelMatcherOp(PromQLParser.LabelMatcherOpContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcherList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelMatcherList(PromQLParser.LabelMatcherListContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction(PromQLParser.FunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#parameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParameter(PromQLParser.ParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#parameterList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParameterList(PromQLParser.ParameterListContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#aggregation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregation(PromQLParser.AggregationContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#by}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBy(PromQLParser.ByContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#without}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWithout(PromQLParser.WithoutContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#grouping}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGrouping(PromQLParser.GroupingContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#on}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOn(PromQLParser.OnContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#ignoring}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIgnoring(PromQLParser.IgnoringContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#groupLeft}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupLeft(PromQLParser.GroupLeftContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#groupRight}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupRight(PromQLParser.GroupRightContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#metricName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMetricName(PromQLParser.MetricNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#metricKeyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMetricKeyword(PromQLParser.MetricKeywordContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelName(PromQLParser.LabelNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelNameList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelNameList(PromQLParser.LabelNameListContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelKeyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLabelKeyword(PromQLParser.LabelKeywordContext ctx);
	/**
	 * Visit a parse tree produced by {@link PromQLParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(PromQLParser.LiteralContext ctx);
}