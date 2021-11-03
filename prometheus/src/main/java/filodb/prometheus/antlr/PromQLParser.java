// Generated from PromQL.g4 by ANTLR 4.9
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PromQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, TIMESTAMP=9, 
		NUMBER=10, INTEGER=11, STRING=12, ADD=13, SUB=14, MUL=15, DIV=16, MOD=17, 
		POW=18, EQ=19, DEQ=20, NE=21, GT=22, LT=23, GE=24, LE=25, RE=26, NRE=27, 
		AT=28, AND=29, OR=30, UNLESS=31, BY=32, WITHOUT=33, ON=34, IGNORING=35, 
		GROUP_LEFT=36, GROUP_RIGHT=37, OFFSET=38, LIMIT=39, BOOL=40, AGGREGATION_OP=41, 
		DURATION=42, IDENTIFIER=43, IDENTIFIER_EXTENDED=44, WS=45, COMMENT=46;
	public static final int
		RULE_expression = 0, RULE_vectorExpression = 1, RULE_unaryOp = 2, RULE_powOp = 3, 
		RULE_multOp = 4, RULE_addOp = 5, RULE_compareOp = 6, RULE_andUnlessOp = 7, 
		RULE_orOp = 8, RULE_vector = 9, RULE_parens = 10, RULE_instantOrRangeSelector = 11, 
		RULE_instantSelector = 12, RULE_window = 13, RULE_offset = 14, RULE_at = 15, 
		RULE_limit = 16, RULE_subquery = 17, RULE_labelMatcher = 18, RULE_labelMatcherOp = 19, 
		RULE_labelMatcherList = 20, RULE_function = 21, RULE_parameter = 22, RULE_parameterList = 23, 
		RULE_aggregation = 24, RULE_by = 25, RULE_without = 26, RULE_grouping = 27, 
		RULE_on = 28, RULE_ignoring = 29, RULE_groupLeft = 30, RULE_groupRight = 31, 
		RULE_metricName = 32, RULE_metricKeyword = 33, RULE_labelName = 34, RULE_labelNameList = 35, 
		RULE_labelKeyword = 36, RULE_literal = 37;
	private static String[] makeRuleNames() {
		return new String[] {
			"expression", "vectorExpression", "unaryOp", "powOp", "multOp", "addOp", 
			"compareOp", "andUnlessOp", "orOp", "vector", "parens", "instantOrRangeSelector", 
			"instantSelector", "window", "offset", "at", "limit", "subquery", "labelMatcher", 
			"labelMatcherOp", "labelMatcherList", "function", "parameter", "parameterList", 
			"aggregation", "by", "without", "grouping", "on", "ignoring", "groupLeft", 
			"groupRight", "metricName", "metricKeyword", "labelName", "labelNameList", 
			"labelKeyword", "literal"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'{'", "'}'", "'['", "']'", "':'", "','", null, null, 
			null, null, "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'='", "'=='", 
			"'!='", "'>'", "'<'", "'>='", "'<='", "'=~'", "'!~'", "'@'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, "TIMESTAMP", "NUMBER", 
			"INTEGER", "STRING", "ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", 
			"DEQ", "NE", "GT", "LT", "GE", "LE", "RE", "NRE", "AT", "AND", "OR", 
			"UNLESS", "BY", "WITHOUT", "ON", "IGNORING", "GROUP_LEFT", "GROUP_RIGHT", 
			"OFFSET", "LIMIT", "BOOL", "AGGREGATION_OP", "DURATION", "IDENTIFIER", 
			"IDENTIFIER_EXTENDED", "WS", "COMMENT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PromQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PromQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class ExpressionContext extends ParserRuleContext {
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(PromQLParser.EOF, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(76);
			vectorExpression(0);
			setState(77);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VectorExpressionContext extends ParserRuleContext {
		public VectorExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_vectorExpression; }
	 
		public VectorExpressionContext() { }
		public void copyFrom(VectorExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BinaryOperationContext extends VectorExpressionContext {
		public List<VectorExpressionContext> vectorExpression() {
			return getRuleContexts(VectorExpressionContext.class);
		}
		public VectorExpressionContext vectorExpression(int i) {
			return getRuleContext(VectorExpressionContext.class,i);
		}
		public PowOpContext powOp() {
			return getRuleContext(PowOpContext.class,0);
		}
		public GroupingContext grouping() {
			return getRuleContext(GroupingContext.class,0);
		}
		public MultOpContext multOp() {
			return getRuleContext(MultOpContext.class,0);
		}
		public AddOpContext addOp() {
			return getRuleContext(AddOpContext.class,0);
		}
		public CompareOpContext compareOp() {
			return getRuleContext(CompareOpContext.class,0);
		}
		public AndUnlessOpContext andUnlessOp() {
			return getRuleContext(AndUnlessOpContext.class,0);
		}
		public OrOpContext orOp() {
			return getRuleContext(OrOpContext.class,0);
		}
		public BinaryOperationContext(VectorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitBinaryOperation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnaryOperationContext extends VectorExpressionContext {
		public UnaryOpContext unaryOp() {
			return getRuleContext(UnaryOpContext.class,0);
		}
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public UnaryOperationContext(VectorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitUnaryOperation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class VectorOperationContext extends VectorExpressionContext {
		public VectorContext vector() {
			return getRuleContext(VectorContext.class,0);
		}
		public VectorOperationContext(VectorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitVectorOperation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryOperationContext extends VectorExpressionContext {
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public SubqueryContext subquery() {
			return getRuleContext(SubqueryContext.class,0);
		}
		public OffsetContext offset() {
			return getRuleContext(OffsetContext.class,0);
		}
		public SubqueryOperationContext(VectorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitSubqueryOperation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LimitOperationContext extends VectorExpressionContext {
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public LimitContext limit() {
			return getRuleContext(LimitContext.class,0);
		}
		public LimitOperationContext(VectorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLimitOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VectorExpressionContext vectorExpression() throws RecognitionException {
		return vectorExpression(0);
	}

	private VectorExpressionContext vectorExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		VectorExpressionContext _localctx = new VectorExpressionContext(_ctx, _parentState);
		VectorExpressionContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_vectorExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(84);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
			case SUB:
				{
				_localctx = new UnaryOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(80);
				unaryOp();
				setState(81);
				vectorExpression(9);
				}
				break;
			case T__0:
			case T__2:
			case NUMBER:
			case STRING:
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case LIMIT:
			case AGGREGATION_OP:
			case IDENTIFIER:
			case IDENTIFIER_EXTENDED:
				{
				_localctx = new VectorOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(83);
				vector();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(137);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(135);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(86);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(87);
						powOp();
						setState(89);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(88);
							grouping();
							}
						}

						setState(91);
						vectorExpression(10);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(93);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(94);
						multOp();
						setState(96);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(95);
							grouping();
							}
						}

						setState(98);
						vectorExpression(9);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(100);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(101);
						addOp();
						setState(103);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(102);
							grouping();
							}
						}

						setState(105);
						vectorExpression(8);
						}
						break;
					case 4:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(107);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(108);
						compareOp();
						setState(110);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(109);
							grouping();
							}
						}

						setState(112);
						vectorExpression(7);
						}
						break;
					case 5:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(114);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(115);
						andUnlessOp();
						setState(117);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(116);
							grouping();
							}
						}

						setState(119);
						vectorExpression(6);
						}
						break;
					case 6:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(121);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(122);
						orOp();
						setState(124);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(123);
							grouping();
							}
						}

						setState(126);
						vectorExpression(5);
						}
						break;
					case 7:
						{
						_localctx = new SubqueryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(128);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(129);
						subquery();
						setState(131);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
						case 1:
							{
							setState(130);
							offset();
							}
							break;
						}
						}
						break;
					case 8:
						{
						_localctx = new LimitOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(133);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(134);
						limit();
						}
						break;
					}
					} 
				}
				setState(139);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class UnaryOpContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(PromQLParser.ADD, 0); }
		public TerminalNode SUB() { return getToken(PromQLParser.SUB, 0); }
		public UnaryOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unaryOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitUnaryOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnaryOpContext unaryOp() throws RecognitionException {
		UnaryOpContext _localctx = new UnaryOpContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_unaryOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(140);
			_la = _input.LA(1);
			if ( !(_la==ADD || _la==SUB) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PowOpContext extends ParserRuleContext {
		public TerminalNode POW() { return getToken(PromQLParser.POW, 0); }
		public PowOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_powOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitPowOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PowOpContext powOp() throws RecognitionException {
		PowOpContext _localctx = new PowOpContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_powOp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(142);
			match(POW);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultOpContext extends ParserRuleContext {
		public TerminalNode MUL() { return getToken(PromQLParser.MUL, 0); }
		public TerminalNode DIV() { return getToken(PromQLParser.DIV, 0); }
		public TerminalNode MOD() { return getToken(PromQLParser.MOD, 0); }
		public MultOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitMultOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultOpContext multOp() throws RecognitionException {
		MultOpContext _localctx = new MultOpContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_multOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(144);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << MOD))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AddOpContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(PromQLParser.ADD, 0); }
		public TerminalNode SUB() { return getToken(PromQLParser.SUB, 0); }
		public AddOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_addOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitAddOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AddOpContext addOp() throws RecognitionException {
		AddOpContext _localctx = new AddOpContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_addOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(146);
			_la = _input.LA(1);
			if ( !(_la==ADD || _la==SUB) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CompareOpContext extends ParserRuleContext {
		public TerminalNode DEQ() { return getToken(PromQLParser.DEQ, 0); }
		public TerminalNode NE() { return getToken(PromQLParser.NE, 0); }
		public TerminalNode GT() { return getToken(PromQLParser.GT, 0); }
		public TerminalNode LT() { return getToken(PromQLParser.LT, 0); }
		public TerminalNode GE() { return getToken(PromQLParser.GE, 0); }
		public TerminalNode LE() { return getToken(PromQLParser.LE, 0); }
		public TerminalNode BOOL() { return getToken(PromQLParser.BOOL, 0); }
		public CompareOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compareOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitCompareOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CompareOpContext compareOp() throws RecognitionException {
		CompareOpContext _localctx = new CompareOpContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_compareOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(148);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DEQ) | (1L << NE) | (1L << GT) | (1L << LT) | (1L << GE) | (1L << LE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(150);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BOOL) {
				{
				setState(149);
				match(BOOL);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AndUnlessOpContext extends ParserRuleContext {
		public TerminalNode AND() { return getToken(PromQLParser.AND, 0); }
		public TerminalNode UNLESS() { return getToken(PromQLParser.UNLESS, 0); }
		public AndUnlessOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_andUnlessOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitAndUnlessOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AndUnlessOpContext andUnlessOp() throws RecognitionException {
		AndUnlessOpContext _localctx = new AndUnlessOpContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_andUnlessOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==UNLESS) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrOpContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(PromQLParser.OR, 0); }
		public OrOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitOrOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrOpContext orOp() throws RecognitionException {
		OrOpContext _localctx = new OrOpContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_orOp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(154);
			match(OR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VectorContext extends ParserRuleContext {
		public FunctionContext function() {
			return getRuleContext(FunctionContext.class,0);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public InstantOrRangeSelectorContext instantOrRangeSelector() {
			return getRuleContext(InstantOrRangeSelectorContext.class,0);
		}
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public ParensContext parens() {
			return getRuleContext(ParensContext.class,0);
		}
		public VectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_vector; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitVector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VectorContext vector() throws RecognitionException {
		VectorContext _localctx = new VectorContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_vector);
		try {
			setState(161);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(156);
				function();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(157);
				aggregation();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(158);
				instantOrRangeSelector();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(159);
				literal();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(160);
				parens();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParensContext extends ParserRuleContext {
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public ParensContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parens; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitParens(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParensContext parens() throws RecognitionException {
		ParensContext _localctx = new ParensContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_parens);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(163);
			match(T__0);
			setState(164);
			vectorExpression(0);
			setState(165);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InstantOrRangeSelectorContext extends ParserRuleContext {
		public InstantSelectorContext instantSelector() {
			return getRuleContext(InstantSelectorContext.class,0);
		}
		public WindowContext window() {
			return getRuleContext(WindowContext.class,0);
		}
		public OffsetContext offset() {
			return getRuleContext(OffsetContext.class,0);
		}
		public AtContext at() {
			return getRuleContext(AtContext.class,0);
		}
		public InstantOrRangeSelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_instantOrRangeSelector; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitInstantOrRangeSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InstantOrRangeSelectorContext instantOrRangeSelector() throws RecognitionException {
		InstantOrRangeSelectorContext _localctx = new InstantOrRangeSelectorContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_instantOrRangeSelector);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(167);
			instantSelector();
			setState(169);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(168);
				window();
				}
				break;
			}
			setState(179);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(171);
				offset();
				}
				break;
			case 2:
				{
				setState(172);
				at();
				}
				break;
			case 3:
				{
				setState(173);
				offset();
				setState(174);
				at();
				}
				break;
			case 4:
				{
				setState(176);
				at();
				setState(177);
				offset();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InstantSelectorContext extends ParserRuleContext {
		public MetricNameContext metricName() {
			return getRuleContext(MetricNameContext.class,0);
		}
		public LabelMatcherListContext labelMatcherList() {
			return getRuleContext(LabelMatcherListContext.class,0);
		}
		public InstantSelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_instantSelector; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitInstantSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InstantSelectorContext instantSelector() throws RecognitionException {
		InstantSelectorContext _localctx = new InstantSelectorContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_instantSelector);
		int _la;
		try {
			setState(193);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case LIMIT:
			case AGGREGATION_OP:
			case IDENTIFIER:
			case IDENTIFIER_EXTENDED:
				enterOuterAlt(_localctx, 1);
				{
				setState(181);
				metricName();
				setState(187);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(182);
					match(T__2);
					setState(184);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << LIMIT) | (1L << BOOL) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER))) != 0)) {
						{
						setState(183);
						labelMatcherList();
						}
					}

					setState(186);
					match(T__3);
					}
					break;
				}
				}
				break;
			case T__2:
				enterOuterAlt(_localctx, 2);
				{
				setState(189);
				match(T__2);
				setState(190);
				labelMatcherList();
				setState(191);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowContext extends ParserRuleContext {
		public TerminalNode DURATION() { return getToken(PromQLParser.DURATION, 0); }
		public WindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_window; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowContext window() throws RecognitionException {
		WindowContext _localctx = new WindowContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_window);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(195);
			match(T__4);
			setState(196);
			match(DURATION);
			setState(197);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OffsetContext extends ParserRuleContext {
		public TerminalNode OFFSET() { return getToken(PromQLParser.OFFSET, 0); }
		public TerminalNode DURATION() { return getToken(PromQLParser.DURATION, 0); }
		public OffsetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offset; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitOffset(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OffsetContext offset() throws RecognitionException {
		OffsetContext _localctx = new OffsetContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_offset);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(199);
			match(OFFSET);
			setState(200);
			match(DURATION);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AtContext extends ParserRuleContext {
		public TerminalNode AT() { return getToken(PromQLParser.AT, 0); }
		public TerminalNode TIMESTAMP() { return getToken(PromQLParser.TIMESTAMP, 0); }
		public AtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_at; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitAt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AtContext at() throws RecognitionException {
		AtContext _localctx = new AtContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_at);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(202);
			match(AT);
			setState(203);
			match(TIMESTAMP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LimitContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(PromQLParser.LIMIT, 0); }
		public TerminalNode NUMBER() { return getToken(PromQLParser.NUMBER, 0); }
		public LimitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLimit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitContext limit() throws RecognitionException {
		LimitContext _localctx = new LimitContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_limit);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(205);
			match(LIMIT);
			setState(206);
			match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubqueryContext extends ParserRuleContext {
		public List<TerminalNode> DURATION() { return getTokens(PromQLParser.DURATION); }
		public TerminalNode DURATION(int i) {
			return getToken(PromQLParser.DURATION, i);
		}
		public SubqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subquery; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubqueryContext subquery() throws RecognitionException {
		SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_subquery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(208);
			match(T__4);
			setState(209);
			match(DURATION);
			setState(210);
			match(T__6);
			setState(212);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DURATION) {
				{
				setState(211);
				match(DURATION);
				}
			}

			setState(214);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelMatcherContext extends ParserRuleContext {
		public LabelNameContext labelName() {
			return getRuleContext(LabelNameContext.class,0);
		}
		public LabelMatcherOpContext labelMatcherOp() {
			return getRuleContext(LabelMatcherOpContext.class,0);
		}
		public TerminalNode STRING() { return getToken(PromQLParser.STRING, 0); }
		public LabelMatcherContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelMatcher; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelMatcher(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelMatcherContext labelMatcher() throws RecognitionException {
		LabelMatcherContext _localctx = new LabelMatcherContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_labelMatcher);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(216);
			labelName();
			setState(217);
			labelMatcherOp();
			setState(218);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelMatcherOpContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(PromQLParser.EQ, 0); }
		public TerminalNode NE() { return getToken(PromQLParser.NE, 0); }
		public TerminalNode RE() { return getToken(PromQLParser.RE, 0); }
		public TerminalNode NRE() { return getToken(PromQLParser.NRE, 0); }
		public LabelMatcherOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelMatcherOp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelMatcherOp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelMatcherOpContext labelMatcherOp() throws RecognitionException {
		LabelMatcherOpContext _localctx = new LabelMatcherOpContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_labelMatcherOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(220);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << NE) | (1L << RE) | (1L << NRE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelMatcherListContext extends ParserRuleContext {
		public List<LabelMatcherContext> labelMatcher() {
			return getRuleContexts(LabelMatcherContext.class);
		}
		public LabelMatcherContext labelMatcher(int i) {
			return getRuleContext(LabelMatcherContext.class,i);
		}
		public LabelMatcherListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelMatcherList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelMatcherList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelMatcherListContext labelMatcherList() throws RecognitionException {
		LabelMatcherListContext _localctx = new LabelMatcherListContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_labelMatcherList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			labelMatcher();
			setState(227);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(223);
				match(T__7);
				setState(224);
				labelMatcher();
				}
				}
				setState(229);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(PromQLParser.IDENTIFIER, 0); }
		public ParameterListContext parameterList() {
			return getRuleContext(ParameterListContext.class,0);
		}
		public FunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionContext function() throws RecognitionException {
		FunctionContext _localctx = new FunctionContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_function);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
			match(IDENTIFIER);
			setState(231);
			parameterList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParameterContext extends ParserRuleContext {
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public VectorExpressionContext vectorExpression() {
			return getRuleContext(VectorExpressionContext.class,0);
		}
		public ParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameter; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParameterContext parameter() throws RecognitionException {
		ParameterContext _localctx = new ParameterContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_parameter);
		try {
			setState(235);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(233);
				literal();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(234);
				vectorExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParameterListContext extends ParserRuleContext {
		public List<ParameterContext> parameter() {
			return getRuleContexts(ParameterContext.class);
		}
		public ParameterContext parameter(int i) {
			return getRuleContext(ParameterContext.class,i);
		}
		public ParameterListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameterList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitParameterList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParameterListContext parameterList() throws RecognitionException {
		ParameterListContext _localctx = new ParameterListContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_parameterList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(237);
			match(T__0);
			setState(246);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << NUMBER) | (1L << STRING) | (1L << ADD) | (1L << SUB) | (1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << OFFSET) | (1L << LIMIT) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER) | (1L << IDENTIFIER_EXTENDED))) != 0)) {
				{
				setState(238);
				parameter();
				setState(243);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(239);
					match(T__7);
					setState(240);
					parameter();
					}
					}
					setState(245);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(248);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationContext extends ParserRuleContext {
		public TerminalNode AGGREGATION_OP() { return getToken(PromQLParser.AGGREGATION_OP, 0); }
		public ParameterListContext parameterList() {
			return getRuleContext(ParameterListContext.class,0);
		}
		public ByContext by() {
			return getRuleContext(ByContext.class,0);
		}
		public WithoutContext without() {
			return getRuleContext(WithoutContext.class,0);
		}
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitAggregation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_aggregation);
		try {
			setState(265);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(250);
				match(AGGREGATION_OP);
				setState(251);
				parameterList();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(252);
				match(AGGREGATION_OP);
				setState(255);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BY:
					{
					setState(253);
					by();
					}
					break;
				case WITHOUT:
					{
					setState(254);
					without();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(257);
				parameterList();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(259);
				match(AGGREGATION_OP);
				setState(260);
				parameterList();
				setState(263);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BY:
					{
					setState(261);
					by();
					}
					break;
				case WITHOUT:
					{
					setState(262);
					without();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ByContext extends ParserRuleContext {
		public TerminalNode BY() { return getToken(PromQLParser.BY, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public ByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_by; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ByContext by() throws RecognitionException {
		ByContext _localctx = new ByContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_by);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(267);
			match(BY);
			setState(268);
			labelNameList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WithoutContext extends ParserRuleContext {
		public TerminalNode WITHOUT() { return getToken(PromQLParser.WITHOUT, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public WithoutContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_without; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitWithout(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithoutContext without() throws RecognitionException {
		WithoutContext _localctx = new WithoutContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_without);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(270);
			match(WITHOUT);
			setState(271);
			labelNameList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingContext extends ParserRuleContext {
		public OnContext on() {
			return getRuleContext(OnContext.class,0);
		}
		public IgnoringContext ignoring() {
			return getRuleContext(IgnoringContext.class,0);
		}
		public GroupLeftContext groupLeft() {
			return getRuleContext(GroupLeftContext.class,0);
		}
		public GroupRightContext groupRight() {
			return getRuleContext(GroupRightContext.class,0);
		}
		public GroupingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grouping; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitGrouping(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingContext grouping() throws RecognitionException {
		GroupingContext _localctx = new GroupingContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_grouping);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(275);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				{
				setState(273);
				on();
				}
				break;
			case IGNORING:
				{
				setState(274);
				ignoring();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(279);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GROUP_LEFT:
				{
				setState(277);
				groupLeft();
				}
				break;
			case GROUP_RIGHT:
				{
				setState(278);
				groupRight();
				}
				break;
			case T__0:
			case T__2:
			case NUMBER:
			case STRING:
			case ADD:
			case SUB:
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case LIMIT:
			case AGGREGATION_OP:
			case IDENTIFIER:
			case IDENTIFIER_EXTENDED:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OnContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(PromQLParser.ON, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public OnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_on; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitOn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OnContext on() throws RecognitionException {
		OnContext _localctx = new OnContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_on);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(281);
			match(ON);
			setState(282);
			labelNameList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IgnoringContext extends ParserRuleContext {
		public TerminalNode IGNORING() { return getToken(PromQLParser.IGNORING, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public IgnoringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ignoring; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitIgnoring(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IgnoringContext ignoring() throws RecognitionException {
		IgnoringContext _localctx = new IgnoringContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_ignoring);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(284);
			match(IGNORING);
			setState(285);
			labelNameList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupLeftContext extends ParserRuleContext {
		public TerminalNode GROUP_LEFT() { return getToken(PromQLParser.GROUP_LEFT, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public GroupLeftContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupLeft; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitGroupLeft(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupLeftContext groupLeft() throws RecognitionException {
		GroupLeftContext _localctx = new GroupLeftContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_groupLeft);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287);
			match(GROUP_LEFT);
			setState(289);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
			case 1:
				{
				setState(288);
				labelNameList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupRightContext extends ParserRuleContext {
		public TerminalNode GROUP_RIGHT() { return getToken(PromQLParser.GROUP_RIGHT, 0); }
		public LabelNameListContext labelNameList() {
			return getRuleContext(LabelNameListContext.class,0);
		}
		public GroupRightContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupRight; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitGroupRight(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupRightContext groupRight() throws RecognitionException {
		GroupRightContext _localctx = new GroupRightContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_groupRight);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(291);
			match(GROUP_RIGHT);
			setState(293);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
			case 1:
				{
				setState(292);
				labelNameList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MetricNameContext extends ParserRuleContext {
		public MetricKeywordContext metricKeyword() {
			return getRuleContext(MetricKeywordContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(PromQLParser.IDENTIFIER, 0); }
		public TerminalNode IDENTIFIER_EXTENDED() { return getToken(PromQLParser.IDENTIFIER_EXTENDED, 0); }
		public MetricNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitMetricName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MetricNameContext metricName() throws RecognitionException {
		MetricNameContext _localctx = new MetricNameContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_metricName);
		try {
			setState(298);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case LIMIT:
			case AGGREGATION_OP:
				enterOuterAlt(_localctx, 1);
				{
				setState(295);
				metricKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(296);
				match(IDENTIFIER);
				}
				break;
			case IDENTIFIER_EXTENDED:
				enterOuterAlt(_localctx, 3);
				{
				setState(297);
				match(IDENTIFIER_EXTENDED);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MetricKeywordContext extends ParserRuleContext {
		public TerminalNode AND() { return getToken(PromQLParser.AND, 0); }
		public TerminalNode OR() { return getToken(PromQLParser.OR, 0); }
		public TerminalNode UNLESS() { return getToken(PromQLParser.UNLESS, 0); }
		public TerminalNode BY() { return getToken(PromQLParser.BY, 0); }
		public TerminalNode WITHOUT() { return getToken(PromQLParser.WITHOUT, 0); }
		public TerminalNode OFFSET() { return getToken(PromQLParser.OFFSET, 0); }
		public TerminalNode LIMIT() { return getToken(PromQLParser.LIMIT, 0); }
		public TerminalNode AGGREGATION_OP() { return getToken(PromQLParser.AGGREGATION_OP, 0); }
		public MetricKeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricKeyword; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitMetricKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MetricKeywordContext metricKeyword() throws RecognitionException {
		MetricKeywordContext _localctx = new MetricKeywordContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_metricKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(300);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << OFFSET) | (1L << LIMIT) | (1L << AGGREGATION_OP))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelNameContext extends ParserRuleContext {
		public LabelKeywordContext labelKeyword() {
			return getRuleContext(LabelKeywordContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(PromQLParser.IDENTIFIER, 0); }
		public LabelNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelNameContext labelName() throws RecognitionException {
		LabelNameContext _localctx = new LabelNameContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_labelName);
		try {
			setState(304);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case ON:
			case IGNORING:
			case GROUP_LEFT:
			case GROUP_RIGHT:
			case OFFSET:
			case LIMIT:
			case BOOL:
			case AGGREGATION_OP:
				enterOuterAlt(_localctx, 1);
				{
				setState(302);
				labelKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(303);
				match(IDENTIFIER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelNameListContext extends ParserRuleContext {
		public List<LabelNameContext> labelName() {
			return getRuleContexts(LabelNameContext.class);
		}
		public LabelNameContext labelName(int i) {
			return getRuleContext(LabelNameContext.class,i);
		}
		public LabelNameListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelNameList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelNameList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelNameListContext labelNameList() throws RecognitionException {
		LabelNameListContext _localctx = new LabelNameListContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_labelNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(306);
			match(T__0);
			setState(315);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << LIMIT) | (1L << BOOL) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER))) != 0)) {
				{
				setState(307);
				labelName();
				setState(312);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(308);
					match(T__7);
					setState(309);
					labelName();
					}
					}
					setState(314);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(317);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelKeywordContext extends ParserRuleContext {
		public TerminalNode AND() { return getToken(PromQLParser.AND, 0); }
		public TerminalNode OR() { return getToken(PromQLParser.OR, 0); }
		public TerminalNode UNLESS() { return getToken(PromQLParser.UNLESS, 0); }
		public TerminalNode BY() { return getToken(PromQLParser.BY, 0); }
		public TerminalNode WITHOUT() { return getToken(PromQLParser.WITHOUT, 0); }
		public TerminalNode ON() { return getToken(PromQLParser.ON, 0); }
		public TerminalNode IGNORING() { return getToken(PromQLParser.IGNORING, 0); }
		public TerminalNode GROUP_LEFT() { return getToken(PromQLParser.GROUP_LEFT, 0); }
		public TerminalNode GROUP_RIGHT() { return getToken(PromQLParser.GROUP_RIGHT, 0); }
		public TerminalNode OFFSET() { return getToken(PromQLParser.OFFSET, 0); }
		public TerminalNode LIMIT() { return getToken(PromQLParser.LIMIT, 0); }
		public TerminalNode BOOL() { return getToken(PromQLParser.BOOL, 0); }
		public TerminalNode AGGREGATION_OP() { return getToken(PromQLParser.AGGREGATION_OP, 0); }
		public LabelKeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelKeyword; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLabelKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LabelKeywordContext labelKeyword() throws RecognitionException {
		LabelKeywordContext _localctx = new LabelKeywordContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_labelKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(319);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << LIMIT) | (1L << BOOL) | (1L << AGGREGATION_OP))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(PromQLParser.NUMBER, 0); }
		public TerminalNode STRING() { return getToken(PromQLParser.STRING, 0); }
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PromQLVisitor ) return ((PromQLVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_literal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(321);
			_la = _input.LA(1);
			if ( !(_la==NUMBER || _la==STRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 1:
			return vectorExpression_sempred((VectorExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean vectorExpression_sempred(VectorExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 10);
		case 1:
			return precpred(_ctx, 8);
		case 2:
			return precpred(_ctx, 7);
		case 3:
			return precpred(_ctx, 6);
		case 4:
			return precpred(_ctx, 5);
		case 5:
			return precpred(_ctx, 4);
		case 6:
			return precpred(_ctx, 3);
		case 7:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\60\u0146\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\3\2\3\2\3\2\3\3\3\3\3\3\3"+
		"\3\3\3\5\3W\n\3\3\3\3\3\3\3\5\3\\\n\3\3\3\3\3\3\3\3\3\3\3\5\3c\n\3\3\3"+
		"\3\3\3\3\3\3\3\3\5\3j\n\3\3\3\3\3\3\3\3\3\3\3\5\3q\n\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3x\n\3\3\3\3\3\3\3\3\3\3\3\5\3\177\n\3\3\3\3\3\3\3\3\3\3\3\5\3"+
		"\u0086\n\3\3\3\3\3\7\3\u008a\n\3\f\3\16\3\u008d\13\3\3\4\3\4\3\5\3\5\3"+
		"\6\3\6\3\7\3\7\3\b\3\b\5\b\u0099\n\b\3\t\3\t\3\n\3\n\3\13\3\13\3\13\3"+
		"\13\3\13\5\13\u00a4\n\13\3\f\3\f\3\f\3\f\3\r\3\r\5\r\u00ac\n\r\3\r\3\r"+
		"\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00b6\n\r\3\16\3\16\3\16\5\16\u00bb\n\16"+
		"\3\16\5\16\u00be\n\16\3\16\3\16\3\16\3\16\5\16\u00c4\n\16\3\17\3\17\3"+
		"\17\3\17\3\20\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\23\3"+
		"\23\5\23\u00d7\n\23\3\23\3\23\3\24\3\24\3\24\3\24\3\25\3\25\3\26\3\26"+
		"\3\26\7\26\u00e4\n\26\f\26\16\26\u00e7\13\26\3\27\3\27\3\27\3\30\3\30"+
		"\5\30\u00ee\n\30\3\31\3\31\3\31\3\31\7\31\u00f4\n\31\f\31\16\31\u00f7"+
		"\13\31\5\31\u00f9\n\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\5\32\u0102\n"+
		"\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u010a\n\32\5\32\u010c\n\32\3\33"+
		"\3\33\3\33\3\34\3\34\3\34\3\35\3\35\5\35\u0116\n\35\3\35\3\35\5\35\u011a"+
		"\n\35\3\36\3\36\3\36\3\37\3\37\3\37\3 \3 \5 \u0124\n \3!\3!\5!\u0128\n"+
		"!\3\"\3\"\3\"\5\"\u012d\n\"\3#\3#\3$\3$\5$\u0133\n$\3%\3%\3%\3%\7%\u0139"+
		"\n%\f%\16%\u013c\13%\5%\u013e\n%\3%\3%\3&\3&\3\'\3\'\3\'\2\3\4(\2\4\6"+
		"\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJL\2\n"+
		"\3\2\17\20\3\2\21\23\3\2\26\33\4\2\37\37!!\5\2\25\25\27\27\34\35\5\2\37"+
		"#()++\3\2\37+\4\2\f\f\16\16\2\u014f\2N\3\2\2\2\4V\3\2\2\2\6\u008e\3\2"+
		"\2\2\b\u0090\3\2\2\2\n\u0092\3\2\2\2\f\u0094\3\2\2\2\16\u0096\3\2\2\2"+
		"\20\u009a\3\2\2\2\22\u009c\3\2\2\2\24\u00a3\3\2\2\2\26\u00a5\3\2\2\2\30"+
		"\u00a9\3\2\2\2\32\u00c3\3\2\2\2\34\u00c5\3\2\2\2\36\u00c9\3\2\2\2 \u00cc"+
		"\3\2\2\2\"\u00cf\3\2\2\2$\u00d2\3\2\2\2&\u00da\3\2\2\2(\u00de\3\2\2\2"+
		"*\u00e0\3\2\2\2,\u00e8\3\2\2\2.\u00ed\3\2\2\2\60\u00ef\3\2\2\2\62\u010b"+
		"\3\2\2\2\64\u010d\3\2\2\2\66\u0110\3\2\2\28\u0115\3\2\2\2:\u011b\3\2\2"+
		"\2<\u011e\3\2\2\2>\u0121\3\2\2\2@\u0125\3\2\2\2B\u012c\3\2\2\2D\u012e"+
		"\3\2\2\2F\u0132\3\2\2\2H\u0134\3\2\2\2J\u0141\3\2\2\2L\u0143\3\2\2\2N"+
		"O\5\4\3\2OP\7\2\2\3P\3\3\2\2\2QR\b\3\1\2RS\5\6\4\2ST\5\4\3\13TW\3\2\2"+
		"\2UW\5\24\13\2VQ\3\2\2\2VU\3\2\2\2W\u008b\3\2\2\2XY\f\f\2\2Y[\5\b\5\2"+
		"Z\\\58\35\2[Z\3\2\2\2[\\\3\2\2\2\\]\3\2\2\2]^\5\4\3\f^\u008a\3\2\2\2_"+
		"`\f\n\2\2`b\5\n\6\2ac\58\35\2ba\3\2\2\2bc\3\2\2\2cd\3\2\2\2de\5\4\3\13"+
		"e\u008a\3\2\2\2fg\f\t\2\2gi\5\f\7\2hj\58\35\2ih\3\2\2\2ij\3\2\2\2jk\3"+
		"\2\2\2kl\5\4\3\nl\u008a\3\2\2\2mn\f\b\2\2np\5\16\b\2oq\58\35\2po\3\2\2"+
		"\2pq\3\2\2\2qr\3\2\2\2rs\5\4\3\ts\u008a\3\2\2\2tu\f\7\2\2uw\5\20\t\2v"+
		"x\58\35\2wv\3\2\2\2wx\3\2\2\2xy\3\2\2\2yz\5\4\3\bz\u008a\3\2\2\2{|\f\6"+
		"\2\2|~\5\22\n\2}\177\58\35\2~}\3\2\2\2~\177\3\2\2\2\177\u0080\3\2\2\2"+
		"\u0080\u0081\5\4\3\7\u0081\u008a\3\2\2\2\u0082\u0083\f\5\2\2\u0083\u0085"+
		"\5$\23\2\u0084\u0086\5\36\20\2\u0085\u0084\3\2\2\2\u0085\u0086\3\2\2\2"+
		"\u0086\u008a\3\2\2\2\u0087\u0088\f\4\2\2\u0088\u008a\5\"\22\2\u0089X\3"+
		"\2\2\2\u0089_\3\2\2\2\u0089f\3\2\2\2\u0089m\3\2\2\2\u0089t\3\2\2\2\u0089"+
		"{\3\2\2\2\u0089\u0082\3\2\2\2\u0089\u0087\3\2\2\2\u008a\u008d\3\2\2\2"+
		"\u008b\u0089\3\2\2\2\u008b\u008c\3\2\2\2\u008c\5\3\2\2\2\u008d\u008b\3"+
		"\2\2\2\u008e\u008f\t\2\2\2\u008f\7\3\2\2\2\u0090\u0091\7\24\2\2\u0091"+
		"\t\3\2\2\2\u0092\u0093\t\3\2\2\u0093\13\3\2\2\2\u0094\u0095\t\2\2\2\u0095"+
		"\r\3\2\2\2\u0096\u0098\t\4\2\2\u0097\u0099\7*\2\2\u0098\u0097\3\2\2\2"+
		"\u0098\u0099\3\2\2\2\u0099\17\3\2\2\2\u009a\u009b\t\5\2\2\u009b\21\3\2"+
		"\2\2\u009c\u009d\7 \2\2\u009d\23\3\2\2\2\u009e\u00a4\5,\27\2\u009f\u00a4"+
		"\5\62\32\2\u00a0\u00a4\5\30\r\2\u00a1\u00a4\5L\'\2\u00a2\u00a4\5\26\f"+
		"\2\u00a3\u009e\3\2\2\2\u00a3\u009f\3\2\2\2\u00a3\u00a0\3\2\2\2\u00a3\u00a1"+
		"\3\2\2\2\u00a3\u00a2\3\2\2\2\u00a4\25\3\2\2\2\u00a5\u00a6\7\3\2\2\u00a6"+
		"\u00a7\5\4\3\2\u00a7\u00a8\7\4\2\2\u00a8\27\3\2\2\2\u00a9\u00ab\5\32\16"+
		"\2\u00aa\u00ac\5\34\17\2\u00ab\u00aa\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac"+
		"\u00b5\3\2\2\2\u00ad\u00b6\5\36\20\2\u00ae\u00b6\5 \21\2\u00af\u00b0\5"+
		"\36\20\2\u00b0\u00b1\5 \21\2\u00b1\u00b6\3\2\2\2\u00b2\u00b3\5 \21\2\u00b3"+
		"\u00b4\5\36\20\2\u00b4\u00b6\3\2\2\2\u00b5\u00ad\3\2\2\2\u00b5\u00ae\3"+
		"\2\2\2\u00b5\u00af\3\2\2\2\u00b5\u00b2\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6"+
		"\31\3\2\2\2\u00b7\u00bd\5B\"\2\u00b8\u00ba\7\5\2\2\u00b9\u00bb\5*\26\2"+
		"\u00ba\u00b9\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00be"+
		"\7\6\2\2\u00bd\u00b8\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00c4\3\2\2\2\u00bf"+
		"\u00c0\7\5\2\2\u00c0\u00c1\5*\26\2\u00c1\u00c2\7\6\2\2\u00c2\u00c4\3\2"+
		"\2\2\u00c3\u00b7\3\2\2\2\u00c3\u00bf\3\2\2\2\u00c4\33\3\2\2\2\u00c5\u00c6"+
		"\7\7\2\2\u00c6\u00c7\7,\2\2\u00c7\u00c8\7\b\2\2\u00c8\35\3\2\2\2\u00c9"+
		"\u00ca\7(\2\2\u00ca\u00cb\7,\2\2\u00cb\37\3\2\2\2\u00cc\u00cd\7\36\2\2"+
		"\u00cd\u00ce\7\13\2\2\u00ce!\3\2\2\2\u00cf\u00d0\7)\2\2\u00d0\u00d1\7"+
		"\f\2\2\u00d1#\3\2\2\2\u00d2\u00d3\7\7\2\2\u00d3\u00d4\7,\2\2\u00d4\u00d6"+
		"\7\t\2\2\u00d5\u00d7\7,\2\2\u00d6\u00d5\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7"+
		"\u00d8\3\2\2\2\u00d8\u00d9\7\b\2\2\u00d9%\3\2\2\2\u00da\u00db\5F$\2\u00db"+
		"\u00dc\5(\25\2\u00dc\u00dd\7\16\2\2\u00dd\'\3\2\2\2\u00de\u00df\t\6\2"+
		"\2\u00df)\3\2\2\2\u00e0\u00e5\5&\24\2\u00e1\u00e2\7\n\2\2\u00e2\u00e4"+
		"\5&\24\2\u00e3\u00e1\3\2\2\2\u00e4\u00e7\3\2\2\2\u00e5\u00e3\3\2\2\2\u00e5"+
		"\u00e6\3\2\2\2\u00e6+\3\2\2\2\u00e7\u00e5\3\2\2\2\u00e8\u00e9\7-\2\2\u00e9"+
		"\u00ea\5\60\31\2\u00ea-\3\2\2\2\u00eb\u00ee\5L\'\2\u00ec\u00ee\5\4\3\2"+
		"\u00ed\u00eb\3\2\2\2\u00ed\u00ec\3\2\2\2\u00ee/\3\2\2\2\u00ef\u00f8\7"+
		"\3\2\2\u00f0\u00f5\5.\30\2\u00f1\u00f2\7\n\2\2\u00f2\u00f4\5.\30\2\u00f3"+
		"\u00f1\3\2\2\2\u00f4\u00f7\3\2\2\2\u00f5\u00f3\3\2\2\2\u00f5\u00f6\3\2"+
		"\2\2\u00f6\u00f9\3\2\2\2\u00f7\u00f5\3\2\2\2\u00f8\u00f0\3\2\2\2\u00f8"+
		"\u00f9\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\7\4\2\2\u00fb\61\3\2\2"+
		"\2\u00fc\u00fd\7+\2\2\u00fd\u010c\5\60\31\2\u00fe\u0101\7+\2\2\u00ff\u0102"+
		"\5\64\33\2\u0100\u0102\5\66\34\2\u0101\u00ff\3\2\2\2\u0101\u0100\3\2\2"+
		"\2\u0102\u0103\3\2\2\2\u0103\u0104\5\60\31\2\u0104\u010c\3\2\2\2\u0105"+
		"\u0106\7+\2\2\u0106\u0109\5\60\31\2\u0107\u010a\5\64\33\2\u0108\u010a"+
		"\5\66\34\2\u0109\u0107\3\2\2\2\u0109\u0108\3\2\2\2\u010a\u010c\3\2\2\2"+
		"\u010b\u00fc\3\2\2\2\u010b\u00fe\3\2\2\2\u010b\u0105\3\2\2\2\u010c\63"+
		"\3\2\2\2\u010d\u010e\7\"\2\2\u010e\u010f\5H%\2\u010f\65\3\2\2\2\u0110"+
		"\u0111\7#\2\2\u0111\u0112\5H%\2\u0112\67\3\2\2\2\u0113\u0116\5:\36\2\u0114"+
		"\u0116\5<\37\2\u0115\u0113\3\2\2\2\u0115\u0114\3\2\2\2\u0116\u0119\3\2"+
		"\2\2\u0117\u011a\5> \2\u0118\u011a\5@!\2\u0119\u0117\3\2\2\2\u0119\u0118"+
		"\3\2\2\2\u0119\u011a\3\2\2\2\u011a9\3\2\2\2\u011b\u011c\7$\2\2\u011c\u011d"+
		"\5H%\2\u011d;\3\2\2\2\u011e\u011f\7%\2\2\u011f\u0120\5H%\2\u0120=\3\2"+
		"\2\2\u0121\u0123\7&\2\2\u0122\u0124\5H%\2\u0123\u0122\3\2\2\2\u0123\u0124"+
		"\3\2\2\2\u0124?\3\2\2\2\u0125\u0127\7\'\2\2\u0126\u0128\5H%\2\u0127\u0126"+
		"\3\2\2\2\u0127\u0128\3\2\2\2\u0128A\3\2\2\2\u0129\u012d\5D#\2\u012a\u012d"+
		"\7-\2\2\u012b\u012d\7.\2\2\u012c\u0129\3\2\2\2\u012c\u012a\3\2\2\2\u012c"+
		"\u012b\3\2\2\2\u012dC\3\2\2\2\u012e\u012f\t\7\2\2\u012fE\3\2\2\2\u0130"+
		"\u0133\5J&\2\u0131\u0133\7-\2\2\u0132\u0130\3\2\2\2\u0132\u0131\3\2\2"+
		"\2\u0133G\3\2\2\2\u0134\u013d\7\3\2\2\u0135\u013a\5F$\2\u0136\u0137\7"+
		"\n\2\2\u0137\u0139\5F$\2\u0138\u0136\3\2\2\2\u0139\u013c\3\2\2\2\u013a"+
		"\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u013e\3\2\2\2\u013c\u013a\3\2"+
		"\2\2\u013d\u0135\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013f\3\2\2\2\u013f"+
		"\u0140\7\4\2\2\u0140I\3\2\2\2\u0141\u0142\t\b\2\2\u0142K\3\2\2\2\u0143"+
		"\u0144\t\t\2\2\u0144M\3\2\2\2#V[bipw~\u0085\u0089\u008b\u0098\u00a3\u00ab"+
		"\u00b5\u00ba\u00bd\u00c3\u00d6\u00e5\u00ed\u00f5\u00f8\u0101\u0109\u010b"+
		"\u0115\u0119\u0123\u0127\u012c\u0132\u013a\u013d";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}