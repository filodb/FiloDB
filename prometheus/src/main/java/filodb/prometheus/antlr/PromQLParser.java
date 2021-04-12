// Generated from PromQL.g4 by ANTLR 4.9.1
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
	static { RuntimeMetaData.checkVersion("4.9.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, NUMBER=8, STRING=9, 
		ADD=10, SUB=11, MUL=12, DIV=13, MOD=14, POW=15, EQ=16, DEQ=17, NE=18, 
		GT=19, LT=20, GE=21, LE=22, RE=23, NRE=24, AND=25, OR=26, UNLESS=27, BY=28, 
		WITHOUT=29, ON=30, IGNORING=31, GROUP_LEFT=32, GROUP_RIGHT=33, OFFSET=34, 
		BOOL=35, AGGREGATION_OP=36, DURATION=37, IDENTIFIER=38, IDENTIFIER_EXTENDED=39, 
		WS=40, COMMENT=41;
	public static final int
		RULE_expression = 0, RULE_vectorExpression = 1, RULE_unaryOp = 2, RULE_powOp = 3, 
		RULE_multOp = 4, RULE_addOp = 5, RULE_compareOp = 6, RULE_andUnlessOp = 7, 
		RULE_orOp = 8, RULE_vector = 9, RULE_parens = 10, RULE_instantOrRangeSelector = 11, 
		RULE_instantSelector = 12, RULE_window = 13, RULE_offset = 14, RULE_labelMatcher = 15, 
		RULE_labelMatcherOp = 16, RULE_labelMatcherList = 17, RULE_function = 18, 
		RULE_parameter = 19, RULE_parameterList = 20, RULE_aggregation = 21, RULE_by = 22, 
		RULE_without = 23, RULE_grouping = 24, RULE_on = 25, RULE_ignoring = 26, 
		RULE_groupLeft = 27, RULE_groupRight = 28, RULE_metricName = 29, RULE_metricKeyword = 30, 
		RULE_labelName = 31, RULE_labelNameList = 32, RULE_labelKeyword = 33, 
		RULE_literal = 34;
	private static String[] makeRuleNames() {
		return new String[] {
			"expression", "vectorExpression", "unaryOp", "powOp", "multOp", "addOp", 
			"compareOp", "andUnlessOp", "orOp", "vector", "parens", "instantOrRangeSelector", 
			"instantSelector", "window", "offset", "labelMatcher", "labelMatcherOp", 
			"labelMatcherList", "function", "parameter", "parameterList", "aggregation", 
			"by", "without", "grouping", "on", "ignoring", "groupLeft", "groupRight", 
			"metricName", "metricKeyword", "labelName", "labelNameList", "labelKeyword", 
			"literal"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'{'", "'}'", "'['", "']'", "','", null, null, "'+'", 
			"'-'", "'*'", "'/'", "'%'", "'^'", "'='", "'=='", "'!='", "'>'", "'<'", 
			"'>='", "'<='", "'=~'", "'!~'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, "NUMBER", "STRING", "ADD", 
			"SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", "GT", "LT", "GE", 
			"LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", "IGNORING", 
			"GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "BOOL", "AGGREGATION_OP", "DURATION", 
			"IDENTIFIER", "IDENTIFIER_EXTENDED", "WS", "COMMENT"
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
			setState(70);
			vectorExpression(0);
			setState(71);
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
			setState(78);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
			case SUB:
				{
				_localctx = new UnaryOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(74);
				unaryOp();
				setState(75);
				vectorExpression(7);
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
			case AGGREGATION_OP:
			case IDENTIFIER:
			case IDENTIFIER_EXTENDED:
				{
				_localctx = new VectorOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(77);
				vector();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(124);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(122);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(80);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(81);
						powOp();
						setState(83);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(82);
							grouping();
							}
						}

						setState(85);
						vectorExpression(8);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(87);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(88);
						multOp();
						setState(90);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(89);
							grouping();
							}
						}

						setState(92);
						vectorExpression(7);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(94);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(95);
						addOp();
						setState(97);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(96);
							grouping();
							}
						}

						setState(99);
						vectorExpression(6);
						}
						break;
					case 4:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(101);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(102);
						compareOp();
						setState(104);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(103);
							grouping();
							}
						}

						setState(106);
						vectorExpression(5);
						}
						break;
					case 5:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(108);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(109);
						andUnlessOp();
						setState(111);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(110);
							grouping();
							}
						}

						setState(113);
						vectorExpression(4);
						}
						break;
					case 6:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(115);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(116);
						orOp();
						setState(118);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(117);
							grouping();
							}
						}

						setState(120);
						vectorExpression(3);
						}
						break;
					}
					} 
				}
				setState(126);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
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
			setState(127);
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
			setState(129);
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
			setState(131);
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
			setState(133);
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
			setState(135);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DEQ) | (1L << NE) | (1L << GT) | (1L << LT) | (1L << GE) | (1L << LE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(137);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BOOL) {
				{
				setState(136);
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
			setState(139);
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
			setState(141);
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
			setState(148);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(143);
				function();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(144);
				aggregation();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(145);
				instantOrRangeSelector();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(146);
				literal();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(147);
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
			setState(150);
			match(T__0);
			setState(151);
			vectorExpression(0);
			setState(152);
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
			setState(154);
			instantSelector();
			setState(156);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				setState(155);
				window();
				}
				break;
			}
			setState(159);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(158);
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
			setState(173);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case AGGREGATION_OP:
			case IDENTIFIER:
			case IDENTIFIER_EXTENDED:
				enterOuterAlt(_localctx, 1);
				{
				setState(161);
				metricName();
				setState(167);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(162);
					match(T__2);
					setState(164);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << BOOL) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER))) != 0)) {
						{
						setState(163);
						labelMatcherList();
						}
					}

					setState(166);
					match(T__3);
					}
					break;
				}
				}
				break;
			case T__2:
				enterOuterAlt(_localctx, 2);
				{
				setState(169);
				match(T__2);
				setState(170);
				labelMatcherList();
				setState(171);
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
			setState(175);
			match(T__4);
			setState(176);
			match(DURATION);
			setState(177);
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
			setState(179);
			match(OFFSET);
			setState(180);
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
		enterRule(_localctx, 30, RULE_labelMatcher);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(182);
			labelName();
			setState(183);
			labelMatcherOp();
			setState(184);
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
		enterRule(_localctx, 32, RULE_labelMatcherOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(186);
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
		enterRule(_localctx, 34, RULE_labelMatcherList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(188);
			labelMatcher();
			setState(193);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(189);
				match(T__6);
				setState(190);
				labelMatcher();
				}
				}
				setState(195);
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
		enterRule(_localctx, 36, RULE_function);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(196);
			match(IDENTIFIER);
			setState(197);
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
		enterRule(_localctx, 38, RULE_parameter);
		try {
			setState(201);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(199);
				literal();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(200);
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
		enterRule(_localctx, 40, RULE_parameterList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(203);
			match(T__0);
			setState(212);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << NUMBER) | (1L << STRING) | (1L << ADD) | (1L << SUB) | (1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << OFFSET) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER) | (1L << IDENTIFIER_EXTENDED))) != 0)) {
				{
				setState(204);
				parameter();
				setState(209);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__6) {
					{
					{
					setState(205);
					match(T__6);
					setState(206);
					parameter();
					}
					}
					setState(211);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(214);
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
		enterRule(_localctx, 42, RULE_aggregation);
		try {
			setState(231);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(216);
				match(AGGREGATION_OP);
				setState(217);
				parameterList();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(218);
				match(AGGREGATION_OP);
				setState(221);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BY:
					{
					setState(219);
					by();
					}
					break;
				case WITHOUT:
					{
					setState(220);
					without();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(223);
				parameterList();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(225);
				match(AGGREGATION_OP);
				setState(226);
				parameterList();
				setState(229);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BY:
					{
					setState(227);
					by();
					}
					break;
				case WITHOUT:
					{
					setState(228);
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
		enterRule(_localctx, 44, RULE_by);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(233);
			match(BY);
			setState(234);
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
		enterRule(_localctx, 46, RULE_without);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(236);
			match(WITHOUT);
			setState(237);
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
		enterRule(_localctx, 48, RULE_grouping);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(241);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				{
				setState(239);
				on();
				}
				break;
			case IGNORING:
				{
				setState(240);
				ignoring();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(245);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GROUP_LEFT:
				{
				setState(243);
				groupLeft();
				}
				break;
			case GROUP_RIGHT:
				{
				setState(244);
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
		enterRule(_localctx, 50, RULE_on);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(247);
			match(ON);
			setState(248);
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
		enterRule(_localctx, 52, RULE_ignoring);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(250);
			match(IGNORING);
			setState(251);
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
		enterRule(_localctx, 54, RULE_groupLeft);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(253);
			match(GROUP_LEFT);
			setState(255);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
			case 1:
				{
				setState(254);
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
		enterRule(_localctx, 56, RULE_groupRight);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(257);
			match(GROUP_RIGHT);
			setState(259);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				{
				setState(258);
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
		enterRule(_localctx, 58, RULE_metricName);
		try {
			setState(264);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case AND:
			case OR:
			case UNLESS:
			case BY:
			case WITHOUT:
			case OFFSET:
			case AGGREGATION_OP:
				enterOuterAlt(_localctx, 1);
				{
				setState(261);
				metricKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(262);
				match(IDENTIFIER);
				}
				break;
			case IDENTIFIER_EXTENDED:
				enterOuterAlt(_localctx, 3);
				{
				setState(263);
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
		enterRule(_localctx, 60, RULE_metricKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(266);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << OFFSET) | (1L << AGGREGATION_OP))) != 0)) ) {
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
		enterRule(_localctx, 62, RULE_labelName);
		try {
			setState(270);
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
			case BOOL:
			case AGGREGATION_OP:
				enterOuterAlt(_localctx, 1);
				{
				setState(268);
				labelKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(269);
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
		enterRule(_localctx, 64, RULE_labelNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(272);
			match(T__0);
			setState(281);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << BOOL) | (1L << AGGREGATION_OP) | (1L << IDENTIFIER))) != 0)) {
				{
				setState(273);
				labelName();
				setState(278);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__6) {
					{
					{
					setState(274);
					match(T__6);
					setState(275);
					labelName();
					}
					}
					setState(280);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(283);
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
		enterRule(_localctx, 66, RULE_labelKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(285);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AND) | (1L << OR) | (1L << UNLESS) | (1L << BY) | (1L << WITHOUT) | (1L << ON) | (1L << IGNORING) | (1L << GROUP_LEFT) | (1L << GROUP_RIGHT) | (1L << OFFSET) | (1L << BOOL) | (1L << AGGREGATION_OP))) != 0)) ) {
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
		enterRule(_localctx, 68, RULE_literal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287);
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
			return precpred(_ctx, 8);
		case 1:
			return precpred(_ctx, 6);
		case 2:
			return precpred(_ctx, 5);
		case 3:
			return precpred(_ctx, 4);
		case 4:
			return precpred(_ctx, 3);
		case 5:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3+\u0124\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\5\3Q\n\3\3\3\3"+
		"\3\3\3\5\3V\n\3\3\3\3\3\3\3\3\3\3\3\5\3]\n\3\3\3\3\3\3\3\3\3\3\3\5\3d"+
		"\n\3\3\3\3\3\3\3\3\3\3\3\5\3k\n\3\3\3\3\3\3\3\3\3\3\3\5\3r\n\3\3\3\3\3"+
		"\3\3\3\3\3\3\5\3y\n\3\3\3\3\3\7\3}\n\3\f\3\16\3\u0080\13\3\3\4\3\4\3\5"+
		"\3\5\3\6\3\6\3\7\3\7\3\b\3\b\5\b\u008c\n\b\3\t\3\t\3\n\3\n\3\13\3\13\3"+
		"\13\3\13\3\13\5\13\u0097\n\13\3\f\3\f\3\f\3\f\3\r\3\r\5\r\u009f\n\r\3"+
		"\r\5\r\u00a2\n\r\3\16\3\16\3\16\5\16\u00a7\n\16\3\16\5\16\u00aa\n\16\3"+
		"\16\3\16\3\16\3\16\5\16\u00b0\n\16\3\17\3\17\3\17\3\17\3\20\3\20\3\20"+
		"\3\21\3\21\3\21\3\21\3\22\3\22\3\23\3\23\3\23\7\23\u00c2\n\23\f\23\16"+
		"\23\u00c5\13\23\3\24\3\24\3\24\3\25\3\25\5\25\u00cc\n\25\3\26\3\26\3\26"+
		"\3\26\7\26\u00d2\n\26\f\26\16\26\u00d5\13\26\5\26\u00d7\n\26\3\26\3\26"+
		"\3\27\3\27\3\27\3\27\3\27\5\27\u00e0\n\27\3\27\3\27\3\27\3\27\3\27\3\27"+
		"\5\27\u00e8\n\27\5\27\u00ea\n\27\3\30\3\30\3\30\3\31\3\31\3\31\3\32\3"+
		"\32\5\32\u00f4\n\32\3\32\3\32\5\32\u00f8\n\32\3\33\3\33\3\33\3\34\3\34"+
		"\3\34\3\35\3\35\5\35\u0102\n\35\3\36\3\36\5\36\u0106\n\36\3\37\3\37\3"+
		"\37\5\37\u010b\n\37\3 \3 \3!\3!\5!\u0111\n!\3\"\3\"\3\"\3\"\7\"\u0117"+
		"\n\"\f\"\16\"\u011a\13\"\5\"\u011c\n\"\3\"\3\"\3#\3#\3$\3$\3$\2\3\4%\2"+
		"\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDF\2\n"+
		"\3\2\f\r\3\2\16\20\3\2\23\30\4\2\33\33\35\35\5\2\22\22\24\24\31\32\5\2"+
		"\33\37$$&&\3\2\33&\3\2\n\13\2\u0129\2H\3\2\2\2\4P\3\2\2\2\6\u0081\3\2"+
		"\2\2\b\u0083\3\2\2\2\n\u0085\3\2\2\2\f\u0087\3\2\2\2\16\u0089\3\2\2\2"+
		"\20\u008d\3\2\2\2\22\u008f\3\2\2\2\24\u0096\3\2\2\2\26\u0098\3\2\2\2\30"+
		"\u009c\3\2\2\2\32\u00af\3\2\2\2\34\u00b1\3\2\2\2\36\u00b5\3\2\2\2 \u00b8"+
		"\3\2\2\2\"\u00bc\3\2\2\2$\u00be\3\2\2\2&\u00c6\3\2\2\2(\u00cb\3\2\2\2"+
		"*\u00cd\3\2\2\2,\u00e9\3\2\2\2.\u00eb\3\2\2\2\60\u00ee\3\2\2\2\62\u00f3"+
		"\3\2\2\2\64\u00f9\3\2\2\2\66\u00fc\3\2\2\28\u00ff\3\2\2\2:\u0103\3\2\2"+
		"\2<\u010a\3\2\2\2>\u010c\3\2\2\2@\u0110\3\2\2\2B\u0112\3\2\2\2D\u011f"+
		"\3\2\2\2F\u0121\3\2\2\2HI\5\4\3\2IJ\7\2\2\3J\3\3\2\2\2KL\b\3\1\2LM\5\6"+
		"\4\2MN\5\4\3\tNQ\3\2\2\2OQ\5\24\13\2PK\3\2\2\2PO\3\2\2\2Q~\3\2\2\2RS\f"+
		"\n\2\2SU\5\b\5\2TV\5\62\32\2UT\3\2\2\2UV\3\2\2\2VW\3\2\2\2WX\5\4\3\nX"+
		"}\3\2\2\2YZ\f\b\2\2Z\\\5\n\6\2[]\5\62\32\2\\[\3\2\2\2\\]\3\2\2\2]^\3\2"+
		"\2\2^_\5\4\3\t_}\3\2\2\2`a\f\7\2\2ac\5\f\7\2bd\5\62\32\2cb\3\2\2\2cd\3"+
		"\2\2\2de\3\2\2\2ef\5\4\3\bf}\3\2\2\2gh\f\6\2\2hj\5\16\b\2ik\5\62\32\2"+
		"ji\3\2\2\2jk\3\2\2\2kl\3\2\2\2lm\5\4\3\7m}\3\2\2\2no\f\5\2\2oq\5\20\t"+
		"\2pr\5\62\32\2qp\3\2\2\2qr\3\2\2\2rs\3\2\2\2st\5\4\3\6t}\3\2\2\2uv\f\4"+
		"\2\2vx\5\22\n\2wy\5\62\32\2xw\3\2\2\2xy\3\2\2\2yz\3\2\2\2z{\5\4\3\5{}"+
		"\3\2\2\2|R\3\2\2\2|Y\3\2\2\2|`\3\2\2\2|g\3\2\2\2|n\3\2\2\2|u\3\2\2\2}"+
		"\u0080\3\2\2\2~|\3\2\2\2~\177\3\2\2\2\177\5\3\2\2\2\u0080~\3\2\2\2\u0081"+
		"\u0082\t\2\2\2\u0082\7\3\2\2\2\u0083\u0084\7\21\2\2\u0084\t\3\2\2\2\u0085"+
		"\u0086\t\3\2\2\u0086\13\3\2\2\2\u0087\u0088\t\2\2\2\u0088\r\3\2\2\2\u0089"+
		"\u008b\t\4\2\2\u008a\u008c\7%\2\2\u008b\u008a\3\2\2\2\u008b\u008c\3\2"+
		"\2\2\u008c\17\3\2\2\2\u008d\u008e\t\5\2\2\u008e\21\3\2\2\2\u008f\u0090"+
		"\7\34\2\2\u0090\23\3\2\2\2\u0091\u0097\5&\24\2\u0092\u0097\5,\27\2\u0093"+
		"\u0097\5\30\r\2\u0094\u0097\5F$\2\u0095\u0097\5\26\f\2\u0096\u0091\3\2"+
		"\2\2\u0096\u0092\3\2\2\2\u0096\u0093\3\2\2\2\u0096\u0094\3\2\2\2\u0096"+
		"\u0095\3\2\2\2\u0097\25\3\2\2\2\u0098\u0099\7\3\2\2\u0099\u009a\5\4\3"+
		"\2\u009a\u009b\7\4\2\2\u009b\27\3\2\2\2\u009c\u009e\5\32\16\2\u009d\u009f"+
		"\5\34\17\2\u009e\u009d\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u00a1\3\2\2\2"+
		"\u00a0\u00a2\5\36\20\2\u00a1\u00a0\3\2\2\2\u00a1\u00a2\3\2\2\2\u00a2\31"+
		"\3\2\2\2\u00a3\u00a9\5<\37\2\u00a4\u00a6\7\5\2\2\u00a5\u00a7\5$\23\2\u00a6"+
		"\u00a5\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00a8\3\2\2\2\u00a8\u00aa\7\6"+
		"\2\2\u00a9\u00a4\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00b0\3\2\2\2\u00ab"+
		"\u00ac\7\5\2\2\u00ac\u00ad\5$\23\2\u00ad\u00ae\7\6\2\2\u00ae\u00b0\3\2"+
		"\2\2\u00af\u00a3\3\2\2\2\u00af\u00ab\3\2\2\2\u00b0\33\3\2\2\2\u00b1\u00b2"+
		"\7\7\2\2\u00b2\u00b3\7\'\2\2\u00b3\u00b4\7\b\2\2\u00b4\35\3\2\2\2\u00b5"+
		"\u00b6\7$\2\2\u00b6\u00b7\7\'\2\2\u00b7\37\3\2\2\2\u00b8\u00b9\5@!\2\u00b9"+
		"\u00ba\5\"\22\2\u00ba\u00bb\7\13\2\2\u00bb!\3\2\2\2\u00bc\u00bd\t\6\2"+
		"\2\u00bd#\3\2\2\2\u00be\u00c3\5 \21\2\u00bf\u00c0\7\t\2\2\u00c0\u00c2"+
		"\5 \21\2\u00c1\u00bf\3\2\2\2\u00c2\u00c5\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c3"+
		"\u00c4\3\2\2\2\u00c4%\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c6\u00c7\7(\2\2\u00c7"+
		"\u00c8\5*\26\2\u00c8\'\3\2\2\2\u00c9\u00cc\5F$\2\u00ca\u00cc\5\4\3\2\u00cb"+
		"\u00c9\3\2\2\2\u00cb\u00ca\3\2\2\2\u00cc)\3\2\2\2\u00cd\u00d6\7\3\2\2"+
		"\u00ce\u00d3\5(\25\2\u00cf\u00d0\7\t\2\2\u00d0\u00d2\5(\25\2\u00d1\u00cf"+
		"\3\2\2\2\u00d2\u00d5\3\2\2\2\u00d3\u00d1\3\2\2\2\u00d3\u00d4\3\2\2\2\u00d4"+
		"\u00d7\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d6\u00ce\3\2\2\2\u00d6\u00d7\3\2"+
		"\2\2\u00d7\u00d8\3\2\2\2\u00d8\u00d9\7\4\2\2\u00d9+\3\2\2\2\u00da\u00db"+
		"\7&\2\2\u00db\u00ea\5*\26\2\u00dc\u00df\7&\2\2\u00dd\u00e0\5.\30\2\u00de"+
		"\u00e0\5\60\31\2\u00df\u00dd\3\2\2\2\u00df\u00de\3\2\2\2\u00e0\u00e1\3"+
		"\2\2\2\u00e1\u00e2\5*\26\2\u00e2\u00ea\3\2\2\2\u00e3\u00e4\7&\2\2\u00e4"+
		"\u00e7\5*\26\2\u00e5\u00e8\5.\30\2\u00e6\u00e8\5\60\31\2\u00e7\u00e5\3"+
		"\2\2\2\u00e7\u00e6\3\2\2\2\u00e8\u00ea\3\2\2\2\u00e9\u00da\3\2\2\2\u00e9"+
		"\u00dc\3\2\2\2\u00e9\u00e3\3\2\2\2\u00ea-\3\2\2\2\u00eb\u00ec\7\36\2\2"+
		"\u00ec\u00ed\5B\"\2\u00ed/\3\2\2\2\u00ee\u00ef\7\37\2\2\u00ef\u00f0\5"+
		"B\"\2\u00f0\61\3\2\2\2\u00f1\u00f4\5\64\33\2\u00f2\u00f4\5\66\34\2\u00f3"+
		"\u00f1\3\2\2\2\u00f3\u00f2\3\2\2\2\u00f4\u00f7\3\2\2\2\u00f5\u00f8\58"+
		"\35\2\u00f6\u00f8\5:\36\2\u00f7\u00f5\3\2\2\2\u00f7\u00f6\3\2\2\2\u00f7"+
		"\u00f8\3\2\2\2\u00f8\63\3\2\2\2\u00f9\u00fa\7 \2\2\u00fa\u00fb\5B\"\2"+
		"\u00fb\65\3\2\2\2\u00fc\u00fd\7!\2\2\u00fd\u00fe\5B\"\2\u00fe\67\3\2\2"+
		"\2\u00ff\u0101\7\"\2\2\u0100\u0102\5B\"\2\u0101\u0100\3\2\2\2\u0101\u0102"+
		"\3\2\2\2\u01029\3\2\2\2\u0103\u0105\7#\2\2\u0104\u0106\5B\"\2\u0105\u0104"+
		"\3\2\2\2\u0105\u0106\3\2\2\2\u0106;\3\2\2\2\u0107\u010b\5> \2\u0108\u010b"+
		"\7(\2\2\u0109\u010b\7)\2\2\u010a\u0107\3\2\2\2\u010a\u0108\3\2\2\2\u010a"+
		"\u0109\3\2\2\2\u010b=\3\2\2\2\u010c\u010d\t\7\2\2\u010d?\3\2\2\2\u010e"+
		"\u0111\5D#\2\u010f\u0111\7(\2\2\u0110\u010e\3\2\2\2\u0110\u010f\3\2\2"+
		"\2\u0111A\3\2\2\2\u0112\u011b\7\3\2\2\u0113\u0118\5@!\2\u0114\u0115\7"+
		"\t\2\2\u0115\u0117\5@!\2\u0116\u0114\3\2\2\2\u0117\u011a\3\2\2\2\u0118"+
		"\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011c\3\2\2\2\u011a\u0118\3\2"+
		"\2\2\u011b\u0113\3\2\2\2\u011b\u011c\3\2\2\2\u011c\u011d\3\2\2\2\u011d"+
		"\u011e\7\4\2\2\u011eC\3\2\2\2\u011f\u0120\t\b\2\2\u0120E\3\2\2\2\u0121"+
		"\u0122\t\t\2\2\u0122G\3\2\2\2!PU\\cjqx|~\u008b\u0096\u009e\u00a1\u00a6"+
		"\u00a9\u00af\u00c3\u00cb\u00d3\u00d6\u00df\u00e7\u00e9\u00f3\u00f7\u0101"+
		"\u0105\u010a\u0110\u0118\u011b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}