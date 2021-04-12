// Generated from PromQL.g4 by ANTLR 4.9.1
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PromQLLexer extends Lexer {
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
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "NUMBER", "STRING", 
			"ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", "GT", "LT", 
			"GE", "LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", 
			"IGNORING", "GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "BOOL", "AGGREGATION_OP", 
			"DURATION", "IDENTIFIER", "IDENTIFIER_EXTENDED", "A", "B", "C", "D", 
			"E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", 
			"S", "T", "U", "V", "W", "X", "Y", "Z", "WS", "COMMENT"
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


	public PromQLLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "PromQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2+\u01e8\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\3\2\3\2\3\3\3\3\3\4\3\4\3\5"+
		"\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\5\t\u0099\n\t\3\t\7\t\u009c\n\t\f\t\16"+
		"\t\u009f\13\t\3\t\5\t\u00a2\n\t\3\t\6\t\u00a5\n\t\r\t\16\t\u00a6\3\t\3"+
		"\t\5\t\u00ab\n\t\3\t\6\t\u00ae\n\t\r\t\16\t\u00af\5\t\u00b2\n\t\3\t\6"+
		"\t\u00b5\n\t\r\t\16\t\u00b6\3\t\3\t\3\t\3\t\6\t\u00bd\n\t\r\t\16\t\u00be"+
		"\5\t\u00c1\n\t\3\n\3\n\3\n\3\n\7\n\u00c7\n\n\f\n\16\n\u00ca\13\n\3\n\3"+
		"\n\3\n\3\n\3\n\7\n\u00d1\n\n\f\n\16\n\u00d4\13\n\3\n\5\n\u00d7\n\n\3\13"+
		"\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20\3\21\3\21\3\22\3\22"+
		"\3\22\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\26\3\26\3\26\3\27\3\27\3\27"+
		"\3\30\3\30\3\30\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\34"+
		"\3\34\3\34\3\34\3\34\3\34\3\34\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36"+
		"\3\36\3\36\3\36\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \3 \3 \3 \3!\3!\3!\3!"+
		"\3!\3!\3!\3!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\""+
		"\3#\3#\3#\3#\3#\3#\3#\3$\3$\3$\3$\3$\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%"+
		"\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%"+
		"\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%"+
		"\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\5%\u0192"+
		"\n%\3&\3&\3&\3\'\3\'\7\'\u0199\n\'\f\'\16\'\u019c\13\'\3(\3(\7(\u01a0"+
		"\n(\f(\16(\u01a3\13(\3)\3)\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3"+
		"\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\3\65\3\65\3\66\3\66\3\67\3"+
		"\67\38\38\39\39\3:\3:\3;\3;\3<\3<\3=\3=\3>\3>\3?\3?\3@\3@\3A\3A\3B\3B"+
		"\3C\6C\u01da\nC\rC\16C\u01db\3C\3C\3D\3D\7D\u01e2\nD\fD\16D\u01e5\13D"+
		"\3D\3D\2\2E\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36;\37= ?!A\"C#E$G%I&K\'M(O)Q\2S\2U\2W\2Y\2[\2]\2_\2a\2c\2e\2g\2"+
		"i\2k\2m\2o\2q\2s\2u\2w\2y\2{\2}\2\177\2\u0081\2\u0083\2\u0085*\u0087+"+
		"\3\2(\4\2--//\3\2\62;\4\2GGgg\4\2ZZzz\5\2\62;CHch\4\2))^^\4\2$$^^\b\2"+
		"ffjkoouuyy{{\5\2C\\aac|\6\2\62;C\\aac|\6\2<<C\\aac|\7\2/\60\62<C\\aac"+
		"|\4\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4\2"+
		"LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTtt\4"+
		"\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4\2[[{{\4\2\\\\||\5\2\13\f\17\17"+
		"\"\"\4\2\f\f\17\17\2\u01ec\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2"+
		"\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2"+
		"\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3"+
		"\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2"+
		"\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67"+
		"\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2"+
		"\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2"+
		"\2\u0085\3\2\2\2\2\u0087\3\2\2\2\3\u0089\3\2\2\2\5\u008b\3\2\2\2\7\u008d"+
		"\3\2\2\2\t\u008f\3\2\2\2\13\u0091\3\2\2\2\r\u0093\3\2\2\2\17\u0095\3\2"+
		"\2\2\21\u0098\3\2\2\2\23\u00d6\3\2\2\2\25\u00d8\3\2\2\2\27\u00da\3\2\2"+
		"\2\31\u00dc\3\2\2\2\33\u00de\3\2\2\2\35\u00e0\3\2\2\2\37\u00e2\3\2\2\2"+
		"!\u00e4\3\2\2\2#\u00e6\3\2\2\2%\u00e9\3\2\2\2\'\u00ec\3\2\2\2)\u00ee\3"+
		"\2\2\2+\u00f0\3\2\2\2-\u00f3\3\2\2\2/\u00f6\3\2\2\2\61\u00f9\3\2\2\2\63"+
		"\u00fc\3\2\2\2\65\u0100\3\2\2\2\67\u0103\3\2\2\29\u010a\3\2\2\2;\u010d"+
		"\3\2\2\2=\u0115\3\2\2\2?\u0118\3\2\2\2A\u0121\3\2\2\2C\u012c\3\2\2\2E"+
		"\u0138\3\2\2\2G\u013f\3\2\2\2I\u0191\3\2\2\2K\u0193\3\2\2\2M\u0196\3\2"+
		"\2\2O\u019d\3\2\2\2Q\u01a4\3\2\2\2S\u01a6\3\2\2\2U\u01a8\3\2\2\2W\u01aa"+
		"\3\2\2\2Y\u01ac\3\2\2\2[\u01ae\3\2\2\2]\u01b0\3\2\2\2_\u01b2\3\2\2\2a"+
		"\u01b4\3\2\2\2c\u01b6\3\2\2\2e\u01b8\3\2\2\2g\u01ba\3\2\2\2i\u01bc\3\2"+
		"\2\2k\u01be\3\2\2\2m\u01c0\3\2\2\2o\u01c2\3\2\2\2q\u01c4\3\2\2\2s\u01c6"+
		"\3\2\2\2u\u01c8\3\2\2\2w\u01ca\3\2\2\2y\u01cc\3\2\2\2{\u01ce\3\2\2\2}"+
		"\u01d0\3\2\2\2\177\u01d2\3\2\2\2\u0081\u01d4\3\2\2\2\u0083\u01d6\3\2\2"+
		"\2\u0085\u01d9\3\2\2\2\u0087\u01df\3\2\2\2\u0089\u008a\7*\2\2\u008a\4"+
		"\3\2\2\2\u008b\u008c\7+\2\2\u008c\6\3\2\2\2\u008d\u008e\7}\2\2\u008e\b"+
		"\3\2\2\2\u008f\u0090\7\177\2\2\u0090\n\3\2\2\2\u0091\u0092\7]\2\2\u0092"+
		"\f\3\2\2\2\u0093\u0094\7_\2\2\u0094\16\3\2\2\2\u0095\u0096\7.\2\2\u0096"+
		"\20\3\2\2\2\u0097\u0099\t\2\2\2\u0098\u0097\3\2\2\2\u0098\u0099\3\2\2"+
		"\2\u0099\u00c0\3\2\2\2\u009a\u009c\t\3\2\2\u009b\u009a\3\2\2\2\u009c\u009f"+
		"\3\2\2\2\u009d\u009b\3\2\2\2\u009d\u009e\3\2\2\2\u009e\u00a1\3\2\2\2\u009f"+
		"\u009d\3\2\2\2\u00a0\u00a2\7\60\2\2\u00a1\u00a0\3\2\2\2\u00a1\u00a2\3"+
		"\2\2\2\u00a2\u00a4\3\2\2\2\u00a3\u00a5\t\3\2\2\u00a4\u00a3\3\2\2\2\u00a5"+
		"\u00a6\3\2\2\2\u00a6\u00a4\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00b1\3\2"+
		"\2\2\u00a8\u00aa\t\4\2\2\u00a9\u00ab\t\2\2\2\u00aa\u00a9\3\2\2\2\u00aa"+
		"\u00ab\3\2\2\2\u00ab\u00ad\3\2\2\2\u00ac\u00ae\t\3\2\2\u00ad\u00ac\3\2"+
		"\2\2\u00ae\u00af\3\2\2\2\u00af\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0"+
		"\u00b2\3\2\2\2\u00b1\u00a8\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00c1\3\2"+
		"\2\2\u00b3\u00b5\t\3\2\2\u00b4\u00b3\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6"+
		"\u00b4\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00c1\7\60"+
		"\2\2\u00b9\u00ba\7\62\2\2\u00ba\u00bc\t\5\2\2\u00bb\u00bd\t\6\2\2\u00bc"+
		"\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bc\3\2\2\2\u00be\u00bf\3\2"+
		"\2\2\u00bf\u00c1\3\2\2\2\u00c0\u009d\3\2\2\2\u00c0\u00b4\3\2\2\2\u00c0"+
		"\u00b9\3\2\2\2\u00c1\22\3\2\2\2\u00c2\u00c8\7)\2\2\u00c3\u00c7\n\7\2\2"+
		"\u00c4\u00c5\7^\2\2\u00c5\u00c7\13\2\2\2\u00c6\u00c3\3\2\2\2\u00c6\u00c4"+
		"\3\2\2\2\u00c7\u00ca\3\2\2\2\u00c8\u00c6\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9"+
		"\u00cb\3\2\2\2\u00ca\u00c8\3\2\2\2\u00cb\u00d7\7)\2\2\u00cc\u00d2\7$\2"+
		"\2\u00cd\u00d1\n\b\2\2\u00ce\u00cf\7^\2\2\u00cf\u00d1\13\2\2\2\u00d0\u00cd"+
		"\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d1\u00d4\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d2"+
		"\u00d3\3\2\2\2\u00d3\u00d5\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d5\u00d7\7$"+
		"\2\2\u00d6\u00c2\3\2\2\2\u00d6\u00cc\3\2\2\2\u00d7\24\3\2\2\2\u00d8\u00d9"+
		"\7-\2\2\u00d9\26\3\2\2\2\u00da\u00db\7/\2\2\u00db\30\3\2\2\2\u00dc\u00dd"+
		"\7,\2\2\u00dd\32\3\2\2\2\u00de\u00df\7\61\2\2\u00df\34\3\2\2\2\u00e0\u00e1"+
		"\7\'\2\2\u00e1\36\3\2\2\2\u00e2\u00e3\7`\2\2\u00e3 \3\2\2\2\u00e4\u00e5"+
		"\7?\2\2\u00e5\"\3\2\2\2\u00e6\u00e7\7?\2\2\u00e7\u00e8\7?\2\2\u00e8$\3"+
		"\2\2\2\u00e9\u00ea\7#\2\2\u00ea\u00eb\7?\2\2\u00eb&\3\2\2\2\u00ec\u00ed"+
		"\7@\2\2\u00ed(\3\2\2\2\u00ee\u00ef\7>\2\2\u00ef*\3\2\2\2\u00f0\u00f1\7"+
		"@\2\2\u00f1\u00f2\7?\2\2\u00f2,\3\2\2\2\u00f3\u00f4\7>\2\2\u00f4\u00f5"+
		"\7?\2\2\u00f5.\3\2\2\2\u00f6\u00f7\7?\2\2\u00f7\u00f8\7\u0080\2\2\u00f8"+
		"\60\3\2\2\2\u00f9\u00fa\7#\2\2\u00fa\u00fb\7\u0080\2\2\u00fb\62\3\2\2"+
		"\2\u00fc\u00fd\5Q)\2\u00fd\u00fe\5k\66\2\u00fe\u00ff\5W,\2\u00ff\64\3"+
		"\2\2\2\u0100\u0101\5m\67\2\u0101\u0102\5s:\2\u0102\66\3\2\2\2\u0103\u0104"+
		"\5y=\2\u0104\u0105\5k\66\2\u0105\u0106\5g\64\2\u0106\u0107\5Y-\2\u0107"+
		"\u0108\5u;\2\u0108\u0109\5u;\2\u01098\3\2\2\2\u010a\u010b\5S*\2\u010b"+
		"\u010c\5\u0081A\2\u010c:\3\2\2\2\u010d\u010e\5}?\2\u010e\u010f\5a\61\2"+
		"\u010f\u0110\5w<\2\u0110\u0111\5_\60\2\u0111\u0112\5m\67\2\u0112\u0113"+
		"\5y=\2\u0113\u0114\5w<\2\u0114<\3\2\2\2\u0115\u0116\5m\67\2\u0116\u0117"+
		"\5k\66\2\u0117>\3\2\2\2\u0118\u0119\5a\61\2\u0119\u011a\5]/\2\u011a\u011b"+
		"\5k\66\2\u011b\u011c\5m\67\2\u011c\u011d\5s:\2\u011d\u011e\5a\61\2\u011e"+
		"\u011f\5k\66\2\u011f\u0120\5]/\2\u0120@\3\2\2\2\u0121\u0122\5]/\2\u0122"+
		"\u0123\5s:\2\u0123\u0124\5m\67\2\u0124\u0125\5y=\2\u0125\u0126\5o8\2\u0126"+
		"\u0127\7a\2\2\u0127\u0128\5g\64\2\u0128\u0129\5Y-\2\u0129\u012a\5[.\2"+
		"\u012a\u012b\5w<\2\u012bB\3\2\2\2\u012c\u012d\5]/\2\u012d\u012e\5s:\2"+
		"\u012e\u012f\5m\67\2\u012f\u0130\5y=\2\u0130\u0131\5o8\2\u0131\u0132\7"+
		"a\2\2\u0132\u0133\5s:\2\u0133\u0134\5a\61\2\u0134\u0135\5]/\2\u0135\u0136"+
		"\5_\60\2\u0136\u0137\5w<\2\u0137D\3\2\2\2\u0138\u0139\5m\67\2\u0139\u013a"+
		"\5[.\2\u013a\u013b\5[.\2\u013b\u013c\5u;\2\u013c\u013d\5Y-\2\u013d\u013e"+
		"\5w<\2\u013eF\3\2\2\2\u013f\u0140\5S*\2\u0140\u0141\5m\67\2\u0141\u0142"+
		"\5m\67\2\u0142\u0143\5g\64\2\u0143H\3\2\2\2\u0144\u0145\5u;\2\u0145\u0146"+
		"\5y=\2\u0146\u0147\5i\65\2\u0147\u0192\3\2\2\2\u0148\u0149\5i\65\2\u0149"+
		"\u014a\5a\61\2\u014a\u014b\5k\66\2\u014b\u0192\3\2\2\2\u014c\u014d\5i"+
		"\65\2\u014d\u014e\5Q)\2\u014e\u014f\5\177@\2\u014f\u0192\3\2\2\2\u0150"+
		"\u0151\5Q)\2\u0151\u0152\5{>\2\u0152\u0153\5]/\2\u0153\u0192\3\2\2\2\u0154"+
		"\u0155\5]/\2\u0155\u0156\5s:\2\u0156\u0157\5m\67\2\u0157\u0158\5y=\2\u0158"+
		"\u0159\5o8\2\u0159\u0192\3\2\2\2\u015a\u015b\5u;\2\u015b\u015c\5w<\2\u015c"+
		"\u015d\5W,\2\u015d\u015e\5W,\2\u015e\u015f\5Y-\2\u015f\u0160\5{>\2\u0160"+
		"\u0192\3\2\2\2\u0161\u0162\5u;\2\u0162\u0163\5w<\2\u0163\u0164\5W,\2\u0164"+
		"\u0165\5{>\2\u0165\u0166\5Q)\2\u0166\u0167\5s:\2\u0167\u0192\3\2\2\2\u0168"+
		"\u0169\5U+\2\u0169\u016a\5m\67\2\u016a\u016b\5y=\2\u016b\u016c\5k\66\2"+
		"\u016c\u016d\5w<\2\u016d\u0192\3\2\2\2\u016e\u016f\5U+\2\u016f\u0170\5"+
		"m\67\2\u0170\u0171\5y=\2\u0171\u0172\5k\66\2\u0172\u0173\5w<\2\u0173\u0174"+
		"\7a\2\2\u0174\u0175\5{>\2\u0175\u0176\5Q)\2\u0176\u0177\5g\64\2\u0177"+
		"\u0178\5y=\2\u0178\u0179\5Y-\2\u0179\u017a\5u;\2\u017a\u0192\3\2\2\2\u017b"+
		"\u017c\5S*\2\u017c\u017d\5m\67\2\u017d\u017e\5w<\2\u017e\u017f\5w<\2\u017f"+
		"\u0180\5m\67\2\u0180\u0181\5i\65\2\u0181\u0182\5e\63\2\u0182\u0192\3\2"+
		"\2\2\u0183\u0184\5w<\2\u0184\u0185\5m\67\2\u0185\u0186\5o8\2\u0186\u0187"+
		"\5e\63\2\u0187\u0192\3\2\2\2\u0188\u0189\5q9\2\u0189\u018a\5y=\2\u018a"+
		"\u018b\5Q)\2\u018b\u018c\5k\66\2\u018c\u018d\5w<\2\u018d\u018e\5a\61\2"+
		"\u018e\u018f\5g\64\2\u018f\u0190\5Y-\2\u0190\u0192\3\2\2\2\u0191\u0144"+
		"\3\2\2\2\u0191\u0148\3\2\2\2\u0191\u014c\3\2\2\2\u0191\u0150\3\2\2\2\u0191"+
		"\u0154\3\2\2\2\u0191\u015a\3\2\2\2\u0191\u0161\3\2\2\2\u0191\u0168\3\2"+
		"\2\2\u0191\u016e\3\2\2\2\u0191\u017b\3\2\2\2\u0191\u0183\3\2\2\2\u0191"+
		"\u0188\3\2\2\2\u0192J\3\2\2\2\u0193\u0194\5\21\t\2\u0194\u0195\t\t\2\2"+
		"\u0195L\3\2\2\2\u0196\u019a\t\n\2\2\u0197\u0199\t\13\2\2\u0198\u0197\3"+
		"\2\2\2\u0199\u019c\3\2\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b"+
		"N\3\2\2\2\u019c\u019a\3\2\2\2\u019d\u01a1\t\f\2\2\u019e\u01a0\t\r\2\2"+
		"\u019f\u019e\3\2\2\2\u01a0\u01a3\3\2\2\2\u01a1\u019f\3\2\2\2\u01a1\u01a2"+
		"\3\2\2\2\u01a2P\3\2\2\2\u01a3\u01a1\3\2\2\2\u01a4\u01a5\t\16\2\2\u01a5"+
		"R\3\2\2\2\u01a6\u01a7\t\17\2\2\u01a7T\3\2\2\2\u01a8\u01a9\t\20\2\2\u01a9"+
		"V\3\2\2\2\u01aa\u01ab\t\21\2\2\u01abX\3\2\2\2\u01ac\u01ad\t\4\2\2\u01ad"+
		"Z\3\2\2\2\u01ae\u01af\t\22\2\2\u01af\\\3\2\2\2\u01b0\u01b1\t\23\2\2\u01b1"+
		"^\3\2\2\2\u01b2\u01b3\t\24\2\2\u01b3`\3\2\2\2\u01b4\u01b5\t\25\2\2\u01b5"+
		"b\3\2\2\2\u01b6\u01b7\t\26\2\2\u01b7d\3\2\2\2\u01b8\u01b9\t\27\2\2\u01b9"+
		"f\3\2\2\2\u01ba\u01bb\t\30\2\2\u01bbh\3\2\2\2\u01bc\u01bd\t\31\2\2\u01bd"+
		"j\3\2\2\2\u01be\u01bf\t\32\2\2\u01bfl\3\2\2\2\u01c0\u01c1\t\33\2\2\u01c1"+
		"n\3\2\2\2\u01c2\u01c3\t\34\2\2\u01c3p\3\2\2\2\u01c4\u01c5\t\35\2\2\u01c5"+
		"r\3\2\2\2\u01c6\u01c7\t\36\2\2\u01c7t\3\2\2\2\u01c8\u01c9\t\37\2\2\u01c9"+
		"v\3\2\2\2\u01ca\u01cb\t \2\2\u01cbx\3\2\2\2\u01cc\u01cd\t!\2\2\u01cdz"+
		"\3\2\2\2\u01ce\u01cf\t\"\2\2\u01cf|\3\2\2\2\u01d0\u01d1\t#\2\2\u01d1~"+
		"\3\2\2\2\u01d2\u01d3\t\5\2\2\u01d3\u0080\3\2\2\2\u01d4\u01d5\t$\2\2\u01d5"+
		"\u0082\3\2\2\2\u01d6\u01d7\t%\2\2\u01d7\u0084\3\2\2\2\u01d8\u01da\t&\2"+
		"\2\u01d9\u01d8\3\2\2\2\u01da\u01db\3\2\2\2\u01db\u01d9\3\2\2\2\u01db\u01dc"+
		"\3\2\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01de\bC\2\2\u01de\u0086\3\2\2\2\u01df"+
		"\u01e3\7%\2\2\u01e0\u01e2\n\'\2\2\u01e1\u01e0\3\2\2\2\u01e2\u01e5\3\2"+
		"\2\2\u01e3\u01e1\3\2\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e6\3\2\2\2\u01e5"+
		"\u01e3\3\2\2\2\u01e6\u01e7\bD\2\2\u01e7\u0088\3\2\2\2\27\2\u0098\u009d"+
		"\u00a1\u00a6\u00aa\u00af\u00b1\u00b6\u00be\u00c0\u00c6\u00c8\u00d0\u00d2"+
		"\u00d6\u0191\u019a\u01a1\u01db\u01e3\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}