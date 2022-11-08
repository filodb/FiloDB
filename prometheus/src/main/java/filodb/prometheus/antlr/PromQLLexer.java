// Generated from java-escape by ANTLR 4.11.1
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class PromQLLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, NUMBER=9, 
		STRING=10, ADD=11, SUB=12, MUL=13, DIV=14, MOD=15, POW=16, EQ=17, DEQ=18, 
		NE=19, GT=20, LT=21, GE=22, LE=23, RE=24, NRE=25, AND=26, OR=27, UNLESS=28, 
		BY=29, WITHOUT=30, ON=31, IGNORING=32, GROUP_LEFT=33, GROUP_RIGHT=34, 
		OFFSET=35, LIMIT=36, BOOL=37, AGGREGATION_OP=38, DURATION=39, IDENTIFIER=40, 
		IDENTIFIER_EXTENDED=41, WS=42, COMMENT=43;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "NUMBER", 
			"STRING", "ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", 
			"GT", "LT", "GE", "LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", 
			"ON", "IGNORING", "GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "LIMIT", "BOOL", 
			"AGGREGATION_OP", "DURATION", "IDENTIFIER", "IDENTIFIER_EXTENDED", "A", 
			"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", 
			"P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "WS", "COMMENT"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'{'", "'}'", "'['", "']'", "':'", "','", null, null, 
			"'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'='", "'=='", "'!='", "'>'", 
			"'<'", "'>='", "'<='", "'=~'", "'!~'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, "NUMBER", "STRING", 
			"ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", "GT", "LT", 
			"GE", "LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", 
			"IGNORING", "GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "LIMIT", "BOOL", "AGGREGATION_OP", 
			"DURATION", "IDENTIFIER", "IDENTIFIER_EXTENDED", "WS", "COMMENT"
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
		"\u0004\u0000+\u01ff\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
		"\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
		"\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
		"\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
		"\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
		"\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
		"\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
		"\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002"+
		"\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002"+
		"\u001b\u0007\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002"+
		"\u001e\u0007\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007"+
		"!\u0002\"\u0007\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007"+
		"&\u0002\'\u0007\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007"+
		"+\u0002,\u0007,\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u0007"+
		"0\u00021\u00071\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u0007"+
		"5\u00026\u00076\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007"+
		":\u0002;\u0007;\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007"+
		"?\u0002@\u0007@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007"+
		"D\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002"+
		"\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005"+
		"\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\b\u0005\b\u009d"+
		"\b\b\n\b\f\b\u00a0\t\b\u0001\b\u0003\b\u00a3\b\b\u0001\b\u0004\b\u00a6"+
		"\b\b\u000b\b\f\b\u00a7\u0001\b\u0001\b\u0003\b\u00ac\b\b\u0001\b\u0004"+
		"\b\u00af\b\b\u000b\b\f\b\u00b0\u0003\b\u00b3\b\b\u0001\b\u0004\b\u00b6"+
		"\b\b\u000b\b\f\b\u00b7\u0001\b\u0001\b\u0001\b\u0001\b\u0004\b\u00be\b"+
		"\b\u000b\b\f\b\u00bf\u0003\b\u00c2\b\b\u0001\t\u0001\t\u0001\t\u0001\t"+
		"\u0005\t\u00c8\b\t\n\t\f\t\u00cb\t\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0005\t\u00d2\b\t\n\t\f\t\u00d5\t\t\u0001\t\u0003\t\u00d8\b\t\u0001"+
		"\n\u0001\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\r\u0001\r\u0001"+
		"\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001"+
		"\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001b\u0001"+
		"\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001"+
		"\u001c\u0001\u001c\u0001\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001"+
		"\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001 \u0001 \u0001"+
		" \u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001!\u0001"+
		"!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001\"\u0001#\u0001"+
		"#\u0001#\u0001#\u0001#\u0001#\u0001$\u0001$\u0001$\u0001$\u0001$\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0003%\u019f\b%\u0001&\u0001&\u0001&\u0004&\u01a4\b&\u000b"+
		"&\f&\u01a5\u0001\'\u0001\'\u0005\'\u01aa\b\'\n\'\f\'\u01ad\t\'\u0001("+
		"\u0005(\u01b0\b(\n(\f(\u01b3\t(\u0001(\u0001(\u0005(\u01b7\b(\n(\f(\u01ba"+
		"\t(\u0001)\u0001)\u0001*\u0001*\u0001+\u0001+\u0001,\u0001,\u0001-\u0001"+
		"-\u0001.\u0001.\u0001/\u0001/\u00010\u00010\u00011\u00011\u00012\u0001"+
		"2\u00013\u00013\u00014\u00014\u00015\u00015\u00016\u00016\u00017\u0001"+
		"7\u00018\u00018\u00019\u00019\u0001:\u0001:\u0001;\u0001;\u0001<\u0001"+
		"<\u0001=\u0001=\u0001>\u0001>\u0001?\u0001?\u0001@\u0001@\u0001A\u0001"+
		"A\u0001B\u0001B\u0001C\u0004C\u01f1\bC\u000bC\fC\u01f2\u0001C\u0001C\u0001"+
		"D\u0001D\u0005D\u01f9\bD\nD\fD\u01fc\tD\u0001D\u0001D\u0000\u0000E\u0001"+
		"\u0001\u0003\u0002\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007"+
		"\u000f\b\u0011\t\u0013\n\u0015\u000b\u0017\f\u0019\r\u001b\u000e\u001d"+
		"\u000f\u001f\u0010!\u0011#\u0012%\u0013\'\u0014)\u0015+\u0016-\u0017/"+
		"\u00181\u00193\u001a5\u001b7\u001c9\u001d;\u001e=\u001f? A!C\"E#G$I%K"+
		"&M\'O(Q)S\u0000U\u0000W\u0000Y\u0000[\u0000]\u0000_\u0000a\u0000c\u0000"+
		"e\u0000g\u0000i\u0000k\u0000m\u0000o\u0000q\u0000s\u0000u\u0000w\u0000"+
		"y\u0000{\u0000}\u0000\u007f\u0000\u0081\u0000\u0083\u0000\u0085\u0000"+
		"\u0087*\u0089+\u0001\u0000\'\u0001\u000009\u0002\u0000EEee\u0002\u0000"+
		"++--\u0002\u0000XXxx\u0003\u000009AFaf\u0002\u0000\'\'\\\\\u0002\u0000"+
		"\"\"\\\\\u0006\u0000ddhimmsswwyy\u0003\u0000AZ__az\u0004\u000009AZ__a"+
		"z\u0002\u0000::__\u0002\u0000AZaz\u0005\u0000-.0:AZ__az\u0002\u0000AA"+
		"aa\u0002\u0000BBbb\u0002\u0000CCcc\u0002\u0000DDdd\u0002\u0000FFff\u0002"+
		"\u0000GGgg\u0002\u0000HHhh\u0002\u0000IIii\u0002\u0000JJjj\u0002\u0000"+
		"KKkk\u0002\u0000LLll\u0002\u0000MMmm\u0002\u0000NNnn\u0002\u0000OOoo\u0002"+
		"\u0000PPpp\u0002\u0000QQqq\u0002\u0000RRrr\u0002\u0000SSss\u0002\u0000"+
		"TTtt\u0002\u0000UUuu\u0002\u0000VVvv\u0002\u0000WWww\u0002\u0000YYyy\u0002"+
		"\u0000ZZzz\u0003\u0000\t\n\r\r  \u0002\u0000\n\n\r\r\u0205\u0000\u0001"+
		"\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000\u0000\u0000\u0000\u0005"+
		"\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000\u0000\u0000\u0000\t\u0001"+
		"\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000\u0000\u0000\r\u0001\u0000"+
		"\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000\u0000\u0011\u0001\u0000"+
		"\u0000\u0000\u0000\u0013\u0001\u0000\u0000\u0000\u0000\u0015\u0001\u0000"+
		"\u0000\u0000\u0000\u0017\u0001\u0000\u0000\u0000\u0000\u0019\u0001\u0000"+
		"\u0000\u0000\u0000\u001b\u0001\u0000\u0000\u0000\u0000\u001d\u0001\u0000"+
		"\u0000\u0000\u0000\u001f\u0001\u0000\u0000\u0000\u0000!\u0001\u0000\u0000"+
		"\u0000\u0000#\u0001\u0000\u0000\u0000\u0000%\u0001\u0000\u0000\u0000\u0000"+
		"\'\u0001\u0000\u0000\u0000\u0000)\u0001\u0000\u0000\u0000\u0000+\u0001"+
		"\u0000\u0000\u0000\u0000-\u0001\u0000\u0000\u0000\u0000/\u0001\u0000\u0000"+
		"\u0000\u00001\u0001\u0000\u0000\u0000\u00003\u0001\u0000\u0000\u0000\u0000"+
		"5\u0001\u0000\u0000\u0000\u00007\u0001\u0000\u0000\u0000\u00009\u0001"+
		"\u0000\u0000\u0000\u0000;\u0001\u0000\u0000\u0000\u0000=\u0001\u0000\u0000"+
		"\u0000\u0000?\u0001\u0000\u0000\u0000\u0000A\u0001\u0000\u0000\u0000\u0000"+
		"C\u0001\u0000\u0000\u0000\u0000E\u0001\u0000\u0000\u0000\u0000G\u0001"+
		"\u0000\u0000\u0000\u0000I\u0001\u0000\u0000\u0000\u0000K\u0001\u0000\u0000"+
		"\u0000\u0000M\u0001\u0000\u0000\u0000\u0000O\u0001\u0000\u0000\u0000\u0000"+
		"Q\u0001\u0000\u0000\u0000\u0000\u0087\u0001\u0000\u0000\u0000\u0000\u0089"+
		"\u0001\u0000\u0000\u0000\u0001\u008b\u0001\u0000\u0000\u0000\u0003\u008d"+
		"\u0001\u0000\u0000\u0000\u0005\u008f\u0001\u0000\u0000\u0000\u0007\u0091"+
		"\u0001\u0000\u0000\u0000\t\u0093\u0001\u0000\u0000\u0000\u000b\u0095\u0001"+
		"\u0000\u0000\u0000\r\u0097\u0001\u0000\u0000\u0000\u000f\u0099\u0001\u0000"+
		"\u0000\u0000\u0011\u00c1\u0001\u0000\u0000\u0000\u0013\u00d7\u0001\u0000"+
		"\u0000\u0000\u0015\u00d9\u0001\u0000\u0000\u0000\u0017\u00db\u0001\u0000"+
		"\u0000\u0000\u0019\u00dd\u0001\u0000\u0000\u0000\u001b\u00df\u0001\u0000"+
		"\u0000\u0000\u001d\u00e1\u0001\u0000\u0000\u0000\u001f\u00e3\u0001\u0000"+
		"\u0000\u0000!\u00e5\u0001\u0000\u0000\u0000#\u00e7\u0001\u0000\u0000\u0000"+
		"%\u00ea\u0001\u0000\u0000\u0000\'\u00ed\u0001\u0000\u0000\u0000)\u00ef"+
		"\u0001\u0000\u0000\u0000+\u00f1\u0001\u0000\u0000\u0000-\u00f4\u0001\u0000"+
		"\u0000\u0000/\u00f7\u0001\u0000\u0000\u00001\u00fa\u0001\u0000\u0000\u0000"+
		"3\u00fd\u0001\u0000\u0000\u00005\u0101\u0001\u0000\u0000\u00007\u0104"+
		"\u0001\u0000\u0000\u00009\u010b\u0001\u0000\u0000\u0000;\u010e\u0001\u0000"+
		"\u0000\u0000=\u0116\u0001\u0000\u0000\u0000?\u0119\u0001\u0000\u0000\u0000"+
		"A\u0122\u0001\u0000\u0000\u0000C\u012d\u0001\u0000\u0000\u0000E\u0139"+
		"\u0001\u0000\u0000\u0000G\u0140\u0001\u0000\u0000\u0000I\u0146\u0001\u0000"+
		"\u0000\u0000K\u019e\u0001\u0000\u0000\u0000M\u01a3\u0001\u0000\u0000\u0000"+
		"O\u01a7\u0001\u0000\u0000\u0000Q\u01b1\u0001\u0000\u0000\u0000S\u01bb"+
		"\u0001\u0000\u0000\u0000U\u01bd\u0001\u0000\u0000\u0000W\u01bf\u0001\u0000"+
		"\u0000\u0000Y\u01c1\u0001\u0000\u0000\u0000[\u01c3\u0001\u0000\u0000\u0000"+
		"]\u01c5\u0001\u0000\u0000\u0000_\u01c7\u0001\u0000\u0000\u0000a\u01c9"+
		"\u0001\u0000\u0000\u0000c\u01cb\u0001\u0000\u0000\u0000e\u01cd\u0001\u0000"+
		"\u0000\u0000g\u01cf\u0001\u0000\u0000\u0000i\u01d1\u0001\u0000\u0000\u0000"+
		"k\u01d3\u0001\u0000\u0000\u0000m\u01d5\u0001\u0000\u0000\u0000o\u01d7"+
		"\u0001\u0000\u0000\u0000q\u01d9\u0001\u0000\u0000\u0000s\u01db\u0001\u0000"+
		"\u0000\u0000u\u01dd\u0001\u0000\u0000\u0000w\u01df\u0001\u0000\u0000\u0000"+
		"y\u01e1\u0001\u0000\u0000\u0000{\u01e3\u0001\u0000\u0000\u0000}\u01e5"+
		"\u0001\u0000\u0000\u0000\u007f\u01e7\u0001\u0000\u0000\u0000\u0081\u01e9"+
		"\u0001\u0000\u0000\u0000\u0083\u01eb\u0001\u0000\u0000\u0000\u0085\u01ed"+
		"\u0001\u0000\u0000\u0000\u0087\u01f0\u0001\u0000\u0000\u0000\u0089\u01f6"+
		"\u0001\u0000\u0000\u0000\u008b\u008c\u0005(\u0000\u0000\u008c\u0002\u0001"+
		"\u0000\u0000\u0000\u008d\u008e\u0005)\u0000\u0000\u008e\u0004\u0001\u0000"+
		"\u0000\u0000\u008f\u0090\u0005{\u0000\u0000\u0090\u0006\u0001\u0000\u0000"+
		"\u0000\u0091\u0092\u0005}\u0000\u0000\u0092\b\u0001\u0000\u0000\u0000"+
		"\u0093\u0094\u0005[\u0000\u0000\u0094\n\u0001\u0000\u0000\u0000\u0095"+
		"\u0096\u0005]\u0000\u0000\u0096\f\u0001\u0000\u0000\u0000\u0097\u0098"+
		"\u0005:\u0000\u0000\u0098\u000e\u0001\u0000\u0000\u0000\u0099\u009a\u0005"+
		",\u0000\u0000\u009a\u0010\u0001\u0000\u0000\u0000\u009b\u009d\u0007\u0000"+
		"\u0000\u0000\u009c\u009b\u0001\u0000\u0000\u0000\u009d\u00a0\u0001\u0000"+
		"\u0000\u0000\u009e\u009c\u0001\u0000\u0000\u0000\u009e\u009f\u0001\u0000"+
		"\u0000\u0000\u009f\u00a2\u0001\u0000\u0000\u0000\u00a0\u009e\u0001\u0000"+
		"\u0000\u0000\u00a1\u00a3\u0005.\u0000\u0000\u00a2\u00a1\u0001\u0000\u0000"+
		"\u0000\u00a2\u00a3\u0001\u0000\u0000\u0000\u00a3\u00a5\u0001\u0000\u0000"+
		"\u0000\u00a4\u00a6\u0007\u0000\u0000\u0000\u00a5\u00a4\u0001\u0000\u0000"+
		"\u0000\u00a6\u00a7\u0001\u0000\u0000\u0000\u00a7\u00a5\u0001\u0000\u0000"+
		"\u0000\u00a7\u00a8\u0001\u0000\u0000\u0000\u00a8\u00b2\u0001\u0000\u0000"+
		"\u0000\u00a9\u00ab\u0007\u0001\u0000\u0000\u00aa\u00ac\u0007\u0002\u0000"+
		"\u0000\u00ab\u00aa\u0001\u0000\u0000\u0000\u00ab\u00ac\u0001\u0000\u0000"+
		"\u0000\u00ac\u00ae\u0001\u0000\u0000\u0000\u00ad\u00af\u0007\u0000\u0000"+
		"\u0000\u00ae\u00ad\u0001\u0000\u0000\u0000\u00af\u00b0\u0001\u0000\u0000"+
		"\u0000\u00b0\u00ae\u0001\u0000\u0000\u0000\u00b0\u00b1\u0001\u0000\u0000"+
		"\u0000\u00b1\u00b3\u0001\u0000\u0000\u0000\u00b2\u00a9\u0001\u0000\u0000"+
		"\u0000\u00b2\u00b3\u0001\u0000\u0000\u0000\u00b3\u00c2\u0001\u0000\u0000"+
		"\u0000\u00b4\u00b6\u0007\u0000\u0000\u0000\u00b5\u00b4\u0001\u0000\u0000"+
		"\u0000\u00b6\u00b7\u0001\u0000\u0000\u0000\u00b7\u00b5\u0001\u0000\u0000"+
		"\u0000\u00b7\u00b8\u0001\u0000\u0000\u0000\u00b8\u00b9\u0001\u0000\u0000"+
		"\u0000\u00b9\u00c2\u0005.\u0000\u0000\u00ba\u00bb\u00050\u0000\u0000\u00bb"+
		"\u00bd\u0007\u0003\u0000\u0000\u00bc\u00be\u0007\u0004\u0000\u0000\u00bd"+
		"\u00bc\u0001\u0000\u0000\u0000\u00be\u00bf\u0001\u0000\u0000\u0000\u00bf"+
		"\u00bd\u0001\u0000\u0000\u0000\u00bf\u00c0\u0001\u0000\u0000\u0000\u00c0"+
		"\u00c2\u0001\u0000\u0000\u0000\u00c1\u009e\u0001\u0000\u0000\u0000\u00c1"+
		"\u00b5\u0001\u0000\u0000\u0000\u00c1\u00ba\u0001\u0000\u0000\u0000\u00c2"+
		"\u0012\u0001\u0000\u0000\u0000\u00c3\u00c9\u0005\'\u0000\u0000\u00c4\u00c8"+
		"\b\u0005\u0000\u0000\u00c5\u00c6\u0005\\\u0000\u0000\u00c6\u00c8\t\u0000"+
		"\u0000\u0000\u00c7\u00c4\u0001\u0000\u0000\u0000\u00c7\u00c5\u0001\u0000"+
		"\u0000\u0000\u00c8\u00cb\u0001\u0000\u0000\u0000\u00c9\u00c7\u0001\u0000"+
		"\u0000\u0000\u00c9\u00ca\u0001\u0000\u0000\u0000\u00ca\u00cc\u0001\u0000"+
		"\u0000\u0000\u00cb\u00c9\u0001\u0000\u0000\u0000\u00cc\u00d8\u0005\'\u0000"+
		"\u0000\u00cd\u00d3\u0005\"\u0000\u0000\u00ce\u00d2\b\u0006\u0000\u0000"+
		"\u00cf\u00d0\u0005\\\u0000\u0000\u00d0\u00d2\t\u0000\u0000\u0000\u00d1"+
		"\u00ce\u0001\u0000\u0000\u0000\u00d1\u00cf\u0001\u0000\u0000\u0000\u00d2"+
		"\u00d5\u0001\u0000\u0000\u0000\u00d3\u00d1\u0001\u0000\u0000\u0000\u00d3"+
		"\u00d4\u0001\u0000\u0000\u0000\u00d4\u00d6\u0001\u0000\u0000\u0000\u00d5"+
		"\u00d3\u0001\u0000\u0000\u0000\u00d6\u00d8\u0005\"\u0000\u0000\u00d7\u00c3"+
		"\u0001\u0000\u0000\u0000\u00d7\u00cd\u0001\u0000\u0000\u0000\u00d8\u0014"+
		"\u0001\u0000\u0000\u0000\u00d9\u00da\u0005+\u0000\u0000\u00da\u0016\u0001"+
		"\u0000\u0000\u0000\u00db\u00dc\u0005-\u0000\u0000\u00dc\u0018\u0001\u0000"+
		"\u0000\u0000\u00dd\u00de\u0005*\u0000\u0000\u00de\u001a\u0001\u0000\u0000"+
		"\u0000\u00df\u00e0\u0005/\u0000\u0000\u00e0\u001c\u0001\u0000\u0000\u0000"+
		"\u00e1\u00e2\u0005%\u0000\u0000\u00e2\u001e\u0001\u0000\u0000\u0000\u00e3"+
		"\u00e4\u0005^\u0000\u0000\u00e4 \u0001\u0000\u0000\u0000\u00e5\u00e6\u0005"+
		"=\u0000\u0000\u00e6\"\u0001\u0000\u0000\u0000\u00e7\u00e8\u0005=\u0000"+
		"\u0000\u00e8\u00e9\u0005=\u0000\u0000\u00e9$\u0001\u0000\u0000\u0000\u00ea"+
		"\u00eb\u0005!\u0000\u0000\u00eb\u00ec\u0005=\u0000\u0000\u00ec&\u0001"+
		"\u0000\u0000\u0000\u00ed\u00ee\u0005>\u0000\u0000\u00ee(\u0001\u0000\u0000"+
		"\u0000\u00ef\u00f0\u0005<\u0000\u0000\u00f0*\u0001\u0000\u0000\u0000\u00f1"+
		"\u00f2\u0005>\u0000\u0000\u00f2\u00f3\u0005=\u0000\u0000\u00f3,\u0001"+
		"\u0000\u0000\u0000\u00f4\u00f5\u0005<\u0000\u0000\u00f5\u00f6\u0005=\u0000"+
		"\u0000\u00f6.\u0001\u0000\u0000\u0000\u00f7\u00f8\u0005=\u0000\u0000\u00f8"+
		"\u00f9\u0005~\u0000\u0000\u00f90\u0001\u0000\u0000\u0000\u00fa\u00fb\u0005"+
		"!\u0000\u0000\u00fb\u00fc\u0005~\u0000\u0000\u00fc2\u0001\u0000\u0000"+
		"\u0000\u00fd\u00fe\u0003S)\u0000\u00fe\u00ff\u0003m6\u0000\u00ff\u0100"+
		"\u0003Y,\u0000\u01004\u0001\u0000\u0000\u0000\u0101\u0102\u0003o7\u0000"+
		"\u0102\u0103\u0003u:\u0000\u01036\u0001\u0000\u0000\u0000\u0104\u0105"+
		"\u0003{=\u0000\u0105\u0106\u0003m6\u0000\u0106\u0107\u0003i4\u0000\u0107"+
		"\u0108\u0003[-\u0000\u0108\u0109\u0003w;\u0000\u0109\u010a\u0003w;\u0000"+
		"\u010a8\u0001\u0000\u0000\u0000\u010b\u010c\u0003U*\u0000\u010c\u010d"+
		"\u0003\u0083A\u0000\u010d:\u0001\u0000\u0000\u0000\u010e\u010f\u0003\u007f"+
		"?\u0000\u010f\u0110\u0003c1\u0000\u0110\u0111\u0003y<\u0000\u0111\u0112"+
		"\u0003a0\u0000\u0112\u0113\u0003o7\u0000\u0113\u0114\u0003{=\u0000\u0114"+
		"\u0115\u0003y<\u0000\u0115<\u0001\u0000\u0000\u0000\u0116\u0117\u0003"+
		"o7\u0000\u0117\u0118\u0003m6\u0000\u0118>\u0001\u0000\u0000\u0000\u0119"+
		"\u011a\u0003c1\u0000\u011a\u011b\u0003_/\u0000\u011b\u011c\u0003m6\u0000"+
		"\u011c\u011d\u0003o7\u0000\u011d\u011e\u0003u:\u0000\u011e\u011f\u0003"+
		"c1\u0000\u011f\u0120\u0003m6\u0000\u0120\u0121\u0003_/\u0000\u0121@\u0001"+
		"\u0000\u0000\u0000\u0122\u0123\u0003_/\u0000\u0123\u0124\u0003u:\u0000"+
		"\u0124\u0125\u0003o7\u0000\u0125\u0126\u0003{=\u0000\u0126\u0127\u0003"+
		"q8\u0000\u0127\u0128\u0005_\u0000\u0000\u0128\u0129\u0003i4\u0000\u0129"+
		"\u012a\u0003[-\u0000\u012a\u012b\u0003].\u0000\u012b\u012c\u0003y<\u0000"+
		"\u012cB\u0001\u0000\u0000\u0000\u012d\u012e\u0003_/\u0000\u012e\u012f"+
		"\u0003u:\u0000\u012f\u0130\u0003o7\u0000\u0130\u0131\u0003{=\u0000\u0131"+
		"\u0132\u0003q8\u0000\u0132\u0133\u0005_\u0000\u0000\u0133\u0134\u0003"+
		"u:\u0000\u0134\u0135\u0003c1\u0000\u0135\u0136\u0003_/\u0000\u0136\u0137"+
		"\u0003a0\u0000\u0137\u0138\u0003y<\u0000\u0138D\u0001\u0000\u0000\u0000"+
		"\u0139\u013a\u0003o7\u0000\u013a\u013b\u0003].\u0000\u013b\u013c\u0003"+
		"].\u0000\u013c\u013d\u0003w;\u0000\u013d\u013e\u0003[-\u0000\u013e\u013f"+
		"\u0003y<\u0000\u013fF\u0001\u0000\u0000\u0000\u0140\u0141\u0003i4\u0000"+
		"\u0141\u0142\u0003c1\u0000\u0142\u0143\u0003k5\u0000\u0143\u0144\u0003"+
		"c1\u0000\u0144\u0145\u0003y<\u0000\u0145H\u0001\u0000\u0000\u0000\u0146"+
		"\u0147\u0003U*\u0000\u0147\u0148\u0003o7\u0000\u0148\u0149\u0003o7\u0000"+
		"\u0149\u014a\u0003i4\u0000\u014aJ\u0001\u0000\u0000\u0000\u014b\u014c"+
		"\u0003w;\u0000\u014c\u014d\u0003{=\u0000\u014d\u014e\u0003k5\u0000\u014e"+
		"\u019f\u0001\u0000\u0000\u0000\u014f\u0150\u0003k5\u0000\u0150\u0151\u0003"+
		"c1\u0000\u0151\u0152\u0003m6\u0000\u0152\u019f\u0001\u0000\u0000\u0000"+
		"\u0153\u0154\u0003k5\u0000\u0154\u0155\u0003S)\u0000\u0155\u0156\u0003"+
		"\u0081@\u0000\u0156\u019f\u0001\u0000\u0000\u0000\u0157\u0158\u0003S)"+
		"\u0000\u0158\u0159\u0003}>\u0000\u0159\u015a\u0003_/\u0000\u015a\u019f"+
		"\u0001\u0000\u0000\u0000\u015b\u015c\u0003_/\u0000\u015c\u015d\u0003u"+
		":\u0000\u015d\u015e\u0003o7\u0000\u015e\u015f\u0003{=\u0000\u015f\u0160"+
		"\u0003q8\u0000\u0160\u019f\u0001\u0000\u0000\u0000\u0161\u0162\u0003w"+
		";\u0000\u0162\u0163\u0003y<\u0000\u0163\u0164\u0003Y,\u0000\u0164\u0165"+
		"\u0003Y,\u0000\u0165\u0166\u0003[-\u0000\u0166\u0167\u0003}>\u0000\u0167"+
		"\u019f\u0001\u0000\u0000\u0000\u0168\u0169\u0003w;\u0000\u0169\u016a\u0003"+
		"y<\u0000\u016a\u016b\u0003Y,\u0000\u016b\u016c\u0003}>\u0000\u016c\u016d"+
		"\u0003S)\u0000\u016d\u016e\u0003u:\u0000\u016e\u019f\u0001\u0000\u0000"+
		"\u0000\u016f\u0170\u0003W+\u0000\u0170\u0171\u0003o7\u0000\u0171\u0172"+
		"\u0003{=\u0000\u0172\u0173\u0003m6\u0000\u0173\u0174\u0003y<\u0000\u0174"+
		"\u019f\u0001\u0000\u0000\u0000\u0175\u0176\u0003W+\u0000\u0176\u0177\u0003"+
		"o7\u0000\u0177\u0178\u0003{=\u0000\u0178\u0179\u0003m6\u0000\u0179\u017a"+
		"\u0003y<\u0000\u017a\u017b\u0005_\u0000\u0000\u017b\u017c\u0003}>\u0000"+
		"\u017c\u017d\u0003S)\u0000\u017d\u017e\u0003i4\u0000\u017e\u017f\u0003"+
		"{=\u0000\u017f\u0180\u0003[-\u0000\u0180\u0181\u0003w;\u0000\u0181\u019f"+
		"\u0001\u0000\u0000\u0000\u0182\u0183\u0003U*\u0000\u0183\u0184\u0003o"+
		"7\u0000\u0184\u0185\u0003y<\u0000\u0185\u0186\u0003y<\u0000\u0186\u0187"+
		"\u0003o7\u0000\u0187\u0188\u0003k5\u0000\u0188\u0189\u0003g3\u0000\u0189"+
		"\u019f\u0001\u0000\u0000\u0000\u018a\u018b\u0003y<\u0000\u018b\u018c\u0003"+
		"o7\u0000\u018c\u018d\u0003q8\u0000\u018d\u018e\u0003g3\u0000\u018e\u019f"+
		"\u0001\u0000\u0000\u0000\u018f\u0190\u0003s9\u0000\u0190\u0191\u0003{"+
		"=\u0000\u0191\u0192\u0003S)\u0000\u0192\u0193\u0003m6\u0000\u0193\u0194"+
		"\u0003y<\u0000\u0194\u0195\u0003c1\u0000\u0195\u0196\u0003i4\u0000\u0196"+
		"\u0197\u0003[-\u0000\u0197\u019f\u0001\u0000\u0000\u0000\u0198\u0199\u0003"+
		"_/\u0000\u0199\u019a\u0003u:\u0000\u019a\u019b\u0003o7\u0000\u019b\u019c"+
		"\u0003{=\u0000\u019c\u019d\u0003q8\u0000\u019d\u019f\u0001\u0000\u0000"+
		"\u0000\u019e\u014b\u0001\u0000\u0000\u0000\u019e\u014f\u0001\u0000\u0000"+
		"\u0000\u019e\u0153\u0001\u0000\u0000\u0000\u019e\u0157\u0001\u0000\u0000"+
		"\u0000\u019e\u015b\u0001\u0000\u0000\u0000\u019e\u0161\u0001\u0000\u0000"+
		"\u0000\u019e\u0168\u0001\u0000\u0000\u0000\u019e\u016f\u0001\u0000\u0000"+
		"\u0000\u019e\u0175\u0001\u0000\u0000\u0000\u019e\u0182\u0001\u0000\u0000"+
		"\u0000\u019e\u018a\u0001\u0000\u0000\u0000\u019e\u018f\u0001\u0000\u0000"+
		"\u0000\u019e\u0198\u0001\u0000\u0000\u0000\u019fL\u0001\u0000\u0000\u0000"+
		"\u01a0\u01a1\u0003\u0011\b\u0000\u01a1\u01a2\u0007\u0007\u0000\u0000\u01a2"+
		"\u01a4\u0001\u0000\u0000\u0000\u01a3\u01a0\u0001\u0000\u0000\u0000\u01a4"+
		"\u01a5\u0001\u0000\u0000\u0000\u01a5\u01a3\u0001\u0000\u0000\u0000\u01a5"+
		"\u01a6\u0001\u0000\u0000\u0000\u01a6N\u0001\u0000\u0000\u0000\u01a7\u01ab"+
		"\u0007\b\u0000\u0000\u01a8\u01aa\u0007\t\u0000\u0000\u01a9\u01a8\u0001"+
		"\u0000\u0000\u0000\u01aa\u01ad\u0001\u0000\u0000\u0000\u01ab\u01a9\u0001"+
		"\u0000\u0000\u0000\u01ab\u01ac\u0001\u0000\u0000\u0000\u01acP\u0001\u0000"+
		"\u0000\u0000\u01ad\u01ab\u0001\u0000\u0000\u0000\u01ae\u01b0\u0007\n\u0000"+
		"\u0000\u01af\u01ae\u0001\u0000\u0000\u0000\u01b0\u01b3\u0001\u0000\u0000"+
		"\u0000\u01b1\u01af\u0001\u0000\u0000\u0000\u01b1\u01b2\u0001\u0000\u0000"+
		"\u0000\u01b2\u01b4\u0001\u0000\u0000\u0000\u01b3\u01b1\u0001\u0000\u0000"+
		"\u0000\u01b4\u01b8\u0007\u000b\u0000\u0000\u01b5\u01b7\u0007\f\u0000\u0000"+
		"\u01b6\u01b5\u0001\u0000\u0000\u0000\u01b7\u01ba\u0001\u0000\u0000\u0000"+
		"\u01b8\u01b6\u0001\u0000\u0000\u0000\u01b8\u01b9\u0001\u0000\u0000\u0000"+
		"\u01b9R\u0001\u0000\u0000\u0000\u01ba\u01b8\u0001\u0000\u0000\u0000\u01bb"+
		"\u01bc\u0007\r\u0000\u0000\u01bcT\u0001\u0000\u0000\u0000\u01bd\u01be"+
		"\u0007\u000e\u0000\u0000\u01beV\u0001\u0000\u0000\u0000\u01bf\u01c0\u0007"+
		"\u000f\u0000\u0000\u01c0X\u0001\u0000\u0000\u0000\u01c1\u01c2\u0007\u0010"+
		"\u0000\u0000\u01c2Z\u0001\u0000\u0000\u0000\u01c3\u01c4\u0007\u0001\u0000"+
		"\u0000\u01c4\\\u0001\u0000\u0000\u0000\u01c5\u01c6\u0007\u0011\u0000\u0000"+
		"\u01c6^\u0001\u0000\u0000\u0000\u01c7\u01c8\u0007\u0012\u0000\u0000\u01c8"+
		"`\u0001\u0000\u0000\u0000\u01c9\u01ca\u0007\u0013\u0000\u0000\u01cab\u0001"+
		"\u0000\u0000\u0000\u01cb\u01cc\u0007\u0014\u0000\u0000\u01ccd\u0001\u0000"+
		"\u0000\u0000\u01cd\u01ce\u0007\u0015\u0000\u0000\u01cef\u0001\u0000\u0000"+
		"\u0000\u01cf\u01d0\u0007\u0016\u0000\u0000\u01d0h\u0001\u0000\u0000\u0000"+
		"\u01d1\u01d2\u0007\u0017\u0000\u0000\u01d2j\u0001\u0000\u0000\u0000\u01d3"+
		"\u01d4\u0007\u0018\u0000\u0000\u01d4l\u0001\u0000\u0000\u0000\u01d5\u01d6"+
		"\u0007\u0019\u0000\u0000\u01d6n\u0001\u0000\u0000\u0000\u01d7\u01d8\u0007"+
		"\u001a\u0000\u0000\u01d8p\u0001\u0000\u0000\u0000\u01d9\u01da\u0007\u001b"+
		"\u0000\u0000\u01dar\u0001\u0000\u0000\u0000\u01db\u01dc\u0007\u001c\u0000"+
		"\u0000\u01dct\u0001\u0000\u0000\u0000\u01dd\u01de\u0007\u001d\u0000\u0000"+
		"\u01dev\u0001\u0000\u0000\u0000\u01df\u01e0\u0007\u001e\u0000\u0000\u01e0"+
		"x\u0001\u0000\u0000\u0000\u01e1\u01e2\u0007\u001f\u0000\u0000\u01e2z\u0001"+
		"\u0000\u0000\u0000\u01e3\u01e4\u0007 \u0000\u0000\u01e4|\u0001\u0000\u0000"+
		"\u0000\u01e5\u01e6\u0007!\u0000\u0000\u01e6~\u0001\u0000\u0000\u0000\u01e7"+
		"\u01e8\u0007\"\u0000\u0000\u01e8\u0080\u0001\u0000\u0000\u0000\u01e9\u01ea"+
		"\u0007\u0003\u0000\u0000\u01ea\u0082\u0001\u0000\u0000\u0000\u01eb\u01ec"+
		"\u0007#\u0000\u0000\u01ec\u0084\u0001\u0000\u0000\u0000\u01ed\u01ee\u0007"+
		"$\u0000\u0000\u01ee\u0086\u0001\u0000\u0000\u0000\u01ef\u01f1\u0007%\u0000"+
		"\u0000\u01f0\u01ef\u0001\u0000\u0000\u0000\u01f1\u01f2\u0001\u0000\u0000"+
		"\u0000\u01f2\u01f0\u0001\u0000\u0000\u0000\u01f2\u01f3\u0001\u0000\u0000"+
		"\u0000\u01f3\u01f4\u0001\u0000\u0000\u0000\u01f4\u01f5\u0006C\u0000\u0000"+
		"\u01f5\u0088\u0001\u0000\u0000\u0000\u01f6\u01fa\u0005#\u0000\u0000\u01f7"+
		"\u01f9\b&\u0000\u0000\u01f8\u01f7\u0001\u0000\u0000\u0000\u01f9\u01fc"+
		"\u0001\u0000\u0000\u0000\u01fa\u01f8\u0001\u0000\u0000\u0000\u01fa\u01fb"+
		"\u0001\u0000\u0000\u0000\u01fb\u01fd\u0001\u0000\u0000\u0000\u01fc\u01fa"+
		"\u0001\u0000\u0000\u0000\u01fd\u01fe\u0006D\u0000\u0000\u01fe\u008a\u0001"+
		"\u0000\u0000\u0000\u0016\u0000\u009e\u00a2\u00a7\u00ab\u00b0\u00b2\u00b7"+
		"\u00bf\u00c1\u00c7\u00c9\u00d1\u00d3\u00d7\u019e\u01a5\u01ab\u01b1\u01b8"+
		"\u01f2\u01fa\u0001\u0006\u0000\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}