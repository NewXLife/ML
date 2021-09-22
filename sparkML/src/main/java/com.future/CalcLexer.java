package com.future;// Generated from Calc.g4 by ANTLR 4.8
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class CalcLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, NUM=6, MUL=7, DIV=8, ADD=9, SUB=10,
		POW=11, MOD=12, ID=13, ZERO=14, INT=15, FLOAT=16, COMMENT_LINE=17, COMMENT_BLOCK=18,
		WS=19;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "NUM", "MUL", "DIV", "ADD", "SUB",
			"POW", "MOD", "ID", "ZERO", "INT", "FLOAT", "COMMENT_LINE", "COMMENT_BLOCK",
			"WS"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'='", "'print'", "'('", "')'", null, "'*'", "'/'", "'+'",
			"'-'", "'^'", "'%'", null, "'0'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, "NUM", "MUL", "DIV", "ADD", "SUB",
			"POW", "MOD", "ID", "ZERO", "INT", "FLOAT", "COMMENT_LINE", "COMMENT_BLOCK",
			"WS"
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


	public CalcLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Calc.g4"; }

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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\25\u0087\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\5"+
		"\3\5\3\6\3\6\3\7\3\7\5\7:\n\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3"+
		"\f\3\r\3\r\3\16\6\16I\n\16\r\16\16\16J\3\16\7\16N\n\16\f\16\16\16Q\13"+
		"\16\3\17\3\17\3\20\3\20\7\20W\n\20\f\20\16\20Z\13\20\3\20\5\20]\n\20\3"+
		"\21\3\21\3\21\6\21b\n\21\r\21\16\21c\3\22\3\22\3\22\3\22\7\22j\n\22\f"+
		"\22\16\22m\13\22\3\22\5\22p\n\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3"+
		"\23\7\23z\n\23\f\23\16\23}\13\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3"+
		"\24\3\24\4k{\2\25\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31"+
		"\16\33\17\35\20\37\21!\22#\23%\24\'\25\3\2\7\4\2C\\c|\5\2\62;C\\c|\3\2"+
		"\63;\3\2\62;\5\2\13\f\17\17\"\"\2\u008f\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3"+
		"\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2"+
		"\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35"+
		"\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\3)"+
		"\3\2\2\2\5+\3\2\2\2\7-\3\2\2\2\t\63\3\2\2\2\13\65\3\2\2\2\r9\3\2\2\2\17"+
		";\3\2\2\2\21=\3\2\2\2\23?\3\2\2\2\25A\3\2\2\2\27C\3\2\2\2\31E\3\2\2\2"+
		"\33H\3\2\2\2\35R\3\2\2\2\37\\\3\2\2\2!^\3\2\2\2#e\3\2\2\2%u\3\2\2\2\'"+
		"\u0083\3\2\2\2)*\7=\2\2*\4\3\2\2\2+,\7?\2\2,\6\3\2\2\2-.\7r\2\2./\7t\2"+
		"\2/\60\7k\2\2\60\61\7p\2\2\61\62\7v\2\2\62\b\3\2\2\2\63\64\7*\2\2\64\n"+
		"\3\2\2\2\65\66\7+\2\2\66\f\3\2\2\2\67:\5\37\20\28:\5!\21\29\67\3\2\2\2"+
		"98\3\2\2\2:\16\3\2\2\2;<\7,\2\2<\20\3\2\2\2=>\7\61\2\2>\22\3\2\2\2?@\7"+
		"-\2\2@\24\3\2\2\2AB\7/\2\2B\26\3\2\2\2CD\7`\2\2D\30\3\2\2\2EF\7\'\2\2"+
		"F\32\3\2\2\2GI\t\2\2\2HG\3\2\2\2IJ\3\2\2\2JH\3\2\2\2JK\3\2\2\2KO\3\2\2"+
		"\2LN\t\3\2\2ML\3\2\2\2NQ\3\2\2\2OM\3\2\2\2OP\3\2\2\2P\34\3\2\2\2QO\3\2"+
		"\2\2RS\7\62\2\2S\36\3\2\2\2TX\t\4\2\2UW\t\5\2\2VU\3\2\2\2WZ\3\2\2\2XV"+
		"\3\2\2\2XY\3\2\2\2Y]\3\2\2\2ZX\3\2\2\2[]\5\35\17\2\\T\3\2\2\2\\[\3\2\2"+
		"\2] \3\2\2\2^_\5\37\20\2_a\7\60\2\2`b\t\5\2\2a`\3\2\2\2bc\3\2\2\2ca\3"+
		"\2\2\2cd\3\2\2\2d\"\3\2\2\2ef\7\61\2\2fg\7\61\2\2gk\3\2\2\2hj\13\2\2\2"+
		"ih\3\2\2\2jm\3\2\2\2kl\3\2\2\2ki\3\2\2\2lo\3\2\2\2mk\3\2\2\2np\7\17\2"+
		"\2on\3\2\2\2op\3\2\2\2pq\3\2\2\2qr\7\f\2\2rs\3\2\2\2st\b\22\2\2t$\3\2"+
		"\2\2uv\7\61\2\2vw\7,\2\2w{\3\2\2\2xz\13\2\2\2yx\3\2\2\2z}\3\2\2\2{|\3"+
		"\2\2\2{y\3\2\2\2|~\3\2\2\2}{\3\2\2\2~\177\7,\2\2\177\u0080\7\61\2\2\u0080"+
		"\u0081\3\2\2\2\u0081\u0082\b\23\2\2\u0082&\3\2\2\2\u0083\u0084\t\6\2\2"+
		"\u0084\u0085\3\2\2\2\u0085\u0086\b\24\2\2\u0086(\3\2\2\2\f\29JOX\\cko"+
		"{\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
