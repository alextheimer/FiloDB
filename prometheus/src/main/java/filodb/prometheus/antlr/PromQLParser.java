// Generated from java-escape by ANTLR 4.11.1
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class PromQLParser extends Parser {
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
	public static final int
		RULE_expression = 0, RULE_vectorExpression = 1, RULE_unaryOp = 2, RULE_powOp = 3, 
		RULE_multOp = 4, RULE_addOp = 5, RULE_compareOp = 6, RULE_andUnlessOp = 7, 
		RULE_orOp = 8, RULE_vector = 9, RULE_parens = 10, RULE_instantOrRangeSelector = 11, 
		RULE_instantSelector = 12, RULE_window = 13, RULE_offset = 14, RULE_limit = 15, 
		RULE_subquery = 16, RULE_labelMatcher = 17, RULE_labelMatcherOp = 18, 
		RULE_labelMatcherList = 19, RULE_function = 20, RULE_parameter = 21, RULE_parameterList = 22, 
		RULE_aggregation = 23, RULE_by = 24, RULE_without = 25, RULE_grouping = 26, 
		RULE_on = 27, RULE_ignoring = 28, RULE_groupLeft = 29, RULE_groupRight = 30, 
		RULE_metricName = 31, RULE_metricKeyword = 32, RULE_labelName = 33, RULE_labelNameList = 34, 
		RULE_labelKeyword = 35, RULE_literal = 36;
	private static String[] makeRuleNames() {
		return new String[] {
			"expression", "vectorExpression", "unaryOp", "powOp", "multOp", "addOp", 
			"compareOp", "andUnlessOp", "orOp", "vector", "parens", "instantOrRangeSelector", 
			"instantSelector", "window", "offset", "limit", "subquery", "labelMatcher", 
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

	@Override
	public String getGrammarFileName() { return "java-escape"; }

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

	@SuppressWarnings("CheckReturnValue")
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
			setState(74);
			vectorExpression(0);
			setState(75);
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

	@SuppressWarnings("CheckReturnValue")
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
	@SuppressWarnings("CheckReturnValue")
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
	@SuppressWarnings("CheckReturnValue")
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
	@SuppressWarnings("CheckReturnValue")
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
	@SuppressWarnings("CheckReturnValue")
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
	@SuppressWarnings("CheckReturnValue")
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
			setState(82);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
			case SUB:
				{
				_localctx = new UnaryOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(78);
				unaryOp();
				setState(79);
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
				setState(81);
				vector();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(135);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(133);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(84);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(85);
						powOp();
						setState(87);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(86);
							grouping();
							}
						}

						setState(89);
						vectorExpression(10);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(91);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(92);
						multOp();
						setState(94);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(93);
							grouping();
							}
						}

						setState(96);
						vectorExpression(9);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(98);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(99);
						addOp();
						setState(101);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(100);
							grouping();
							}
						}

						setState(103);
						vectorExpression(8);
						}
						break;
					case 4:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(105);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(106);
						compareOp();
						setState(108);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(107);
							grouping();
							}
						}

						setState(110);
						vectorExpression(7);
						}
						break;
					case 5:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(112);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(113);
						andUnlessOp();
						setState(115);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(114);
							grouping();
							}
						}

						setState(117);
						vectorExpression(6);
						}
						break;
					case 6:
						{
						_localctx = new BinaryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(119);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(120);
						orOp();
						setState(122);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ON || _la==IGNORING) {
							{
							setState(121);
							grouping();
							}
						}

						setState(124);
						vectorExpression(5);
						}
						break;
					case 7:
						{
						_localctx = new SubqueryOperationContext(new VectorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_vectorExpression);
						setState(126);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(127);
						subquery();
						setState(129);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
						case 1:
							{
							setState(128);
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
						setState(131);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(132);
						limit();
						}
						break;
					}
					} 
				}
				setState(137);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(138);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(140);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(142);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 57344L) != 0) ) {
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(144);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(146);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 16515072L) != 0) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(148);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BOOL) {
				{
				setState(147);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(150);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(152);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(159);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(154);
				function();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(155);
				aggregation();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(156);
				instantOrRangeSelector();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(157);
				literal();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(158);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(161);
			match(T__0);
			setState(162);
			vectorExpression(0);
			setState(163);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(165);
			instantSelector();
			setState(167);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(166);
				window();
				}
				break;
			}
			setState(170);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(169);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(184);
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
				setState(172);
				metricName();
				setState(178);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(173);
					match(T__2);
					setState(175);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (((_la) & ~0x3f) == 0 && ((1L << _la) & 1649200332800L) != 0) {
						{
						setState(174);
						labelMatcherList();
						}
					}

					setState(177);
					match(T__3);
					}
					break;
				}
				}
				break;
			case T__2:
				enterOuterAlt(_localctx, 2);
				{
				setState(180);
				match(T__2);
				setState(181);
				labelMatcherList();
				setState(182);
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

	@SuppressWarnings("CheckReturnValue")
	public static class WindowContext extends ParserRuleContext {
		public List<TerminalNode> DURATION() { return getTokens(PromQLParser.DURATION); }
		public TerminalNode DURATION(int i) {
			return getToken(PromQLParser.DURATION, i);
		}
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
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(186);
			match(T__4);
			setState(188); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(187);
				match(DURATION);
				}
				}
				setState(190); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==DURATION );
			setState(192);
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

	@SuppressWarnings("CheckReturnValue")
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
			setState(194);
			match(OFFSET);
			setState(195);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 30, RULE_limit);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(197);
			match(LIMIT);
			setState(198);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 32, RULE_subquery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(200);
			match(T__4);
			setState(201);
			match(DURATION);
			setState(202);
			match(T__6);
			setState(204);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DURATION) {
				{
				setState(203);
				match(DURATION);
				}
			}

			setState(206);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 34, RULE_labelMatcher);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(208);
			labelName();
			setState(209);
			labelMatcherOp();
			setState(210);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 36, RULE_labelMatcherOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(212);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 50987008L) != 0) ) {
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 38, RULE_labelMatcherList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(214);
			labelMatcher();
			setState(219);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(215);
				match(T__7);
				setState(216);
				labelMatcher();
				}
				}
				setState(221);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 40, RULE_function);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			match(IDENTIFIER);
			setState(223);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 42, RULE_parameter);
		try {
			setState(227);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(225);
				literal();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(226);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 44, RULE_parameterList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(229);
			match(T__0);
			setState(238);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((_la) & ~0x3f) == 0 && ((1L << _la) & 3678572387850L) != 0) {
				{
				setState(230);
				parameter();
				setState(235);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(231);
					match(T__7);
					setState(232);
					parameter();
					}
					}
					setState(237);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(240);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 46, RULE_aggregation);
		try {
			setState(257);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(242);
				match(AGGREGATION_OP);
				setState(243);
				parameterList();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(244);
				match(AGGREGATION_OP);
				setState(247);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BY:
					{
					setState(245);
					by();
					}
					break;
				case WITHOUT:
					{
					setState(246);
					without();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(249);
				parameterList();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(251);
				match(AGGREGATION_OP);
				setState(252);
				parameterList();
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 48, RULE_by);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(259);
			match(BY);
			setState(260);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 50, RULE_without);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(262);
			match(WITHOUT);
			setState(263);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 52, RULE_grouping);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(267);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				{
				setState(265);
				on();
				}
				break;
			case IGNORING:
				{
				setState(266);
				ignoring();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(271);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GROUP_LEFT:
				{
				setState(269);
				groupLeft();
				}
				break;
			case GROUP_RIGHT:
				{
				setState(270);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 54, RULE_on);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(273);
			match(ON);
			setState(274);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 56, RULE_ignoring);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(276);
			match(IGNORING);
			setState(277);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 58, RULE_groupLeft);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(279);
			match(GROUP_LEFT);
			setState(281);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
			case 1:
				{
				setState(280);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 60, RULE_groupRight);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(283);
			match(GROUP_RIGHT);
			setState(285);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
			case 1:
				{
				setState(284);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 62, RULE_metricName);
		try {
			setState(290);
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
				setState(287);
				metricKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(288);
				match(IDENTIFIER);
				}
				break;
			case IDENTIFIER_EXTENDED:
				enterOuterAlt(_localctx, 3);
				{
				setState(289);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 64, RULE_metricKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(292);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 380037496832L) != 0) ) {
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 66, RULE_labelName);
		try {
			setState(296);
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
				setState(294);
				labelKeyword();
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(295);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 68, RULE_labelNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(298);
			match(T__0);
			setState(307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((_la) & ~0x3f) == 0 && ((1L << _la) & 1649200332800L) != 0) {
				{
				setState(299);
				labelName();
				setState(304);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(300);
					match(T__7);
					setState(301);
					labelName();
					}
					}
					setState(306);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(309);
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 70, RULE_labelKeyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(311);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & 549688705024L) != 0) ) {
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

	@SuppressWarnings("CheckReturnValue")
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
		enterRule(_localctx, 72, RULE_literal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(313);
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
		"\u0004\u0001+\u013c\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
		"\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
		"\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
		"\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
		"\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007\"\u0002"+
		"#\u0007#\u0002$\u0007$\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001S\b\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001X\b\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001_\b\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001"+
		"f\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0003\u0001m\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0003\u0001t\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0003\u0001{\b\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u0082\b\u0001\u0001\u0001"+
		"\u0001\u0001\u0005\u0001\u0086\b\u0001\n\u0001\f\u0001\u0089\t\u0001\u0001"+
		"\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001"+
		"\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0003\u0006\u0095\b\u0006\u0001"+
		"\u0007\u0001\u0007\u0001\b\u0001\b\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0003\t\u00a0\b\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\u000b\u0001"+
		"\u000b\u0003\u000b\u00a8\b\u000b\u0001\u000b\u0003\u000b\u00ab\b\u000b"+
		"\u0001\f\u0001\f\u0001\f\u0003\f\u00b0\b\f\u0001\f\u0003\f\u00b3\b\f\u0001"+
		"\f\u0001\f\u0001\f\u0001\f\u0003\f\u00b9\b\f\u0001\r\u0001\r\u0004\r\u00bd"+
		"\b\r\u000b\r\f\r\u00be\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0010"+
		"\u0001\u0010\u0003\u0010\u00cd\b\u0010\u0001\u0010\u0001\u0010\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0013"+
		"\u0001\u0013\u0001\u0013\u0005\u0013\u00da\b\u0013\n\u0013\f\u0013\u00dd"+
		"\t\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0003"+
		"\u0015\u00e4\b\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0005"+
		"\u0016\u00ea\b\u0016\n\u0016\f\u0016\u00ed\t\u0016\u0003\u0016\u00ef\b"+
		"\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0003\u0017\u00f8\b\u0017\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0003\u0017\u0100\b\u0017\u0003"+
		"\u0017\u0102\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0001"+
		"\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0003\u001a\u010c\b\u001a\u0001"+
		"\u001a\u0001\u001a\u0003\u001a\u0110\b\u001a\u0001\u001b\u0001\u001b\u0001"+
		"\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001d\u0001\u001d\u0003"+
		"\u001d\u011a\b\u001d\u0001\u001e\u0001\u001e\u0003\u001e\u011e\b\u001e"+
		"\u0001\u001f\u0001\u001f\u0001\u001f\u0003\u001f\u0123\b\u001f\u0001 "+
		"\u0001 \u0001!\u0001!\u0003!\u0129\b!\u0001\"\u0001\"\u0001\"\u0001\""+
		"\u0005\"\u012f\b\"\n\"\f\"\u0132\t\"\u0003\"\u0134\b\"\u0001\"\u0001\""+
		"\u0001#\u0001#\u0001$\u0001$\u0001$\u0000\u0001\u0002%\u0000\u0002\u0004"+
		"\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \""+
		"$&(*,.02468:<>@BDFH\u0000\b\u0001\u0000\u000b\f\u0001\u0000\r\u000f\u0001"+
		"\u0000\u0012\u0017\u0002\u0000\u001a\u001a\u001c\u001c\u0003\u0000\u0011"+
		"\u0011\u0013\u0013\u0018\u0019\u0003\u0000\u001a\u001e#$&&\u0001\u0000"+
		"\u001a&\u0001\u0000\t\n\u0144\u0000J\u0001\u0000\u0000\u0000\u0002R\u0001"+
		"\u0000\u0000\u0000\u0004\u008a\u0001\u0000\u0000\u0000\u0006\u008c\u0001"+
		"\u0000\u0000\u0000\b\u008e\u0001\u0000\u0000\u0000\n\u0090\u0001\u0000"+
		"\u0000\u0000\f\u0092\u0001\u0000\u0000\u0000\u000e\u0096\u0001\u0000\u0000"+
		"\u0000\u0010\u0098\u0001\u0000\u0000\u0000\u0012\u009f\u0001\u0000\u0000"+
		"\u0000\u0014\u00a1\u0001\u0000\u0000\u0000\u0016\u00a5\u0001\u0000\u0000"+
		"\u0000\u0018\u00b8\u0001\u0000\u0000\u0000\u001a\u00ba\u0001\u0000\u0000"+
		"\u0000\u001c\u00c2\u0001\u0000\u0000\u0000\u001e\u00c5\u0001\u0000\u0000"+
		"\u0000 \u00c8\u0001\u0000\u0000\u0000\"\u00d0\u0001\u0000\u0000\u0000"+
		"$\u00d4\u0001\u0000\u0000\u0000&\u00d6\u0001\u0000\u0000\u0000(\u00de"+
		"\u0001\u0000\u0000\u0000*\u00e3\u0001\u0000\u0000\u0000,\u00e5\u0001\u0000"+
		"\u0000\u0000.\u0101\u0001\u0000\u0000\u00000\u0103\u0001\u0000\u0000\u0000"+
		"2\u0106\u0001\u0000\u0000\u00004\u010b\u0001\u0000\u0000\u00006\u0111"+
		"\u0001\u0000\u0000\u00008\u0114\u0001\u0000\u0000\u0000:\u0117\u0001\u0000"+
		"\u0000\u0000<\u011b\u0001\u0000\u0000\u0000>\u0122\u0001\u0000\u0000\u0000"+
		"@\u0124\u0001\u0000\u0000\u0000B\u0128\u0001\u0000\u0000\u0000D\u012a"+
		"\u0001\u0000\u0000\u0000F\u0137\u0001\u0000\u0000\u0000H\u0139\u0001\u0000"+
		"\u0000\u0000JK\u0003\u0002\u0001\u0000KL\u0005\u0000\u0000\u0001L\u0001"+
		"\u0001\u0000\u0000\u0000MN\u0006\u0001\uffff\uffff\u0000NO\u0003\u0004"+
		"\u0002\u0000OP\u0003\u0002\u0001\tPS\u0001\u0000\u0000\u0000QS\u0003\u0012"+
		"\t\u0000RM\u0001\u0000\u0000\u0000RQ\u0001\u0000\u0000\u0000S\u0087\u0001"+
		"\u0000\u0000\u0000TU\n\n\u0000\u0000UW\u0003\u0006\u0003\u0000VX\u0003"+
		"4\u001a\u0000WV\u0001\u0000\u0000\u0000WX\u0001\u0000\u0000\u0000XY\u0001"+
		"\u0000\u0000\u0000YZ\u0003\u0002\u0001\nZ\u0086\u0001\u0000\u0000\u0000"+
		"[\\\n\b\u0000\u0000\\^\u0003\b\u0004\u0000]_\u00034\u001a\u0000^]\u0001"+
		"\u0000\u0000\u0000^_\u0001\u0000\u0000\u0000_`\u0001\u0000\u0000\u0000"+
		"`a\u0003\u0002\u0001\ta\u0086\u0001\u0000\u0000\u0000bc\n\u0007\u0000"+
		"\u0000ce\u0003\n\u0005\u0000df\u00034\u001a\u0000ed\u0001\u0000\u0000"+
		"\u0000ef\u0001\u0000\u0000\u0000fg\u0001\u0000\u0000\u0000gh\u0003\u0002"+
		"\u0001\bh\u0086\u0001\u0000\u0000\u0000ij\n\u0006\u0000\u0000jl\u0003"+
		"\f\u0006\u0000km\u00034\u001a\u0000lk\u0001\u0000\u0000\u0000lm\u0001"+
		"\u0000\u0000\u0000mn\u0001\u0000\u0000\u0000no\u0003\u0002\u0001\u0007"+
		"o\u0086\u0001\u0000\u0000\u0000pq\n\u0005\u0000\u0000qs\u0003\u000e\u0007"+
		"\u0000rt\u00034\u001a\u0000sr\u0001\u0000\u0000\u0000st\u0001\u0000\u0000"+
		"\u0000tu\u0001\u0000\u0000\u0000uv\u0003\u0002\u0001\u0006v\u0086\u0001"+
		"\u0000\u0000\u0000wx\n\u0004\u0000\u0000xz\u0003\u0010\b\u0000y{\u0003"+
		"4\u001a\u0000zy\u0001\u0000\u0000\u0000z{\u0001\u0000\u0000\u0000{|\u0001"+
		"\u0000\u0000\u0000|}\u0003\u0002\u0001\u0005}\u0086\u0001\u0000\u0000"+
		"\u0000~\u007f\n\u0003\u0000\u0000\u007f\u0081\u0003 \u0010\u0000\u0080"+
		"\u0082\u0003\u001c\u000e\u0000\u0081\u0080\u0001\u0000\u0000\u0000\u0081"+
		"\u0082\u0001\u0000\u0000\u0000\u0082\u0086\u0001\u0000\u0000\u0000\u0083"+
		"\u0084\n\u0002\u0000\u0000\u0084\u0086\u0003\u001e\u000f\u0000\u0085T"+
		"\u0001\u0000\u0000\u0000\u0085[\u0001\u0000\u0000\u0000\u0085b\u0001\u0000"+
		"\u0000\u0000\u0085i\u0001\u0000\u0000\u0000\u0085p\u0001\u0000\u0000\u0000"+
		"\u0085w\u0001\u0000\u0000\u0000\u0085~\u0001\u0000\u0000\u0000\u0085\u0083"+
		"\u0001\u0000\u0000\u0000\u0086\u0089\u0001\u0000\u0000\u0000\u0087\u0085"+
		"\u0001\u0000\u0000\u0000\u0087\u0088\u0001\u0000\u0000\u0000\u0088\u0003"+
		"\u0001\u0000\u0000\u0000\u0089\u0087\u0001\u0000\u0000\u0000\u008a\u008b"+
		"\u0007\u0000\u0000\u0000\u008b\u0005\u0001\u0000\u0000\u0000\u008c\u008d"+
		"\u0005\u0010\u0000\u0000\u008d\u0007\u0001\u0000\u0000\u0000\u008e\u008f"+
		"\u0007\u0001\u0000\u0000\u008f\t\u0001\u0000\u0000\u0000\u0090\u0091\u0007"+
		"\u0000\u0000\u0000\u0091\u000b\u0001\u0000\u0000\u0000\u0092\u0094\u0007"+
		"\u0002\u0000\u0000\u0093\u0095\u0005%\u0000\u0000\u0094\u0093\u0001\u0000"+
		"\u0000\u0000\u0094\u0095\u0001\u0000\u0000\u0000\u0095\r\u0001\u0000\u0000"+
		"\u0000\u0096\u0097\u0007\u0003\u0000\u0000\u0097\u000f\u0001\u0000\u0000"+
		"\u0000\u0098\u0099\u0005\u001b\u0000\u0000\u0099\u0011\u0001\u0000\u0000"+
		"\u0000\u009a\u00a0\u0003(\u0014\u0000\u009b\u00a0\u0003.\u0017\u0000\u009c"+
		"\u00a0\u0003\u0016\u000b\u0000\u009d\u00a0\u0003H$\u0000\u009e\u00a0\u0003"+
		"\u0014\n\u0000\u009f\u009a\u0001\u0000\u0000\u0000\u009f\u009b\u0001\u0000"+
		"\u0000\u0000\u009f\u009c\u0001\u0000\u0000\u0000\u009f\u009d\u0001\u0000"+
		"\u0000\u0000\u009f\u009e\u0001\u0000\u0000\u0000\u00a0\u0013\u0001\u0000"+
		"\u0000\u0000\u00a1\u00a2\u0005\u0001\u0000\u0000\u00a2\u00a3\u0003\u0002"+
		"\u0001\u0000\u00a3\u00a4\u0005\u0002\u0000\u0000\u00a4\u0015\u0001\u0000"+
		"\u0000\u0000\u00a5\u00a7\u0003\u0018\f\u0000\u00a6\u00a8\u0003\u001a\r"+
		"\u0000\u00a7\u00a6\u0001\u0000\u0000\u0000\u00a7\u00a8\u0001\u0000\u0000"+
		"\u0000\u00a8\u00aa\u0001\u0000\u0000\u0000\u00a9\u00ab\u0003\u001c\u000e"+
		"\u0000\u00aa\u00a9\u0001\u0000\u0000\u0000\u00aa\u00ab\u0001\u0000\u0000"+
		"\u0000\u00ab\u0017\u0001\u0000\u0000\u0000\u00ac\u00b2\u0003>\u001f\u0000"+
		"\u00ad\u00af\u0005\u0003\u0000\u0000\u00ae\u00b0\u0003&\u0013\u0000\u00af"+
		"\u00ae\u0001\u0000\u0000\u0000\u00af\u00b0\u0001\u0000\u0000\u0000\u00b0"+
		"\u00b1\u0001\u0000\u0000\u0000\u00b1\u00b3\u0005\u0004\u0000\u0000\u00b2"+
		"\u00ad\u0001\u0000\u0000\u0000\u00b2\u00b3\u0001\u0000\u0000\u0000\u00b3"+
		"\u00b9\u0001\u0000\u0000\u0000\u00b4\u00b5\u0005\u0003\u0000\u0000\u00b5"+
		"\u00b6\u0003&\u0013\u0000\u00b6\u00b7\u0005\u0004\u0000\u0000\u00b7\u00b9"+
		"\u0001\u0000\u0000\u0000\u00b8\u00ac\u0001\u0000\u0000\u0000\u00b8\u00b4"+
		"\u0001\u0000\u0000\u0000\u00b9\u0019\u0001\u0000\u0000\u0000\u00ba\u00bc"+
		"\u0005\u0005\u0000\u0000\u00bb\u00bd\u0005\'\u0000\u0000\u00bc\u00bb\u0001"+
		"\u0000\u0000\u0000\u00bd\u00be\u0001\u0000\u0000\u0000\u00be\u00bc\u0001"+
		"\u0000\u0000\u0000\u00be\u00bf\u0001\u0000\u0000\u0000\u00bf\u00c0\u0001"+
		"\u0000\u0000\u0000\u00c0\u00c1\u0005\u0006\u0000\u0000\u00c1\u001b\u0001"+
		"\u0000\u0000\u0000\u00c2\u00c3\u0005#\u0000\u0000\u00c3\u00c4\u0005\'"+
		"\u0000\u0000\u00c4\u001d\u0001\u0000\u0000\u0000\u00c5\u00c6\u0005$\u0000"+
		"\u0000\u00c6\u00c7\u0005\t\u0000\u0000\u00c7\u001f\u0001\u0000\u0000\u0000"+
		"\u00c8\u00c9\u0005\u0005\u0000\u0000\u00c9\u00ca\u0005\'\u0000\u0000\u00ca"+
		"\u00cc\u0005\u0007\u0000\u0000\u00cb\u00cd\u0005\'\u0000\u0000\u00cc\u00cb"+
		"\u0001\u0000\u0000\u0000\u00cc\u00cd\u0001\u0000\u0000\u0000\u00cd\u00ce"+
		"\u0001\u0000\u0000\u0000\u00ce\u00cf\u0005\u0006\u0000\u0000\u00cf!\u0001"+
		"\u0000\u0000\u0000\u00d0\u00d1\u0003B!\u0000\u00d1\u00d2\u0003$\u0012"+
		"\u0000\u00d2\u00d3\u0005\n\u0000\u0000\u00d3#\u0001\u0000\u0000\u0000"+
		"\u00d4\u00d5\u0007\u0004\u0000\u0000\u00d5%\u0001\u0000\u0000\u0000\u00d6"+
		"\u00db\u0003\"\u0011\u0000\u00d7\u00d8\u0005\b\u0000\u0000\u00d8\u00da"+
		"\u0003\"\u0011\u0000\u00d9\u00d7\u0001\u0000\u0000\u0000\u00da\u00dd\u0001"+
		"\u0000\u0000\u0000\u00db\u00d9\u0001\u0000\u0000\u0000\u00db\u00dc\u0001"+
		"\u0000\u0000\u0000\u00dc\'\u0001\u0000\u0000\u0000\u00dd\u00db\u0001\u0000"+
		"\u0000\u0000\u00de\u00df\u0005(\u0000\u0000\u00df\u00e0\u0003,\u0016\u0000"+
		"\u00e0)\u0001\u0000\u0000\u0000\u00e1\u00e4\u0003H$\u0000\u00e2\u00e4"+
		"\u0003\u0002\u0001\u0000\u00e3\u00e1\u0001\u0000\u0000\u0000\u00e3\u00e2"+
		"\u0001\u0000\u0000\u0000\u00e4+\u0001\u0000\u0000\u0000\u00e5\u00ee\u0005"+
		"\u0001\u0000\u0000\u00e6\u00eb\u0003*\u0015\u0000\u00e7\u00e8\u0005\b"+
		"\u0000\u0000\u00e8\u00ea\u0003*\u0015\u0000\u00e9\u00e7\u0001\u0000\u0000"+
		"\u0000\u00ea\u00ed\u0001\u0000\u0000\u0000\u00eb\u00e9\u0001\u0000\u0000"+
		"\u0000\u00eb\u00ec\u0001\u0000\u0000\u0000\u00ec\u00ef\u0001\u0000\u0000"+
		"\u0000\u00ed\u00eb\u0001\u0000\u0000\u0000\u00ee\u00e6\u0001\u0000\u0000"+
		"\u0000\u00ee\u00ef\u0001\u0000\u0000\u0000\u00ef\u00f0\u0001\u0000\u0000"+
		"\u0000\u00f0\u00f1\u0005\u0002\u0000\u0000\u00f1-\u0001\u0000\u0000\u0000"+
		"\u00f2\u00f3\u0005&\u0000\u0000\u00f3\u0102\u0003,\u0016\u0000\u00f4\u00f7"+
		"\u0005&\u0000\u0000\u00f5\u00f8\u00030\u0018\u0000\u00f6\u00f8\u00032"+
		"\u0019\u0000\u00f7\u00f5\u0001\u0000\u0000\u0000\u00f7\u00f6\u0001\u0000"+
		"\u0000\u0000\u00f8\u00f9\u0001\u0000\u0000\u0000\u00f9\u00fa\u0003,\u0016"+
		"\u0000\u00fa\u0102\u0001\u0000\u0000\u0000\u00fb\u00fc\u0005&\u0000\u0000"+
		"\u00fc\u00ff\u0003,\u0016\u0000\u00fd\u0100\u00030\u0018\u0000\u00fe\u0100"+
		"\u00032\u0019\u0000\u00ff\u00fd\u0001\u0000\u0000\u0000\u00ff\u00fe\u0001"+
		"\u0000\u0000\u0000\u0100\u0102\u0001\u0000\u0000\u0000\u0101\u00f2\u0001"+
		"\u0000\u0000\u0000\u0101\u00f4\u0001\u0000\u0000\u0000\u0101\u00fb\u0001"+
		"\u0000\u0000\u0000\u0102/\u0001\u0000\u0000\u0000\u0103\u0104\u0005\u001d"+
		"\u0000\u0000\u0104\u0105\u0003D\"\u0000\u01051\u0001\u0000\u0000\u0000"+
		"\u0106\u0107\u0005\u001e\u0000\u0000\u0107\u0108\u0003D\"\u0000\u0108"+
		"3\u0001\u0000\u0000\u0000\u0109\u010c\u00036\u001b\u0000\u010a\u010c\u0003"+
		"8\u001c\u0000\u010b\u0109\u0001\u0000\u0000\u0000\u010b\u010a\u0001\u0000"+
		"\u0000\u0000\u010c\u010f\u0001\u0000\u0000\u0000\u010d\u0110\u0003:\u001d"+
		"\u0000\u010e\u0110\u0003<\u001e\u0000\u010f\u010d\u0001\u0000\u0000\u0000"+
		"\u010f\u010e\u0001\u0000\u0000\u0000\u010f\u0110\u0001\u0000\u0000\u0000"+
		"\u01105\u0001\u0000\u0000\u0000\u0111\u0112\u0005\u001f\u0000\u0000\u0112"+
		"\u0113\u0003D\"\u0000\u01137\u0001\u0000\u0000\u0000\u0114\u0115\u0005"+
		" \u0000\u0000\u0115\u0116\u0003D\"\u0000\u01169\u0001\u0000\u0000\u0000"+
		"\u0117\u0119\u0005!\u0000\u0000\u0118\u011a\u0003D\"\u0000\u0119\u0118"+
		"\u0001\u0000\u0000\u0000\u0119\u011a\u0001\u0000\u0000\u0000\u011a;\u0001"+
		"\u0000\u0000\u0000\u011b\u011d\u0005\"\u0000\u0000\u011c\u011e\u0003D"+
		"\"\u0000\u011d\u011c\u0001\u0000\u0000\u0000\u011d\u011e\u0001\u0000\u0000"+
		"\u0000\u011e=\u0001\u0000\u0000\u0000\u011f\u0123\u0003@ \u0000\u0120"+
		"\u0123\u0005(\u0000\u0000\u0121\u0123\u0005)\u0000\u0000\u0122\u011f\u0001"+
		"\u0000\u0000\u0000\u0122\u0120\u0001\u0000\u0000\u0000\u0122\u0121\u0001"+
		"\u0000\u0000\u0000\u0123?\u0001\u0000\u0000\u0000\u0124\u0125\u0007\u0005"+
		"\u0000\u0000\u0125A\u0001\u0000\u0000\u0000\u0126\u0129\u0003F#\u0000"+
		"\u0127\u0129\u0005(\u0000\u0000\u0128\u0126\u0001\u0000\u0000\u0000\u0128"+
		"\u0127\u0001\u0000\u0000\u0000\u0129C\u0001\u0000\u0000\u0000\u012a\u0133"+
		"\u0005\u0001\u0000\u0000\u012b\u0130\u0003B!\u0000\u012c\u012d\u0005\b"+
		"\u0000\u0000\u012d\u012f\u0003B!\u0000\u012e\u012c\u0001\u0000\u0000\u0000"+
		"\u012f\u0132\u0001\u0000\u0000\u0000\u0130\u012e\u0001\u0000\u0000\u0000"+
		"\u0130\u0131\u0001\u0000\u0000\u0000\u0131\u0134\u0001\u0000\u0000\u0000"+
		"\u0132\u0130\u0001\u0000\u0000\u0000\u0133\u012b\u0001\u0000\u0000\u0000"+
		"\u0133\u0134\u0001\u0000\u0000\u0000\u0134\u0135\u0001\u0000\u0000\u0000"+
		"\u0135\u0136\u0005\u0002\u0000\u0000\u0136E\u0001\u0000\u0000\u0000\u0137"+
		"\u0138\u0007\u0006\u0000\u0000\u0138G\u0001\u0000\u0000\u0000\u0139\u013a"+
		"\u0007\u0007\u0000\u0000\u013aI\u0001\u0000\u0000\u0000\"RW^elsz\u0081"+
		"\u0085\u0087\u0094\u009f\u00a7\u00aa\u00af\u00b2\u00b8\u00be\u00cc\u00db"+
		"\u00e3\u00eb\u00ee\u00f7\u00ff\u0101\u010b\u010f\u0119\u011d\u0122\u0128"+
		"\u0130\u0133";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}