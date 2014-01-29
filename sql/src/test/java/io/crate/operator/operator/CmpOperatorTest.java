package io.crate.operator.operator;

import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CmpOperatorTest {

    private GtOperator op_gt_string;
    private GteOperator op_gte_double;
    private LtOperator op_lt_int;
    private LteOperator op_lte_long;
    private LikeOperator op_like_string;

    @Before
    public void setUp() {
        op_gt_string = new GtOperator(Operator.generateInfo(GtOperator.NAME, DataType.STRING));
        op_gte_double = new GteOperator(Operator.generateInfo(GteOperator.NAME, DataType.DOUBLE));
        op_lt_int = new LtOperator(Operator.generateInfo(LtOperator.NAME, DataType.INTEGER));
        op_lte_long = new LteOperator(Operator.generateInfo(LteOperator.NAME, DataType.LONG));
        op_like_string = new LikeOperator(Operator.generateInfo(LikeOperator.NAME, DataType.STRING));
    }

    private Function getFunction(Operator operator, Symbol... symbols) {
        return new Function(operator.info(), Arrays.asList(symbols));
    }

    private Symbol normalize(Operator operator, Symbol... symbols) {
        return operator.normalizeSymbol(getFunction(operator, symbols));
    }

    @Test
    public void testLteNormalizeSymbolTwoLiteral() throws Exception {
        Symbol symbol = normalize(op_lte_long, new LongLiteral(8L), new LongLiteral(200L));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(true));

        symbol = normalize(op_lte_long, new LongLiteral(8L), new LongLiteral(8L));
        assertThat(((BooleanLiteral) symbol).value(), is(true));

        symbol = normalize(op_lte_long, new LongLiteral(16L), new LongLiteral(8L));
        assertThat(((BooleanLiteral) symbol).value(), is(false));
    }

    @Test
    public void testGteNormalizeSymbolTwoLiteral() throws Exception {
        Symbol symbol = normalize(op_gte_double, new DoubleLiteral(0.03), new DoubleLiteral(0.4));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(false));

        symbol = normalize(op_gte_double, new DoubleLiteral(0.4), new DoubleLiteral(0.4));
        assertThat(((BooleanLiteral) symbol).value(), is(true));

        symbol = normalize(op_gte_double, new DoubleLiteral(0.6), new DoubleLiteral(0.4));
        assertThat(((BooleanLiteral) symbol).value(), is(true));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralTrue() throws Exception {
        Symbol symbol = normalize(op_lt_int, new IntegerLiteral(2), new IntegerLiteral(4));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(true));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralFalse() throws Exception {
        Symbol symbol = normalize(op_lt_int, new IntegerLiteral(4), new IntegerLiteral(2));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(false));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralFalseEq() throws Exception {
        Symbol symbol = normalize(op_lt_int, new IntegerLiteral(4), new IntegerLiteral(4));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(false));
    }

    @Test
    public void testGtNormalizeSymbolTwoLiteralFalse() throws Exception {
        Symbol symbol = normalize(op_gt_string, new StringLiteral("aa"), new StringLiteral("bbb"));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolWithNull() throws Exception {
        Symbol symbol = normalize(op_gt_string, Null.INSTANCE, new StringLiteral("aa"));
        assertThat(symbol, instanceOf(Null.class));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() throws Exception {
        Symbol symbol = normalize(op_gt_string, new StringLiteral("a"), new Reference(null));
        assertThat(symbol, instanceOf(Function.class));
    }

    @Test
    public void testNormalizeLikeLiteral() throws Exception {
        Symbol symbol = normalize(op_like_string, new StringLiteral("a"), new Reference(null));
        assertThat(symbol, instanceOf(Function.class));

        symbol = normalize(op_like_string, new StringLiteral("a"), new StringLiteral("a"));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral)symbol).value(), is(true));

        symbol = normalize(op_like_string, new StringLiteral("a"), new StringLiteral("b"));
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral)symbol).value(), is(false));
    }
}
