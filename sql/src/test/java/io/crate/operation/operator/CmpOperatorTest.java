package io.crate.operation.operator;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Value;
import io.crate.operation.Input;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.instanceOf;

public class CmpOperatorTest extends CrateUnitTest {

    private GtOperator op_gt_string;
    private GteOperator op_gte_double;
    private LtOperator op_lt_int;
    private LteOperator op_lte_long;
    private LtOperator op_lt_string;

    @Before
    public void prepare() {
        op_gt_string = new GtOperator(Operator.generateInfo(GtOperator.NAME, DataTypes.STRING));
        op_lt_string = new LtOperator(Operator.generateInfo(LtOperator.NAME, DataTypes.STRING));
        op_gte_double = new GteOperator(Operator.generateInfo(GteOperator.NAME, DataTypes.DOUBLE));
        op_lt_int = new LtOperator(Operator.generateInfo(LtOperator.NAME, DataTypes.INTEGER));
        op_lte_long = new LteOperator(Operator.generateInfo(LteOperator.NAME, DataTypes.LONG));
    }

    private Function getFunction(Operator operator, Symbol... symbols) {
        return new Function(operator.info(), Arrays.asList(symbols));
    }

    private Symbol normalize(Operator operator, Symbol... symbols) {
        return operator.normalizeSymbol(getFunction(operator, symbols));
    }

    @Test
    public void testLteNormalizeSymbolTwoLiteral() throws Exception {
        Symbol symbol = normalize(op_lte_long, Literal.newLiteral(8L), Literal.newLiteral(200L));
        assertThat(symbol, isLiteral(true));

        symbol = normalize(op_lte_long, Literal.newLiteral(8L), Literal.newLiteral(8L));
        assertThat(symbol, isLiteral(true));

        symbol = normalize(op_lte_long, Literal.newLiteral(16L), Literal.newLiteral(8L));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testGteNormalizeSymbolTwoLiteral() throws Exception {
        Symbol symbol = normalize(op_gte_double, Literal.newLiteral(0.03), Literal.newLiteral(0.4));
        assertThat(symbol, isLiteral(false));

        symbol = normalize(op_gte_double, Literal.newLiteral(0.4), Literal.newLiteral(0.4));
        assertThat(symbol, isLiteral(true));

        symbol = normalize(op_gte_double, Literal.newLiteral(0.6), Literal.newLiteral(0.4));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralTrue() throws Exception {
        Symbol symbol = normalize(op_lt_int, Literal.newLiteral(2), Literal.newLiteral(4));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralFalse() throws Exception {
        Symbol symbol = normalize(op_lt_int, Literal.newLiteral(4), Literal.newLiteral(2));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testLtNormalizeSymbolTwoLiteralFalseEq() throws Exception {
        Symbol symbol = normalize(op_lt_int, Literal.newLiteral(4), Literal.newLiteral(4));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testGtNormalizeSymbolTwoLiteralFalse() throws Exception {
        Symbol symbol = normalize(op_gt_string, Literal.newLiteral("aa"), Literal.newLiteral("bbb"));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testCisGtThanA() throws Exception {
        assertTrue(op_gt_string.evaluate((Input) Literal.newLiteral("c"), (Input) Literal.newLiteral("a")));
    }

    @Test
    public void testAisLtThanC() throws Exception {
        assertTrue(op_lt_string.evaluate((Input) Literal.newLiteral("a"), (Input) Literal.newLiteral("c")));
    }

    @Test
    public void testNormalizeSymbolWithNull() throws Exception {
        Literal literal = (Literal)normalize(op_gt_string, Literal.NULL, Literal.newLiteral("aa"));
        assertNull(literal.value());
        assertEquals(DataTypes.UNDEFINED, literal.valueType());
    }

    @Test
    public void testNormalizeSymbolNonLiteral() throws Exception {
        Symbol symbol = normalize(op_gt_string, Literal.newLiteral("a"), new Value(DataTypes.STRING));
        assertThat(symbol, instanceOf(Function.class));
    }
}
