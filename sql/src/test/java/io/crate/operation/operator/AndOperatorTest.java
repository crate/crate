package io.crate.operation.operator;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.CoreMatchers.instanceOf;

public class AndOperatorTest extends CrateUnitTest {

    @Test
    public void testNormalizeBooleanTrueAndNonLiteral() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(Literal.newLiteral(true), new Reference()));
        Symbol symbol = operator.normalizeSymbol(function);
        assertThat(symbol, instanceOf(Reference.class));
    }

    @Test
    public void testNormalizeBooleanFalseAndNonLiteral() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(Literal.newLiteral(false), new Reference()));
        Symbol symbol = operator.normalizeSymbol(function);

        assertLiteralSymbol(symbol, false);
    }

    @Test
    public void testNormalizeLiteralAndLiteral() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(Literal.newLiteral(true), Literal.newLiteral(true)));
        Symbol symbol = operator.normalizeSymbol(function);
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testNormalizeLiteralAndLiteralFalse() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(Literal.newLiteral(true), Literal.newLiteral(false)));
        Symbol symbol = operator.normalizeSymbol(function);
        assertLiteralSymbol(symbol, false);
    }

    private Boolean and(Boolean left, Boolean right) {
        AndOperator operator = new AndOperator();
        return operator.evaluate(Literal.newLiteral(left), Literal.newLiteral(right));
    }

    @Test
    public void testEvaluateAndOperator() {
        assertTrue(and(true, true));
        assertFalse(and(false, false));
        assertFalse(and(true, false));
        assertFalse(and(false, true));
        assertNull(and(true, null));
        assertNull(and(null, true));
        assertFalse(and(false, null));
        assertFalse(and(null, false));
        assertNull(and(null, null));
    }
}
