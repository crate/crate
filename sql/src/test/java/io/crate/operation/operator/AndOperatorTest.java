package io.crate.operation.operator;

import io.crate.operation.operator.input.BooleanInput;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class AndOperatorTest {

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
        return operator.evaluate(new BooleanInput(left), new BooleanInput(right));
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
