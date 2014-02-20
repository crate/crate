package io.crate.operator.operator;

import io.crate.operator.operator.input.BooleanInput;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class AndOperatorTest {
    @Test
    public void testNormalizeLiteralAndNonLiteral() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new BooleanLiteral(true), new Reference()));
        Symbol symbol = operator.normalizeSymbol(function);
        assertThat(symbol, instanceOf(Function.class));
    }

    @Test
    public void testNormalizeLiteralAndLiteral() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new BooleanLiteral(true), new BooleanLiteral(true)));
        Symbol symbol = operator.normalizeSymbol(function);
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(true));
    }

    @Test
    public void testNormalizeLiteralAndLiteralFalse() throws Exception {
        AndOperator operator = new AndOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new BooleanLiteral(true), new BooleanLiteral(false)));
        Symbol symbol = operator.normalizeSymbol(function);
        assertThat(symbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) symbol).value(), is(false));
    }

    @Test
    public void testEvaluateTrueTrue() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(true)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateTrueFalse() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(false)
        );

        assertFalse(result);
    }

    @Test
    public void testEvaluateFalseTrue() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                new BooleanInput(true)
        );

        assertFalse(result);
    }

    @Test
    public void testEvaluateTrueUnknown() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(null)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateUnknownTrue() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(true)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateFalseUnknown() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                new BooleanInput(null)
        );

        assertFalse(result);
    }

    @Test
    public void testEvaluateUnknownFalse() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(false)
        );

        assertFalse(result);
    }

    @Test
    public void testEvaluateUnknownUnknown() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(null)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateNullTrue() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                null,
                new BooleanInput(true)
        );
        assertNull(result);
    }

    @Test
    public void testEvaluateTrueNull() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                null
        );
        assertNull(result);
    }

    @Test
    public void testEvaluateNullFalse() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                null,
                new BooleanInput(false)
        );
        assertFalse(result);
    }

    @Test
    public void testEvaluateFalseNull() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                null
        );
        assertFalse(result);
    }

    @Test
    public void testEvaluateNullNull() {
        AndOperator operator = new AndOperator();
        Boolean result = operator.evaluate(
                null,
                null
        );
        assertNull(result);
    }
}
