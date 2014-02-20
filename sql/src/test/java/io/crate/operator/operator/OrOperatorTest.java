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
import static org.junit.Assert.assertThat;

public class OrOperatorTest {

    @Test
    public void testNormalizeSymbolReferenceAndLiteral() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), new BooleanLiteral(true)));
        Symbol normalizedSymbol = operator.normalizeSymbol(function);
        assertThat(normalizedSymbol, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral)normalizedSymbol).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolReferenceAndLiteralFalse() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), new BooleanLiteral(false)));
        Symbol normalizedSymbol = operator.normalizeSymbol(function);
        assertThat(normalizedSymbol, instanceOf(Function.class));
    }

    @Test
    public void testNormalizeSymbolReferenceAndReference() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), new Reference()));
        Symbol normalizedSymbol = operator.normalizeSymbol(function);
        assertThat(normalizedSymbol, instanceOf(Function.class));
    }

    @Test
    public void testEvaluateTrueTrue() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(true)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateTrueFalse() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(false)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateFalseTrue() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                new BooleanInput(true)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateTrueUnknown() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                new BooleanInput(null)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateUnknownTrue() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(true)
        );

        assertTrue(result);
    }

    @Test
    public void testEvaluateFalseUnknown() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                new BooleanInput(null)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateUnknownFalse() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(false)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateUnknownUnknown() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(null),
                new BooleanInput(null)
        );

        assertNull(result);
    }

    @Test
    public void testEvaluateNullTrue() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                null,
                new BooleanInput(true)
        );
        assertTrue(result);
    }

    @Test
    public void testEvaluateTrueNull() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(true),
                null
        );
        assertTrue(result);
    }

    @Test
    public void testEvaluateNullFalse() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                null,
                new BooleanInput(false)
        );
        assertNull(result);
    }

    @Test
    public void testEvaluateFalseNull() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                new BooleanInput(false),
                null
        );
        assertNull(result);
    }

    @Test
    public void testEvaluateNullNull() {
        OrOperator operator = new OrOperator();
        Boolean result = operator.evaluate(
                null,
                null
        );
        assertNull(result);
    }

}
