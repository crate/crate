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

public class OrOperatorTest {

    @Test
    public void testNormalizeSymbolReferenceAndLiteral() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), Literal.newLiteral(true)));
        Symbol normalizedSymbol = operator.normalizeSymbol(function);
        assertLiteralSymbol(normalizedSymbol, true);
    }

    @Test
    public void testNormalizeSymbolReferenceAndLiteralFalse() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), Literal.newLiteral(false)));
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

    private Boolean or(Boolean left, Boolean right) {
        OrOperator operator = new OrOperator();
        return operator.evaluate(new BooleanInput(left), new BooleanInput(right));
    }

    @Test
    public void testEvaluateAndOperator() {
        assertTrue(or(true, true));
        assertFalse(or(false, false));
        assertTrue(or(true, false));
        assertTrue(or(false, true));
        assertTrue(or(true, null));
        assertTrue(or(null, true));
        assertNull(or(false, null));
        assertNull(or(null, false));
        assertNull(or(null, null));
    }

}
