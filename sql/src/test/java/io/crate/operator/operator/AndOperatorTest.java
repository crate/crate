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
