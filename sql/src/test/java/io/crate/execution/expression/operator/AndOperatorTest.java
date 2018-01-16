package io.crate.execution.expression.operator;

import io.crate.execution.expression.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class AndOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeBooleanTrueAndNonLiteral() throws Exception {
        assertNormalize("is_awesome and true", isField("is_awesome"));
    }

    @Test
    public void testNormalizeBooleanFalseAndNonLiteral() throws Exception {
        assertNormalize("is_awesome and false", isLiteral(false));
    }

    @Test
    public void testNormalizeLiteralAndLiteral() throws Exception {
        assertNormalize("true and true", isLiteral(true));
    }

    @Test
    public void testNormalizeLiteralAndLiteralFalse() throws Exception {
        assertNormalize("true and false", isLiteral(false));
    }

    @Test
    public void testEvaluateAndOperator() {
        assertEvaluate("true and true", true);
        assertEvaluate("false and false", false);
        assertEvaluate("true and false", false);
        assertEvaluate("false and true", false);
        assertEvaluate("true and null", null);
        assertEvaluate("null and true", null);
        assertEvaluate("false and null", false);
        assertEvaluate("null and false", false);
        assertEvaluate("null and null", null);
    }
}
