package io.crate.operation.operator;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class OrOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalize() throws Exception {
        assertNormalize("name or true", isLiteral(true));
        assertNormalize("true or name", isLiteral(true));
        assertNormalize("false or name", isField("name"));
        assertNormalize("name or name", isFunction(OrOperator.NAME));

        assertNormalize("true or 1/0", isLiteral(true));
        assertNormalize("1/0 or true", isLiteral(true));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("true or true", true);
        assertEvaluate("false or false", false);
        assertEvaluate("true or false", true);
        assertEvaluate("false or true", true);
        assertEvaluate("true or null", true);
        assertEvaluate("null or true", true);
        assertEvaluate("false or null", null);
        assertEvaluate("null or false", null);
        assertEvaluate("null or null", null);
    }
}
