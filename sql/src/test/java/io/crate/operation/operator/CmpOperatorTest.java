package io.crate.operation.operator;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isFunction;
import static io.crate.testing.TestingHelpers.isLiteral;

public class CmpOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testLte() {
        assertNormalize("id <= 8", isFunction("op_<="));
        assertNormalize("8 <= 200", isLiteral(true));
        assertNormalize("0.1 <= 0.1", isLiteral(true));
        assertNormalize("16 <= 8", isLiteral(false));
        assertNormalize("'abc' <= 'abd'", isLiteral(true));
        assertEvaluate("true <= null", null);
        assertEvaluate("null <= 1", null);
        assertEvaluate("null <= 'abc'", null);
        assertEvaluate("null <= null", null);
    }

    @Test
    public void testLt() {
        assertNormalize("id < 8", isFunction("op_<"));
        assertNormalize("0.1 < 0.2", isLiteral(true));
        assertNormalize("'abc' < 'abd'", isLiteral(true));
        assertEvaluate("true < null", null);
        assertEvaluate("null < 1", null);
        assertEvaluate("null < name", null);
        assertEvaluate("null < null", null);
    }

    @Test
    public void testGte() {
        assertNormalize("id >= 8", isFunction("op_>="));
        assertNormalize("0.1 >= 0.1", isLiteral(true));
        assertNormalize("16 >= 8", isLiteral(true));
        assertNormalize("'abc' >= 'abd'", isLiteral(false));
        assertEvaluate("true >= null", null);
        assertEvaluate("null >= 1", null);
        assertEvaluate("null >= 'abc'", null);
        assertEvaluate("null >= null", null);
    }

    @Test
    public void testGt() {
        assertNormalize("id > 200", isFunction("op_>"));
        assertNormalize("0.1 > 0.1", isLiteral(false));
        assertNormalize("16 > 8", isLiteral(true));
        assertNormalize("'abd' > 'abc'", isLiteral(true));
        assertEvaluate("true > null", null);
        assertEvaluate("null > 1", null);
        assertEvaluate("name > null", null);
        assertEvaluate("null > null", null);
    }
}
