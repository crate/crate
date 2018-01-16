package io.crate.execution.expression.operator;

import io.crate.analyze.symbol.Literal;
import io.crate.execution.expression.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

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
        assertEvaluate("null < name", null, Literal.of("foo"));
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
        assertEvaluate("name > null", null, Literal.of("foo"));
        assertEvaluate("null > null", null);
    }

    @Test
    public void testBetween() {
        assertNormalize("0.1 between 0.01 and 0.2", isLiteral(true));
        assertNormalize("10 between 1 and 2", isLiteral(false));
        assertNormalize("'abd' between 'abc' and 'abe'", isLiteral(true));
        assertEvaluate("1 between 0 and null", null);
        assertEvaluate("1 between null and 10", null);
        assertEvaluate("1 between null and null", null);
        assertEvaluate("null between 1 and 10", null);
        assertEvaluate("null between 1 and null", null);
        assertEvaluate("null between null and 10", null);
        assertEvaluate("null between null and null", null);
    }
}
