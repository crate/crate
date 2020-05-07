package io.crate.expression.operator;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class EqOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbol() {
        assertNormalize("2 = 2", isLiteral(true));
    }

    @Test
    public void testEqArrayLeftSideIsNull_RightSideNull() throws Exception {
        assertEvaluate("[ [1, 1], [10] ] = null", null);
        assertEvaluate("null = [ [1, 1], [10] ]", null);
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1, 1], [10] ]", isLiteral(true));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1], [10] ]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoEqualArraysShouldReturnTrueLiteral() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 1, 10]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoNotEqualArraysShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 2, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        assertNormalize("null = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        assertNormalize("2 = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        assertNormalize("2 = 4", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        assertNormalize("name = 'Arthur'", isFunction(EqOperator.NAME));
    }

    @Test
    public void testEvaluateEqOperator() {
        assertNormalize("{l=1, b=true} = {l=1, b=true}", isLiteral(true));
        assertNormalize("{l=2, b=true} = {l=1, b=true}", isLiteral(false));

        assertNormalize("1.2 = null", isLiteral(null));
        assertNormalize("'foo' = null", isLiteral(null));
    }
}
