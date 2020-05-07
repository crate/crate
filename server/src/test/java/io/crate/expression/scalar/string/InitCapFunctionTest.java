package io.crate.expression.scalar.string;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class InitCapFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeCapInitFuncForAllLowerCase() {
        assertNormalize("initcap('hello world!')", isLiteral("Hello World!"));
    }

    @Test
    public void testNormalizeCapInitFuncForAllUpperCase() {
        assertNormalize("initcap('HELLO WORLD!')", isLiteral("Hello World!"));
    }

    @Test
    public void testNormalizeCapInitFuncForMixedCase() {
        assertNormalize("initcap('HellO 1WORLD !')", isLiteral("Hello 1world !"));
    }

    @Test
    public void testNormalizeCapInitFuncForEmptyString() {
        assertNormalize("initcap('')", isLiteral(""));
    }

    @Test
    public void testNormalizeCapInitFuncForNonEnglishLatinChars() {
        assertNormalize("initcap('ÄÖÜ αß àbc γ')", isLiteral("Äöü Αß Àbc Γ"));
    }

    @Test
    public void testNormalizeCapInitFuncForNonLatinChars() {
        assertNormalize("initcap('汉字 this is chinese!')", isLiteral("汉字 This Is Chinese!"));
    }

    @Test
    public void testEvaluateWithNull() {
        assertEvaluate("initcap(name)", null, Literal.of(DataTypes.STRING, null));
    }
}
