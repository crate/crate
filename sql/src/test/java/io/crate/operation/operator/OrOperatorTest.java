package io.crate.operation.operator;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.StmtCtx;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.instanceOf;

public class OrOperatorTest extends CrateUnitTest {

    @Test
    public void testNormalizeSymbolReferenceAndLiteral() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), Literal.newLiteral(true)));
        Symbol normalizedSymbol = operator.normalizeSymbol(function, new StmtCtx());
        assertThat(normalizedSymbol, isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolReferenceAndLiteralFalse() throws Exception {
        OrOperator operator = new OrOperator();
        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), Literal.newLiteral(false)));
        Symbol normalizedSymbol = operator.normalizeSymbol(function, new StmtCtx());
        assertThat(normalizedSymbol, instanceOf(Reference.class));
    }

    @Test
    public void testNormalizeSymbolReferenceAndReference() throws Exception {
        OrOperator operator = new OrOperator();

        Function function = new Function(
                operator.info(), Arrays.<Symbol>asList(new Reference(), new Reference()));
        Symbol normalizedSymbol = operator.normalizeSymbol(function, new StmtCtx());
        assertThat(normalizedSymbol, instanceOf(Function.class));
    }

    private Boolean or(Boolean left, Boolean right) {
        OrOperator operator = new OrOperator();
        return operator.evaluate(Literal.newLiteral(left), Literal.newLiteral(right));
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
