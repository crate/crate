package io.crate.expression.operator;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Symbol;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;

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

    @Test
    public void test_get_conjunctions_of_predicate_with_2_ands() {
        Symbol query = sqlExpressions.asSymbol("(a = 1 or a = 2) AND x = 2 AND name = 'foo'");
        List<Symbol> split = AndOperator.split(query);
        assertThat(split, contains(
            isSQL("((doc.users.a = 1) OR (doc.users.a = 2))"),
            isSQL("(doc.users.x = 2)"),
            isSQL("(doc.users.name = 'foo')")
        ));
    }

    @Test
    public void test_get_conjunctions_of_predicate_without_any_ands() {
        Symbol query = sqlExpressions.asSymbol("a = 1");
        List<Symbol> split = AndOperator.split(query);
        assertThat(split, contains(
            isSQL("(doc.users.a = 1)")
        ));
    }
}
