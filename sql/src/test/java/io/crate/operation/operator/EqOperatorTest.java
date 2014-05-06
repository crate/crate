package io.crate.operation.operator;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.operator.input.ObjectInput;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EqOperatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testNormalizeSymbol() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, true);
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.NULL, Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.NULL, result.valueType());
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.NULL, result.valueType());
    }

    @Test
    public void testNormalizeSymbolNeq() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, false);
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));
        Function f1 = new Function(
                new FunctionInfo(
                        new FunctionIdent("dummy_function", Arrays.<DataType>asList(DataTypes.INTEGER)),
                        DataTypes.INTEGER
                ),
                Arrays.<Symbol>asList(Literal.newLiteral(2))
        );

        Function f2 = new Function(
                new FunctionInfo(
                        new FunctionIdent("dummy_function", Arrays.<DataType>asList(DataTypes.INTEGER)),
                        DataTypes.INTEGER
                ),
                Arrays.<Symbol>asList(Literal.newLiteral(2))
        );

        assertThat(f1.equals(f2), is(true)); // symbols are equal

        // EqOperator doesn't know (yet) if the result of the functions will be equal so no normalization
        Function function = new Function(op.info(), Arrays.<Symbol>asList(f1, f2));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(Function.class));
    }

    private Boolean eq(Object left, Object right) {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataTypes.INTEGER));
        return op.evaluate(new ObjectInput(left),new ObjectInput(right));
    }

    @Test
    public void testEvaluateEqOperator() {
        assertTrue(eq(1, 1));
        assertFalse(eq(1L, 2L));
        assertTrue(eq(
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build(),
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build()
        ));
        assertFalse(eq(
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build(),
                ImmutableMap.<String, Object>builder()
                        .put("int", 2)
                        .put("boolean", false)
                        .build()
        ));
        assertNull(eq(null, 1f));
        assertNull(eq("boing", null));
        assertNull(eq(null, null));
    }
}
