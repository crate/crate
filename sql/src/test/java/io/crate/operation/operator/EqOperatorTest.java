package io.crate.operation.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.operator.input.ObjectInput;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
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

    private Functions functions;

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder().add(new OperatorModule()).createInjector().getInstance(Functions.class);
    }

    private EqOperator getOp(DataType dataType) {
        return (EqOperator) functions.get(
                new FunctionIdent(EqOperator.NAME, ImmutableList.of(dataType, dataType)));
    }

    @Test
    public void testNormalizeSymbol() {
        EqOperator op = getOp(DataTypes.INTEGER);

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, true);
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        EqOperator op = getOp(DataTypes.INTEGER);
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.NULL, Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.NULL, result.valueType());
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        EqOperator op = getOp(DataTypes.INTEGER);
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.NULL, result.valueType());
    }

    @Test
    public void testNormalizeSymbolNeq() {
        EqOperator op = getOp(DataTypes.INTEGER);

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, false);
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        EqOperator op = getOp(DataTypes.INTEGER);
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

    private Boolean eq(DataType type, Object left, Object right) {
        EqOperator op = getOp(type);
        return op.evaluate(new Input[] {new ObjectInput(left),new ObjectInput(right) });
    }

    @Test
    public void testEvaluateEqOperator() {
        assertTrue(eq(DataTypes.INTEGER, 1, 1));
        assertFalse(eq(DataTypes.LONG, 1L, 2L));
        assertTrue(eq(DataTypes.OBJECT,
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build(),
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build()
        ));
        assertFalse(eq(DataTypes.OBJECT,
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build(),
                ImmutableMap.<String, Object>builder()
                        .put("int", 2)
                        .put("boolean", false)
                        .build()
        ));
        assertNull(eq(DataTypes.FLOAT, null, 1f));
        assertNull(eq(DataTypes.STRING, "boing", null));
        assertNull(eq(DataTypes.STRING, null, null));
    }
}
