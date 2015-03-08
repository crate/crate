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
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static io.crate.testing.TestingHelpers.createFunction;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;

public class EqOperatorTest extends CrateUnitTest {

    private Functions functions;

    @Before
    public void prepare() throws Exception {
        functions = new ModulesBuilder().add(new OperatorModule()).createInjector().getInstance(Functions.class);
    }

    private CmpOperator getOp(DataType dataType) {
        return (CmpOperator) functions.get(
                new FunctionIdent(EqOperator.NAME, ImmutableList.of(dataType, dataType)));
    }

    @Test
    public void testNormalizeSymbol() {
        CmpOperator op = getOp(DataTypes.INTEGER);

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, true);
    }

    @Test
    public void testEqArrayLeftSideIsNull_RightSideNull() throws Exception {
        ArrayType intIntArray = new ArrayType(new ArrayType(DataTypes.INTEGER));
        CmpOperator op = getOp(intIntArray);
        Object[][] values = new Object[][] {
                new Object[] { 1, 1},
                new Object[] { 10 }
        };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intIntArray, null),
                Literal.newLiteral(intIntArray, values));
        assertLiteralSymbol(op.normalizeSymbol(function), null, DataTypes.BOOLEAN);

        function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intIntArray, values),
                Literal.newLiteral(intIntArray, null));
        assertLiteralSymbol(op.normalizeSymbol(function), null, DataTypes.BOOLEAN);

        assertThat(eq(intIntArray, null, values), nullValue());
        assertThat(eq(intIntArray, values, null), nullValue());
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfEquals() throws Exception {
        ArrayType intIntArray = new ArrayType(new ArrayType(DataTypes.INTEGER));
        CmpOperator op = getOp(intIntArray);

        Object[][] left = new Object[][] {
                new Object[] { 1, 1},
                new Object[] { 10 }
        };
        Object[][] right = new Object[][] {
                new Object[] { 1, 1},
                new Object[] { 10 }
        };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intIntArray, left),
                Literal.newLiteral(intIntArray, right));

        assertLiteralSymbol(op.normalizeSymbol(function), true);
        assertThat(eq(intIntArray, left, right), is(true));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotEquals() throws Exception {
        ArrayType intIntArray = new ArrayType(new ArrayType(DataTypes.INTEGER));
        CmpOperator op = getOp(intIntArray);

        Object[][] left = new Object[][] {
                new Object[] { 1 },
                new Object[] { 10 }
        };
        Object[][] right = new Object[][] {
                new Object[] { 1, 1},
                new Object[] { 10 }
        };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intIntArray, left),
                Literal.newLiteral(intIntArray, right));

        assertLiteralSymbol(op.normalizeSymbol(function), false);
        assertThat(eq(intIntArray, left, right), is(false));
    }

    @Test
    public void testNormalizeAndEvalTwoEqualArraysShouldReturnTrueLiteral() throws Exception {
        ArrayType intArray = new ArrayType(DataTypes.INTEGER);
        CmpOperator op = getOp(intArray);

        Object[] left = new Object[] { 1, 1, 10 };
        Object[] right = new Object[] { 1, 1, 10 };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intArray, left),
                Literal.newLiteral(intArray, right));

        assertLiteralSymbol(op.normalizeSymbol(function), true);
        assertThat(eq(intArray, left, right), is(true));
    }

    @Test
    public void testNormalizeAndEvalTwoNotEqualArraysShouldReturnFalse() throws Exception {
        ArrayType intArray = new ArrayType(DataTypes.INTEGER);
        CmpOperator op = getOp(intArray);

        Object[] left = new Object[] { 1, 1, 10 };
        Object[] right = new Object[] { 1, 10 };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intArray, left),
                Literal.newLiteral(intArray, right));

        assertLiteralSymbol(op.normalizeSymbol(function), false);
        assertThat(eq(intArray, left, right), is(false));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldReturnFalse() throws Exception {
        ArrayType intArray = new ArrayType(DataTypes.INTEGER);
        CmpOperator op = getOp(intArray);

        Object[] left = new Object[] { 1, 1, 10 };
        Object[] right = new Object[] { 1, 2, 10 };
        Function function = createFunction(EqOperator.NAME, DataTypes.BOOLEAN,
                Literal.newLiteral(intArray, left),
                Literal.newLiteral(intArray, right));

        assertLiteralSymbol(op.normalizeSymbol(function), false);
        assertThat(eq(intArray, left, right), is(false));
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        CmpOperator op = getOp(DataTypes.INTEGER);
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.NULL, Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.UNDEFINED, result.valueType());
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        CmpOperator op = getOp(DataTypes.INTEGER);
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.NULL));
        Literal result = (Literal)op.normalizeSymbol(function);
        assertNull(result.value());
        assertEquals(DataTypes.UNDEFINED, result.valueType());
    }

    @Test
    public void testNormalizeSymbolNeq() {
        CmpOperator op = getOp(DataTypes.INTEGER);

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), Literal.newLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertLiteralSymbol(result, false);
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        CmpOperator op = getOp(DataTypes.INTEGER);
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
        CmpOperator op = getOp(type);
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
