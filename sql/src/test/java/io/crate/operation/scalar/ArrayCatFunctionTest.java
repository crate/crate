/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.metadata.StmtCtx;
import io.crate.operation.Input;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.is;

public class ArrayCatFunctionTest extends AbstractScalarFunctionsTest {

    private static final ArrayType arrayOfIntegerType = new ArrayType(DataTypes.INTEGER);
    private static final ArrayType arrayOfLongType = new ArrayType(DataTypes.LONG);
    private static final ArrayType arrayOfStringType = new ArrayType(DataTypes.STRING);
    private static final ArrayType arrayOfBooleanType = new ArrayType(DataTypes.BOOLEAN);
    private static final ArrayType arrayOfIpType = new ArrayType(DataTypes.IP);
    private static final ArrayType arrayOfUndefinedType = new ArrayType(DataTypes.UNDEFINED);

    private final StmtCtx stmtCtx = new StmtCtx();

    private ArrayCatFunction getFunction(ArrayType... args) {
        List<DataType> argumentTypes = new ArrayList<>(args.length);
        for (int i = 0; i < args.length; i++) {
            argumentTypes.add(args[i]);
        }
        ArrayCatFunction function = ((ArrayCatFunction) functions.get(new FunctionIdent(ArrayCatFunction.NAME, argumentTypes)));
        return function;
    }

    private void assertEval(Object[] expected, Literal... args) {
        List<DataType> argumentTypes = new ArrayList<>(args.length);
        Input[] inputs = new Input[args.length];
        for (int i = 0; i < args.length; i++) {
            inputs[i] = args[i];
            argumentTypes.add(args[i].valueType());
        }
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ArrayCatFunction.NAME, argumentTypes)));
        Object[] evaluate = (Object[]) scalar.evaluate(inputs);
        assertThat(evaluate, is(expected));
    }

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        ArrayCatFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Symbol symbol = function.normalizeSymbol(new Function(function.info(), Arrays.<Symbol>asList(
            Literal.of(new Integer[]{10, 20}, arrayOfIntegerType),
            Literal.of(new Integer[]{10, 30}, arrayOfIntegerType)
        )), stmtCtx);

        assertThat(symbol, isLiteral(new Integer[]{10, 20, 10, 30}, arrayOfIntegerType));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        ArrayCatFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Function functionSymbol = new Function(function.info(), Arrays.<Symbol>asList(
            TestingHelpers.createReference("foo", arrayOfIntegerType),
            Literal.of(new Integer[]{10, 30}, arrayOfIntegerType)
        ));
        Function symbol = (Function) function.normalizeSymbol(functionSymbol, stmtCtx);
        assertThat(symbol, Matchers.sameInstance(functionSymbol));
    }

    @Test
    public void testNullArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 2 of the array_cat function cannot be converted to array");
        assertEval(
            null,
            Literal.of(new Object[]{1, 2, 3}, arrayOfIntegerType),
            Literal.NULL);
    }

    @Test
    public void testNullArgumentsWithoutCheckingForArrays() throws Exception {
        ArrayCatFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
            Literal.NULL,
            null
        };

        Object[] expected = new Object[]{};
        Object[] evaluate = function.evaluate(inputs);
        assertThat(evaluate, is(expected));
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array_cat function requires 2 arguments");
        assertEval(null);
    }

    @Test
    public void testOneArgument() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array_cat function requires 2 arguments");
        assertEval(new Object[]{}, Literal.of(new Object[]{1}, new ArrayType(DataTypes.INTEGER)));
    }

    @Test
    public void testThreeArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array_cat function requires 2 arguments");
        assertEval(new Object[]{},
            Literal.of(new Object[]{1}, new ArrayType(DataTypes.INTEGER)),
            Literal.of(new Object[]{2}, new ArrayType(DataTypes.INTEGER)),
            Literal.of(new Object[]{3}, new ArrayType(DataTypes.INTEGER)));
    }

    @Test
    public void testDifferentConvertableInnerTypes() throws Exception {
        assertEval(
            new Object[]{1, 1},
            Literal.of(new Object[]{1}, arrayOfIntegerType),
            Literal.of(new Object[]{1L}, arrayOfLongType));
    }

    @Test
    public void testStringToNumberCast() throws Exception {
        assertEval(
            new Object[]{1, 2},
            Literal.of(new Object[]{1}, arrayOfIntegerType),
            Literal.of(new Object[]{"2"}, arrayOfStringType));
    }

    @Test
    public void testNumberToStringCast() throws Exception {
        assertEval(
            new Object[]{new BytesRef("2"), new BytesRef("1")},
            Literal.of(new Object[]{"2"}, arrayOfStringType),
            Literal.of(new Object[]{1}, arrayOfIntegerType));
    }

    @Test
    public void testConvertNonNumericStringToNumber() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertEval(
            null,
            Literal.of(new Object[]{1}, arrayOfIntegerType),
            Literal.of(new Object[]{"foo", "bar"}, arrayOfStringType));
    }

    @Test
    public void testDifferentUnconvertableInnerTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Second argument's inner type (ip) of the array_cat function cannot be converted to the first argument's inner type (boolean)");
        assertEval(
            null,
            Literal.of(new Object[]{true}, arrayOfBooleanType),
            Literal.of(new Object[]{new BytesRef("127.0.0.1")}, arrayOfIpType));

    }

    @Test
    public void testNullElements() throws Exception {
        assertEval(
            new Object[]{1, null, 3, null, 2, 3},
            Literal.of(new Object[]{1, null, 3}, arrayOfIntegerType),
            Literal.of(new Object[]{null, 2, 3}, arrayOfIntegerType));
    }

    @Test
    public void testTwoIntegerArguments() throws Exception {
        assertEval(
            new Object[]{1, 2, 2, 3},
            Literal.of(new Object[]{1, 2}, arrayOfIntegerType),
            Literal.of(new Object[]{2, 3}, arrayOfIntegerType));
    }

    @Test
    public void testTwoLongArguments() throws Exception {
        assertEval(
            new Object[]{44L, 55L, 55L, 66L},
            Literal.of(new Object[]{44L, 55L}, arrayOfLongType),
            Literal.of(new Object[]{55L, 66L}, arrayOfLongType));
    }

    @Test
    public void testTwoStringArguments() throws Exception {
        assertEval(
            new Object[]{new BytesRef("foo"), new BytesRef("bar"), new BytesRef("bar"), new BytesRef("baz")},
            Literal.of(new Object[]{"foo", "bar"}, arrayOfStringType),
            Literal.of(new Object[]{"bar", "baz"}, arrayOfStringType));
    }

    @Test
    public void testEmptyArrayAndIntegerArray() throws Exception {
        assertEval(
            new Object[]{1, 2},
            Literal.of(new Object[]{}, arrayOfUndefinedType),
            Literal.of(new Object[]{1, 2}, arrayOfIntegerType));
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the array_cat function can be of undefined inner type, but not both");
        assertEval(null,
            Literal.of(new Object[]{}, arrayOfUndefinedType),
            Literal.of(new Object[]{}, arrayOfUndefinedType));
    }
}
