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
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.*;

public class ArrayDifferenceFunctionTest extends AbstractScalarFunctionsTest {

    private static final ArrayType arrayOfIntegerType    = new ArrayType(DataTypes.INTEGER);
    private static final ArrayType arrayOfLongType       = new ArrayType(DataTypes.LONG);
    private static final ArrayType arrayOfStringType     = new ArrayType(DataTypes.STRING);
    private static final ArrayType arrayOfBooleanType    = new ArrayType(DataTypes.BOOLEAN);
    private static final ArrayType arrayOfIpType         = new ArrayType(DataTypes.IP);
    private static final ArrayType arrayOfUndefinedType  = new ArrayType(DataTypes.UNDEFINED);

    private ArrayDifferenceFunction getFunction(ArrayType... args) {
        List<DataType> argumentTypes = new ArrayList<>(args.length);
        for (int i = 0; i < args.length; i++) {
            argumentTypes.add(args[i]);
        }
        ArrayDifferenceFunction function = ((ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        return function;
    }

    private void assertEval(Object[] expected, Literal ... args) {
        List<DataType> argumentTypes = new ArrayList<>(args.length);
        Input[] inputs = new Input[args.length];
        for (int i = 0; i < args.length; i++) {
            inputs[i] = args[i];
            argumentTypes.add(args[i].valueType());
        }
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        Object[] evaluate = (Object[]) scalar.evaluate(inputs);
        assertThat(evaluate, is(expected));
    }

    @Test
    public void testCompileWithValues() throws Exception {
        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayOfIntegerType, arrayOfIntegerType);
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral(new Object[]{1, 2, 3},arrayOfIntegerType),
                Literal.newLiteral(new Object[]{3, 4, 5}, arrayOfIntegerType)
        );

        Scalar function = ((ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        Scalar compiled = function.compile(arguments);

        assertThat(compiled, instanceOf(ArrayDifferenceFunction.class));
    }

    @Test
    public void testCompileWithRefs() throws Exception {
        DataType type = new ArrayType(DataTypes.INTEGER);
        List<DataType> argumentTypes = Arrays.<DataType>asList(type, type);
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral(new Object[]{1, 2, 3},type),
                createReference("foo", type)
        );

        Scalar function = ((ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        Scalar compiled = function.compile(arguments);

        assertThat(compiled, sameInstance(function));
    }

    @Test
    public void testCompileWithNullArgs() throws Exception {
        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayOfIntegerType, arrayOfIntegerType);
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral(new Object[]{1, 2, 3},arrayOfIntegerType),
                null
        );

        Scalar function = ((ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        Scalar compiled = function.compile(arguments);

        assertThat(compiled, sameInstance(function));
    }

    @Test
    public void testCompileWithNullArgValues() throws Exception {
        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayOfIntegerType, arrayOfIntegerType);
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral(new Object[]{1, 2, 3}, arrayOfIntegerType),
                Literal.NULL
        );

        Scalar function = ((ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes)));
        Scalar compiled = function.compile(arguments);

        assertThat(compiled, sameInstance(function));
    }

    @Test
    public void testCompiledEvaluation() throws Exception {
        Literal arg1 = Literal.newLiteral(new Object[]{1, 2, 3},arrayOfIntegerType);
        Literal arg2 = Literal.newLiteral(new Object[]{3, 4, 5}, arrayOfIntegerType);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayOfIntegerType, arrayOfIntegerType);
        List<Symbol>   arguments     = Arrays.<Symbol>asList(arg1, arg2);
        Input[]        inputs        = new Input[]{ arg1, arg2};

        ArrayDifferenceFunction function = (ArrayDifferenceFunction) functions.get(new FunctionIdent(ArrayDifferenceFunction.NAME, argumentTypes));
        ArrayDifferenceFunction compiled = (ArrayDifferenceFunction) function.compile(arguments);
        assertThat(compiled, instanceOf(ArrayDifferenceFunction.class));

        Object[] evaluate = compiled.evaluate(inputs);
        Object[] expected = new Object[] {1, 2};
        assertThat(evaluate, is(expected));
    }

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        ArrayType type = new ArrayType(DataTypes.INTEGER);
        ArrayDifferenceFunction function = getFunction(type, type);

        Symbol symbol = function.normalizeSymbol(new Function(function.info(), Arrays.<Symbol>asList(
                Literal.newLiteral(new Integer[]{10, 20}, type),
                Literal.newLiteral(new Integer[]{10, 30}, type)
        )));

        assertThat(symbol, isLiteral(new Integer[]{20}, type));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        ArrayType type = new ArrayType(DataTypes.INTEGER);
        ArrayDifferenceFunction function = getFunction(type, type);

        Function functionSymbol = new Function(function.info(), Arrays.<Symbol>asList(
                createReference("foo", type),
                Literal.newLiteral(new Integer[]{10, 30}, type)
        ));
        Function symbol = (Function) function.normalizeSymbol(functionSymbol);
        assertThat(symbol, Matchers.sameInstance(functionSymbol));
    }

    @Test
    public void testNullArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 2 of the array_difference function cannot be converted to array");
        assertEval(null,
                Literal.newLiteral(new Object[]{1}, arrayOfIntegerType),
                Literal.NULL);
    }

    @Test
    public void testNullArgumentsWithoutCheckingForArrays() throws Exception {
        ArrayDifferenceFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
                Literal.NULL,
                null
        };

        Object[] expected = null;
        Object[] evaluate = function.evaluate(inputs);
        assertThat(evaluate, is(expected));
    }

    @Test
    public void testNullFirstArgumentWithoutCheckingForArrays() throws Exception {
        ArrayDifferenceFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
                null,
                Literal.newLiteral(new Object[]{22, 33, 44}, arrayOfIntegerType)
        };

        Object[] evaluate = function.evaluate(inputs);
        assertNull(evaluate);
    }

    @Test
    public void testNullValueFirstArgumentWithoutCheckingForArrays() throws Exception {
        ArrayDifferenceFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
                Literal.NULL,
                Literal.newLiteral(new Object[]{22, 33, 44}, arrayOfIntegerType)
        };

        Object[] evaluate = function.evaluate(inputs);
        assertNull(evaluate);
    }

    @Test
    public void testNullSecondArgumentWithoutCheckingForArrays() throws Exception {
        ArrayDifferenceFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
                Literal.newLiteral(new Object[]{22, 33, 44}, arrayOfIntegerType),
                null
        };

        Object[] evaluate = function.evaluate(inputs);
        assertThat(evaluate, is(new Object[]{22, 33, 44}));
    }

    @Test
    public void testNullValueSecondArgumentWithoutCheckingForArrays() throws Exception {
        ArrayDifferenceFunction function = getFunction(arrayOfIntegerType, arrayOfIntegerType);

        Input[] inputs = new Input[]{
                Literal.newLiteral(new Object[]{22, 33, 44}, arrayOfIntegerType),
                Literal.NULL
        };

        Object[] evaluate = function.evaluate(inputs);
        assertThat(evaluate, is(new Object[]{22, 33, 44}));
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array_difference function requires 2 arguments");
        assertEval(null);
    }

    @Test
    public void testOneArgument() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array_difference function requires 2 arguments");
        assertEval(null,
                Literal.newLiteral(new Object[]{1}, arrayOfIntegerType));
    }

    @Test
    public void testDifferentBuConvertableInnerTypes() throws Exception {
        assertEval(new Object[]{},
                Literal.newLiteral(new Object[]{1},  arrayOfIntegerType),
                Literal.newLiteral(new Object[]{1L}, arrayOfLongType));
    }

    @Test
    public void testConvertNonNumericStringToNumber() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertEval(
                null,
                Literal.newLiteral(new Object[]{1},              arrayOfIntegerType),
                Literal.newLiteral(new Object[]{"foo","bar"},    arrayOfStringType));
    }

    @Test
    public void testDifferentUnconvertableInnerTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Second argument's inner type (ip) of the array_difference function cannot be converted to the first argument's inner type (boolean)");
        assertEval(
                null,
                Literal.newLiteral(new Object[]{true},                       arrayOfBooleanType),
                Literal.newLiteral(new Object[]{new BytesRef("127.0.0.1")},  arrayOfIpType));

    }

    @Test
    public void testNullElements() throws Exception {
        assertEval(
                new Object[]{1},
                Literal.newLiteral(new Object[]{1,null,3},  arrayOfIntegerType),
                Literal.newLiteral(new Object[]{null, 2,3}, arrayOfIntegerType));
        assertEval(
                new Object[]{1, null, 2, null},
                Literal.newLiteral(new Object[]{1, null, 3, 2, null},  arrayOfIntegerType),
                Literal.newLiteral(new Object[]{3},                    arrayOfIntegerType));
    }

    @Test
    public void testTwoIntegerArguments() throws Exception {
        assertEval(
                new Object[]{1},
                Literal.newLiteral(new Object[]{1,2}, arrayOfIntegerType),
                Literal.newLiteral(new Object[]{2,3}, arrayOfIntegerType));
    }

    @Test
    public void testTwoLongArguments() throws Exception {
        assertEval(
                new Object[]{44L},
                Literal.newLiteral(new Object[]{44L, 55L}, arrayOfLongType),
                Literal.newLiteral(new Object[]{55L, 66L}, arrayOfLongType));
    }

    @Test
    public void testTwoStringArguments() throws Exception {
        assertEval(
                new Object[]{new BytesRef("foo")},
                Literal.newLiteral(new Object[]{"foo","bar"}, arrayOfStringType),
                Literal.newLiteral(new Object[]{"bar","baz"}, arrayOfStringType));
    }

    @Test
    public void testEmptyArrayAndIntegerArray() throws Exception {
        assertEval(
                new Object[]{},
                Literal.newLiteral(new Object[]{},    arrayOfUndefinedType),
                Literal.newLiteral(new Object[]{1,2}, arrayOfIntegerType));
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the array_difference function can be of undefined inner type, but not both");
        assertEval(null,
                Literal.newLiteral(new Object[]{}, arrayOfUndefinedType),
                Literal.newLiteral(new Object[]{}, arrayOfUndefinedType));
    }
}