/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.is;

public class ConcatFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static class ObjectInput implements Input<Object> {

        private Object value;

        public ObjectInput(Object value) {
            this.value = value;
        }

        @Override
        public Object value() {
            return value;
        }
    }

    private void assertEval(String expected, String arg1, String arg2) {
        List<DataType> argumentTypes = Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING);
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes)));

        Input[] inputs = new Input[2];
        inputs[0] = Literal.newLiteral(arg1);
        inputs[1] = Literal.newLiteral(arg2);
        @SuppressWarnings("unchecked")

        BytesRef evaluate = (BytesRef) scalar.evaluate(inputs);
        assertThat(evaluate.utf8ToString(), is(expected));
    }

    private void assertEval(String expected, Object ... args) {
        List<DataType> argumentTypes = new ArrayList<>(args.length);
        Input[] inputs = new Input[args.length];
        for (int i = 0; i < args.length; i++) {
            inputs[i] = new ObjectInput(args[i]);
            argumentTypes.add(DataTypes.guessType(args[i]));
        }
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes)));
        @SuppressWarnings("unchecked")
        BytesRef evaluate = (BytesRef) scalar.evaluate(inputs);
        assertThat(evaluate.utf8ToString(), is(expected));
    }

    @Test
    public void testTooFewArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        assertEval("", 2);
    }

    @Test
    public void testArgumentThatHasNoStringRepr() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 1 of the concat function can't be converted to string");
        assertEval("", 2, ImmutableMap.of("foo", "bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNormalize() throws Exception {
        List<DataType> argumentTypes = Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING);
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes)));

        Symbol symbol = scalar.normalizeSymbol(new Function(scalar.info(),
                Arrays.<Symbol>asList(Literal.newLiteral("foo"), Literal.newLiteral("bar"))));
        assertThat(symbol, isLiteral("foobar"));

        symbol = scalar.normalizeSymbol(new Function(scalar.info(),
                Arrays.<Symbol>asList(createReference("col1", DataTypes.STRING), Literal.newLiteral("bar"))));
        assertThat(symbol, isFunction(ConcatFunction.NAME));
    }

    @Test
    public void testTwoStringsNullCombinations() throws Exception {
        assertEval("", null, null);
        assertEval("foo", null, "foo");
        assertEval("foo", "foo", null);
    }

    @Test
    public void testTwoStrings() throws Exception {
        assertEval("foobar", "foo", "bar");
        assertEval("foobar", TestingHelpers.addOffset(new BytesRef("foo")),
                             TestingHelpers.addOffset(new BytesRef("bar")));
    }

    @Test
    public void testManyStrings() throws Exception {
        assertEval("foo_testingis_boring",
                new BytesRef("foo"), null, new BytesRef("_"), new BytesRef("testing"), null, new BytesRef("is_boring"));
    }

    @Test
    public void testStringAndNull() throws Exception {
        assertEval("foo", new BytesRef("foo"), null);
    }

    @Test
    public void testNumberAndNull() throws Exception {
        assertEval("5", 5L, null);
    }

    @Test
    public void testStringAndNumber() throws Exception {
        assertEval("foo3", new BytesRef("foo"), 3);
        assertEval("foo3", new BytesRef("foo"), 3L);
        assertEval("foo3", TestingHelpers.addOffset(new BytesRef("foo")), 3L);
        assertEval("foo3", new BytesRef("foo"), (short)3);
    }

    @Test
    public void testTwoArrays() throws Exception{
        ArrayType arrayType = new ArrayType(DataTypes.INTEGER);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayType, arrayType);
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes)));

        Input[] inputs = new Input[2];
        inputs[0] = Literal.newLiteral(new Object[]{1, 2}, arrayType);
        inputs[1] = Literal.newLiteral(new Object[]{2, 3}, arrayType);

        Object[] evaluate = (Object[]) scalar.evaluate(inputs);
        assertThat(evaluate, is(new Object[]{1, 2, 2, 3}));
    }

    @Test
    public void testArrayWithAUndefinedInnerType() throws Exception{
        ArrayType arrayType0 = new ArrayType(DataTypes.UNDEFINED);
        ArrayType arrayType1 = new ArrayType(DataTypes.INTEGER);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayType0, arrayType1);
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes)));

        Input[] inputs = new Input[2];
        inputs[0] = Literal.newLiteral(new Object[]{},     arrayType0);
        inputs[1] = Literal.newLiteral(new Object[]{1, 2}, arrayType1);

        Object[] evaluate = (Object[]) scalar.evaluate(inputs);
        assertThat(evaluate, is(new Object[]{1, 2}));
    }

    @Test
    public void testArrayAndString() throws Exception{
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 0 of the concat function can't be converted to string");
        ArrayType arrayType = new ArrayType(DataTypes.INTEGER);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayType, DataTypes.STRING);
        functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes));
    }

    @Test
    public void testStringAndArray() throws Exception{
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 1 of the concat function can't be converted to string");
        ArrayType arrayType = new ArrayType(DataTypes.INTEGER);

        List<DataType> argumentTypes = Arrays.<DataType>asList(DataTypes.STRING, arrayType);
        functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes));
    }

    @Test
    public void testThirdArgumentArray() throws Exception{
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 2 of the concat function can't be converted to string");
        ArrayType arrayType = new ArrayType(DataTypes.INTEGER);

        List<DataType> argumentTypes = Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, arrayType);
        functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes));
    }

    @Test
    public void testTwoArraysOfIncompatibleInnerTypes() throws Exception{
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Second argument's inner type (integer_array) of the array_cat function cannot be converted to the first argument's inner type (integer)");
        ArrayType arrayType0 = new ArrayType(DataTypes.INTEGER);
        ArrayType arrayType1 = new ArrayType(arrayType0);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayType0, arrayType1);
        functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes));
    }

    @Test
    public void testTwoArraysOfUndefinedTypes() throws Exception{
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("When concatenating arrays, one of the two arguments can be of undefined inner type, but not both");
        ArrayType arrayType = new ArrayType(DataTypes.UNDEFINED);

        List<DataType> argumentTypes = Arrays.<DataType>asList(arrayType, arrayType);
        functions.get(new FunctionIdent(ConcatFunction.NAME, argumentTypes));
    }
}