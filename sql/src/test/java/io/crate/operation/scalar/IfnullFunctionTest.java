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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * first JUnit test class to drive the IfnullFunction class
 *
 * @author: kanisfluh
 * @version: 0.1
 *
 */

public class IfnullFunctionTest extends AbstractScalarFunctionsTest {

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
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(IfnullFunction.NAME, argumentTypes)));

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
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(IfnullFunction.NAME, argumentTypes)));
        @SuppressWarnings("unchecked")
        BytesRef evaluate = (BytesRef) scalar.evaluate(inputs);
        assertThat(evaluate.utf8ToString(), is(expected));
    }


    @Test
    public void testArgumentThatHasNoStringRepr() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 1 of the ifnull function can't be converted to string");
        assertEval("", 2, ImmutableMap.of("foo", "bar"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNormalize() throws Exception {
        List<DataType> argumentTypes = Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING);
        Scalar scalar = ((Scalar) functions.get(new FunctionIdent(IfnullFunction.NAME, argumentTypes)));

        Symbol symbol = scalar.normalizeSymbol(new Function(scalar.info(),
                Arrays.<Symbol>asList(Literal.newLiteral("foo"), Literal.newLiteral("bar"))));
        TestingHelpers.assertLiteralSymbol(symbol, "foo");

        symbol = scalar.normalizeSymbol(new Function(scalar.info(),
                Arrays.<Symbol>asList(TestingHelpers.createReference("col1", DataTypes.STRING), Literal.newLiteral("bar"))));
        assertThat(symbol, TestingHelpers.isFunction(IfnullFunction.NAME));
    }
    



    /**
     * the ifnull function returns expr1 if it is not null,
     *   if expr1 is null, expr2 should be returned
     *
     * these are the possible combinations currently considered:
     *    ifnull(expr1, expr2)     methodName           --> result/returnValue
     *
     * 1) ifnull(null, null);      testNullAndNull         --> (null)
     * 2) ifnull(null, 'str2');    testNullAndString       --> 'str2'
     * 3) ifnull('str1', null);    testStringAndNull       --> 'str1'
     * 4) ifnull('str1', 'str2');  testStringAndString     --> 'str1'
     * 5) ifnull(null, int2);      testNullAndNumber       --> int2
     * 6) ifnull(int1, null);      testNumberAndNull       --> int1
     * 7) ifnull(int1, int2);      testNumberAndNumber     --> int1
     * 8) ifnull('str1', int2);    testStringAndNumber     --> 'str1'
     * 9) ifnull(int1, 'str2');    testNumberAndString     --> int1
     *
     */

    //1 --> expr2 (which, unfortunately is null also. this mimics mysql behaviour.
    @Test
    public void testNullAndNull() throws Exception {
        assertEval("(null)", null, null);
    }

    //2 --> expr2
    @Test
    public void testNullAndString() throws Exception {
        assertEval("bar", null, new BytesRef("bar"));
    }

    //3 --> expr1
    @Test
    public void testStringAndNull() throws Exception {
        assertEval("foo", new BytesRef("foo"), null);
    }

    //4 --> expr1
    @Test
    public void testStringAndString() throws Exception {
        assertEval("foo", new BytesRef("foo"), new BytesRef("bar"));
    }

    //5 --> (expr1)
    @Test
    public void testNullAndNumber() throws Exception {
        assertEval("7", null, 7);
        assertEval("7", null, 7L);
        assertEval("7", null, (short)7);
    }

    //6 --> expr1
    @Test
    public void testNumberAndNull() throws Exception {
        assertEval("1", 1, null);
        assertEval("1", 1L, null);
        assertEval("1", (short)1, null);
    }

    //7 --> expr1
    @Test
    public void testNumberAndNumber() throws Exception {
        assertEval("1", 1, 7);
        assertEval("1", 1L, 7L);
        assertEval("1", (short)1, (short)7);
    }

    //8 --> expr1
    @Test
    public void testStringAndNumber() throws Exception {
        assertEval("foo", new BytesRef("foo"), 7);
        assertEval("foo", new BytesRef("foo"), 7L);
        assertEval("foo", new BytesRef("foo"), (short)7);
    }

    //9 --> expr1
    @Test
    public void testNumberAndString() throws Exception {
        assertEval("1", 1, new BytesRef("bar"));
        assertEval("1", 1L, new BytesRef("bar"));
        assertEval("1", (short)1, new BytesRef("bar"));
    }

    /**
     * everything with less than 2 arguments should be rejected
     */
    @Test
    public void testTooFewArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        assertEval("", 2);
    }

    /**
     * everything with more than 2 arguments should throw an (expected) exception
     */
    @Test (expected = IllegalArgumentException.class)
    public void testManyStrings() throws Exception {
        // what to do here when an exception is expected???
        assertEval("dont care, throw ...",
                new BytesRef("dont care"), null, new BytesRef(","), new BytesRef("throw ..."), null, new BytesRef("!"));
    }

}