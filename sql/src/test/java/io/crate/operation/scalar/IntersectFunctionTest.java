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

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class IntersectFunctionTest {

    private Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder().add(new ScalarFunctionModule())
                .createInjector()
                .getInstance(Functions.class);
    }

    @Test
    public void testDifferentInnerType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Inner types of arrays in intersection must match");
        getFunction(new ArrayType(DataTypes.INTEGER), new ArrayType(DataTypes.LONG));
    }

    @Test
    public void testInvalidArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Argument 1 of intersection must be an array, not integer");
        getFunction(DataTypes.INTEGER, DataTypes.STRING);
    }

    @Test
    public void testEvaluateWithNullArgs() throws Exception {
        DataType type = new ArrayType(DataTypes.INTEGER);
        IntersectFunction function = getFunction(type, type);
        assertThat(function.evaluate(new Input[]{
                Literal.newLiteral(type, null),
                Literal.newLiteral(type, new Integer[]{10, 20})
        }), nullValue());

        assertThat(function.evaluate(new Input[]{
                Literal.newLiteral(type, new Integer[]{10, 20}),
                Literal.newLiteral(type, null)
        }), nullValue());
    }

    @Test
    public void testEvaluateWithTwoArraysThatIntersect() throws Exception {
        Object[] result =  evaluateArrayAndList(
                new Integer[]{20, 10},
                new Integer[]{4, 2, 10, 1, 3}
        );
        Integer[] integers = Arrays.copyOf(result, result.length, Integer[].class);
        assertThat(integers, Matchers.arrayContaining(10));
    }

    @Test
    public void testEvaluateWithArraysAllIntersect() throws Exception {
        Object[] result =  evaluateArrayAndList(
                new Integer[]{30, 80, 20},
                new Integer[]{20, 30, 80}
        );
        Integer[] integers = Arrays.copyOf(result, result.length, Integer[].class);
        assertThat(integers, Matchers.arrayContainingInAnyOrder(20, 30, 80));
    }

    @Test
    public void testEvaluateWithArraysNoIntersect() throws Exception {
        Object[] result =  evaluateArrayAndList(
                new Integer[]{3, 8, 5},
                new Integer[]{2, 6, 4}
        );
        Integer[] integers = Arrays.copyOf(result, result.length, Integer[].class);
        assertThat(integers, Matchers.emptyArray());
    }

    @Test
    public void testEvaluateWithDuplicateItems() throws Exception {
        Object[] result =  evaluateArrayAndList(
                new Integer[]{30, 30, 80, 20},
                new Integer[]{20, 30, 80}
        );
        Integer[] integers = Arrays.copyOf(result, result.length, Integer[].class);
        assertThat(integers, Matchers.arrayContainingInAnyOrder(20, 30, 80));
    }

    @Test
    public void testEvaluateWithNullItem() throws Exception {
        Object[] result =  evaluateArrayAndList(
                new Integer[]{30, 30, 80, 20, null},
                new Integer[]{20, null, 30, 80}
        );
        Integer[] integers = Arrays.copyOf(result, result.length, Integer[].class);
        assertThat(integers, Matchers.arrayContainingInAnyOrder(null, 20, 30, 80));
    }

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        ArrayType type = new ArrayType(DataTypes.INTEGER);
        IntersectFunction function = getFunction(type, type);

        Symbol symbol = function.normalizeSymbol(new Function(function.info(), Arrays.<Symbol>asList(
                Literal.newLiteral(type, new Integer[]{10, 20}),
                Literal.newLiteral(type, new Integer[]{10, 30})
        )));

        TestingHelpers.assertLiteralSymbol(symbol, new Integer[] { 10 }, type);
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        ArrayType type = new ArrayType(DataTypes.INTEGER);
        IntersectFunction function = getFunction(type, type);
        Function functionSymbol = new Function(function.info(), Arrays.<Symbol>asList(
                TestingHelpers.createReference("foo", type),
                Literal.newLiteral(type, new Integer[]{10, 30})
        ));
        Function symbol = (Function) function.normalizeSymbol(functionSymbol);
        assertThat(symbol, Matchers.sameInstance(functionSymbol));
    }

    @Test
    public void testEvaluateWithTwoLists() throws Exception {
        IntersectFunction function = getFunction(
                new ArrayType(DataTypes.INTEGER), new ArrayType(DataTypes.INTEGER));

        Object[] evaluate = function.evaluate(new Input[]{
                new Input() {
                    @Override
                    public Object value() {
                        return Arrays.asList(10, 20, 30);
                    }
                },
                new Input() {
                    @Override
                    public Object value() {
                        return Arrays.asList(10, 2);
                    }
                }
        });
        Integer[] integers = Arrays.copyOf(evaluate, evaluate.length, Integer[].class);
        assertThat(integers, Matchers.arrayContaining(10));
    }

    private Object[] evaluateArrayAndList(final Integer[] arg1, final Integer[] arg2) {
        DataType type1 = DataTypes.guessType(arg1);
        DataType type2 = DataTypes.guessType(arg2);
        IntersectFunction function = getFunction(type1, type2);
        Object[] result1 =  function.evaluate(
                new Input[] {
                        Literal.newLiteral(type1, arg1),
                        Literal.newLiteral(type2, arg2)
                }
        );

        Object[] result = function.evaluate(new Input[] {
                new Input() {
                    @Override
                    public Object value() {
                        return Arrays.asList(arg1);
                    }
                },
                new Input() {
                    @Override
                    public Object value() {
                        return Arrays.asList(arg2);
                    }
                }
        });

        assertArrayEquals(result, result1);
        return result1;
    }

    private IntersectFunction getFunction(DataType typeA, DataType typeB) {
        return (IntersectFunction) functions.get(
                new FunctionIdent(IntersectFunction.NAME, Arrays.asList(typeA, typeB)));
    }
}