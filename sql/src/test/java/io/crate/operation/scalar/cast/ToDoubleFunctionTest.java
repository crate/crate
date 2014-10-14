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

package io.crate.operation.scalar.cast;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ToDoubleFunctionTest {

    private Functions functions;

    private String functionName = ToDoubleFunction.NAME;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }


    private ToDoubleFunction getFunction(DataType type) {
        return (ToDoubleFunction) functions.get(new FunctionIdent(functionName, Arrays.asList(type)));
    }

    private Double evaluate(Object value, DataType type) {
        Input[] input = {(Input)Literal.newLiteral(type, value)};
        return getFunction(type).evaluate(input);
    }

    private Symbol normalize(Object value, DataType type) {
        ToDoubleFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, value))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        TestingHelpers.assertLiteralSymbol(normalize("123", DataTypes.STRING), 123d);
        TestingHelpers.assertLiteralSymbol(normalize(12.5f, DataTypes.FLOAT), 12.5d);
    }

    @Test
    public void testEvaluate() throws Exception {
        assertThat(evaluate("123", DataTypes.STRING), is(123d));
        assertThat(evaluate(null, DataTypes.STRING), nullValue());
        assertThat(evaluate(123.5, DataTypes.FLOAT), is(123.5d));
        assertThat(evaluate(123.5d, DataTypes.DOUBLE), is(123.5d));
        assertThat(evaluate(null, DataTypes.FLOAT), nullValue());
        assertThat(evaluate(42L, DataTypes.LONG), is(42d));
        assertThat(evaluate(42, DataTypes.INTEGER), is(42d));
        assertThat(evaluate(null, DataTypes.INTEGER), nullValue());
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid datatype object for double conversion");
        functions.get(new FunctionIdent(functionName, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }

    @Test
    public void testNormalizeInvalidString() throws Exception {
        expectedException.expect(NumberFormatException.class);
        expectedException.expectMessage("For input string: \"hello\"");
        TestingHelpers.assertLiteralSymbol(normalize("hello", DataTypes.STRING), 123L);
    }
}
