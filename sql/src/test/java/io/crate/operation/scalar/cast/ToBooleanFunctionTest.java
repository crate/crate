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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ToBooleanFunctionTest {

    private Functions functions;

    private String functionName = ToBooleanFunction.NAME;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }


    private ToBooleanFunction getFunction(DataType type) {
        return (ToBooleanFunction) functions.get(new FunctionIdent(functionName, Arrays.asList(type)));
    }

    private Boolean evaluate(Object value, DataType type) {
        Input[] input = {(Input)Literal.newLiteral(type, value)};
        return getFunction(type).evaluate(input);
    }

    private Symbol normalize(Object value, DataType type) {
        ToBooleanFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, value))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        TestingHelpers.assertLiteralSymbol(normalize("f", DataTypes.STRING), Boolean.FALSE);
        TestingHelpers.assertLiteralSymbol(normalize("t", DataTypes.STRING), Boolean.TRUE);
    }

    @Test
    public void testEvaluate() throws Exception {
        assertThat(evaluate(1, DataTypes.INTEGER), is(Boolean.TRUE));
        assertThat(evaluate(0, DataTypes.INTEGER), is(Boolean.FALSE));

        assertThat(evaluate("true", DataTypes.STRING), is(Boolean.TRUE));
        assertThat(evaluate("false", DataTypes.STRING), is(Boolean.FALSE));

        assertThat(evaluate(0.1f, DataTypes.FLOAT), is(Boolean.TRUE));
        assertThat(evaluate(0.0d, DataTypes.DOUBLE), is(Boolean.FALSE));
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion");
        functions.get(new FunctionIdent(functionName, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }

    @Test
    public void testNormalizeInvalidString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't convert \"hello\" to boolean");
        TestingHelpers.assertLiteralSymbol(normalize("hello", DataTypes.STRING), 123L);
    }
}
