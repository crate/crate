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
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ToIntFunctionTest {

    private Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {

        FunctionImplementation castStringToInteger = functions.get(new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING)));

        Function function = new Function(castStringToInteger.info(), Arrays.<Symbol>asList(Literal.newLiteral("123")));
        Symbol result = castStringToInteger.normalizeSymbol(function);
        assertLiteralSymbol(result, 123);

        FunctionImplementation castFloatToInteger = functions.get(new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.FLOAT)));

        function = new Function(castFloatToInteger.info(), Arrays.<Symbol>asList(Literal.newLiteral(12.5f)));
        result = castStringToInteger.normalizeSymbol(function);
        assertLiteralSymbol(result, 12);
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion");
        functions.get(new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }

    @Test
    public void testNormalizeInvalidString() throws Exception {
        expectedException.expect(NumberFormatException.class);
        expectedException.expectMessage("For input string: \"hello\"");
        FunctionImplementation castStringToInteger = functions.get(new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING)));
        Function function = new Function(castStringToInteger.info(), Arrays.<Symbol>asList(Literal.newLiteral("hello")));
        castStringToInteger.normalizeSymbol(function);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        FunctionIdent ident = new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING));
        Scalar<Object, Object> format = (Scalar<Object, Object>) functions.get(ident);
        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return "123";
            }
        };
        Object result = format.evaluate(arg1);
        assertThat((Integer)result, is(123));

        ident = new FunctionIdent(ToIntFunction.NAME, ImmutableList.<DataType>of(DataTypes.FLOAT));
        format = (Scalar<Object, Object>) functions.get(ident);
        arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return 42.5f;
            }
        };
        result = format.evaluate(arg1);
        assertThat((Integer)result, is(42));
    }

}
