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
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ToLongFunctionTest extends AbstractScalarFunctionsTest {

    private String functionName = ToLongFunction.NAME;

    private ToLongFunction getFunction(DataType type) {
        return (ToLongFunction) functions.get(new FunctionIdent(functionName, Arrays.asList(type)));
    }

    private Long evaluate(Object value, DataType type) {
        Input[] input = {(Input)Literal.newLiteral(type, value)};
        return getFunction(type).evaluate(input);
    }

    private Symbol normalize(Object value, DataType type) {
        ToLongFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, value))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        TestingHelpers.assertLiteralSymbol(normalize("123", DataTypes.STRING), 123L);
        TestingHelpers.assertLiteralSymbol(normalize(12.5f, DataTypes.FLOAT), 12L);
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
        expectedException.expectMessage("cannot cast 'hello' to long");
        TestingHelpers.assertLiteralSymbol(normalize("hello", DataTypes.STRING), 123L);
    }

    @Test
    public void testEvaluateOnString() throws Exception {
        assertThat(evaluate("123", DataTypes.STRING), is(123L));
        assertThat(evaluate(null, DataTypes.STRING), nullValue());
    }

    @Test
    public void testEvaluateOnFloatAndDouble() throws Exception {
        assertThat(evaluate(123.5f, DataTypes.FLOAT), is(123L));
        assertThat(evaluate(123.5d, DataTypes.DOUBLE), is(123L));
        assertThat(evaluate(null, DataTypes.FLOAT), nullValue());
        assertThat(evaluate(null, DataTypes.DOUBLE), nullValue());
    }

    @Test
    public void testEvaluateOnIntAndLong() throws Exception {
        assertThat(evaluate(42L, DataTypes.LONG), is(42L));
        assertThat(evaluate(42, DataTypes.INTEGER), is(42L));
        assertThat(evaluate(null, DataTypes.LONG), nullValue());
        assertThat(evaluate(null, DataTypes.INTEGER), nullValue());
    }
}
