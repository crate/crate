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
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ToLongFunctionTest extends AbstractScalarFunctionsTest {

    private final String functionName = CastFunctionResolver.FunctionNames.TO_LONG;

    private Long evaluate(Object value, DataType type) {
        Input[] input = {(Input)Literal.newLiteral(type, value)};
        ToPrimitiveFunction fn = getFunction(functionName, type);
        return (Long) fn.evaluate(input);
    }

    private Symbol normalize(Object value, DataType type) {
        ToPrimitiveFunction function = getFunction(functionName, type);
        return function.normalizeSymbol(new Function(function.info(),
                Collections.<Symbol>singletonList(Literal.newLiteral(type, value))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        assertThat(normalize("123", DataTypes.STRING), isLiteral(123L));
        assertThat(normalize(12.5f, DataTypes.FLOAT), isLiteral(12L));
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
        assertThat(normalize("hello", DataTypes.STRING), isLiteral(123L));
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
