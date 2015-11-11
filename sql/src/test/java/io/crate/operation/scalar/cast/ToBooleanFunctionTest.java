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
import static org.hamcrest.core.Is.is;

public class ToBooleanFunctionTest extends AbstractScalarFunctionsTest {

    private final String functionName = CastFunctionResolver.FunctionNames.TO_BOOLEAN;

    private ToPrimitiveFunction getFunction(DataType type) {
        return (ToPrimitiveFunction) functions.get(new FunctionIdent(functionName, Collections.singletonList(type)));
    }

    private Boolean evaluate(Object value, DataType type) {
        Input[] input = {(Input)Literal.newLiteral(type, value)};
        return (Boolean) getFunction(type).evaluate(input);
    }

    private Symbol normalize(Object value, DataType type) {
        ToPrimitiveFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Collections.<Symbol>singletonList(Literal.newLiteral(type, value))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        assertThat(normalize("f", DataTypes.STRING), isLiteral(false));
        assertThat(normalize("t", DataTypes.STRING), isLiteral(true));
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
        expectedException.expectMessage("cannot cast 'hello' to boolean");
        assertThat(normalize("hello", DataTypes.STRING), isLiteral(123L));
    }
}
