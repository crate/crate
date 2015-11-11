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

public class ToIntFunctionTest extends AbstractScalarFunctionsTest {

    private final String functionName = CastFunctionResolver.FunctionNames.TO_INTEGER;

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {

        ToPrimitiveFunction castStringToInteger = getFunction(functionName, DataTypes.STRING);

        Function function = new Function(castStringToInteger.info(), Collections.<Symbol>singletonList(Literal.newLiteral("123")));
        Symbol result = castStringToInteger.normalizeSymbol(function);
        assertThat(result, isLiteral(123));

        ToPrimitiveFunction castFloatToInteger = getFunction(functionName, DataTypes.FLOAT);

        function = new Function(castFloatToInteger.info(), Collections.<Symbol>singletonList(Literal.newLiteral(12.5f)));
        result = castStringToInteger.normalizeSymbol(function);
        assertThat(result, isLiteral(12));
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
        expectedException.expectMessage("cannot cast 'hello' to int");
        ToPrimitiveFunction castStringToInteger = getFunction(functionName, DataTypes.STRING);
        Function function = new Function(castStringToInteger.info(), Collections.<Symbol>singletonList(Literal.newLiteral("hello")));
        castStringToInteger.normalizeSymbol(function);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        ToPrimitiveFunction stringFn = getFunction(functionName, DataTypes.STRING);
        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return "123";
            }
        };
        Object result = stringFn.evaluate(arg1);
        assertThat((Integer)result, is(123));

        ToPrimitiveFunction floatFn = getFunction(functionName, DataTypes.FLOAT);
        arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return 42.5f;
            }
        };
        result = floatFn.evaluate(arg1);
        assertThat((Integer)result, is(42));
    }

}
