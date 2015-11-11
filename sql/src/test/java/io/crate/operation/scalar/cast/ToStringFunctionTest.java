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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.core.Is.is;

public class ToStringFunctionTest extends AbstractScalarFunctionsTest {

    private final String functionName = CastFunctionResolver.FunctionNames.TO_STRING;

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {

        FunctionImplementation castIntegerToString = getFunction(functionName, DataTypes.INTEGER);

        Function function = new Function(castIntegerToString.info(), Collections.<Symbol>singletonList(Literal.newLiteral(123)));
        Symbol result = castIntegerToString.normalizeSymbol(function);
        assertThat(result, isLiteral("123"));

        FunctionImplementation castFloatToString = getFunction(functionName, DataTypes.FLOAT);
        function = new Function(castFloatToString.info(), Collections.<Symbol>singletonList(Literal.newLiteral(0.5f)));
        result = castFloatToString.normalizeSymbol(function);
        assertThat(result, isLiteral("0.5"));

        FunctionImplementation castStringToString = getFunction(functionName, DataTypes.STRING);
        function = new Function(castStringToString.info(), Collections.<Symbol>singletonList(Literal.newLiteral("hello")));
        result = castStringToString.normalizeSymbol(function);
        assertThat(result, isLiteral("hello"));
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion");
        getFunction(functionName, DataTypes.OBJECT);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        ToPrimitiveFunction format = getFunction(functionName, DataTypes.INTEGER);
        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return 123;
            }
        };
        Object result = format.evaluate(arg1);
        assertThat(((BytesRef)result), is(new BytesRef("123")));
    }

}
