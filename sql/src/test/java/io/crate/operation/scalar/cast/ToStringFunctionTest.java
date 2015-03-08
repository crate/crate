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
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.core.Is.is;

public class ToStringFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {

        FunctionImplementation castIntegerToString = functions.get(new FunctionIdent(ToStringFunction.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER)));

        Function function = new Function(castIntegerToString.info(), Arrays.<Symbol>asList(Literal.newLiteral(123)));
        Symbol result = castIntegerToString.normalizeSymbol(function);
        assertLiteralSymbol(result, "123");

        FunctionImplementation castFloatToString = functions.get(new FunctionIdent(ToStringFunction.NAME, ImmutableList.<DataType>of(DataTypes.FLOAT)));
        function = new Function(castFloatToString.info(), Arrays.<Symbol>asList(Literal.newLiteral(0.5f)));
        result = castFloatToString.normalizeSymbol(function);
        assertLiteralSymbol(result, "0.5");

        FunctionImplementation castStringToString = functions.get(new FunctionIdent(ToStringFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING)));
        function = new Function(castStringToString.info(), Arrays.<Symbol>asList(Literal.newLiteral("hello")));
        result = castStringToString.normalizeSymbol(function);
        assertLiteralSymbol(result, "hello");
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion");
        functions.get(new FunctionIdent(ToStringFunction.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        FunctionIdent ident = new FunctionIdent(ToStringFunction.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER));
        Scalar<Object, Object> format = (Scalar<Object, Object>) functions.get(ident);
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
