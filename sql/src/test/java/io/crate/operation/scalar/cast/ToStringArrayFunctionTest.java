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
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.core.Is.is;

public class ToStringArrayFunctionTest extends AbstractScalarFunctionsTest {

    public static final String FUNCTION_NAME = CastFunctionResolver.FunctionNames.TO_STRING_ARRAY;

    @Test
    public void testNormalize() throws Exception {
        List<Symbol> arguments = Collections.<Symbol>singletonList(
                Literal.newLiteral(new Integer[]{1, 2, 3}, new ArrayType(DataTypes.INTEGER))
        );
        BytesRef[] expected = new BytesRef[]{ new BytesRef("1"), new BytesRef("2"), new BytesRef("3") };
        Function function = createFunction(FUNCTION_NAME, new ArrayType(DataTypes.STRING), arguments);
        ToArrayFunction arrayFunction = (ToArrayFunction) functions.get(function.info().ident());

        Symbol result = arrayFunction.normalizeSymbol(function);
        assertThat(result, isLiteral(expected, new ArrayType(DataTypes.STRING)));

        arguments = Collections.<Symbol>singletonList(
                Literal.newLiteral(new Float[]{ 1.0f, 2.0f, 3.0f }, new ArrayType(DataTypes.FLOAT)
        ));
        expected = new BytesRef[]{ new BytesRef("1.0"), new BytesRef("2.0"), new BytesRef("3.0") };
        function = createFunction(FUNCTION_NAME, new ArrayType(DataTypes.STRING), arguments);
        arrayFunction = (ToArrayFunction) functions.get(function.info().ident());

        result = arrayFunction.normalizeSymbol(function);
        assertThat(result, isLiteral(expected, new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testEvaluate() throws Exception {
        Object[] expected = new BytesRef[]{ new BytesRef("1"), new BytesRef("2"), new BytesRef("3") };
        ToArrayFunction arrayFunction = getFunction(FUNCTION_NAME, new ArrayType(DataTypes.INTEGER));

        Input[] args = new Input[1];
        args[0] = new Input<Object>() {
            @Override
            public Object value() {
                return new Integer[]{ 1, 2, 3 };
            }
        };

        Object[] result = arrayFunction.evaluate(args);
        assertThat(result, is(expected));
    }

    @Test
    public void testInvalidArgumentType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'string' not supported for conversion to 'string_array'");
        getFunction(FUNCTION_NAME, DataTypes.STRING);
    }

    @Test
    public void testInvalidArgumentInnerType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object_array' not supported for conversion to 'string_array'");
        getFunction(FUNCTION_NAME, new ArrayType(DataTypes.OBJECT));
    }

}
