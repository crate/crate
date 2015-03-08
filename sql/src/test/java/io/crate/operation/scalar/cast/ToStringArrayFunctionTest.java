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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.core.Is.is;

public class ToStringArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalize() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral(new Integer[]{ 1, 2, 3 }, new ArrayType(DataTypes.INTEGER))
        );
        BytesRef[] expected = new BytesRef[]{ new BytesRef("1"), new BytesRef("2"), new BytesRef("3") };
        Function function = createFunction(ToStringArrayFunction.NAME, new ArrayType(DataTypes.STRING), arguments);
        ToStringArrayFunction arrayFunction = (ToStringArrayFunction) functions.get(function.info().ident());

        Symbol result = arrayFunction.normalizeSymbol(function);
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));

        arguments.set(0, Literal.newLiteral(new Float[]{ 1.0f, 2.0f, 3.0f }, new ArrayType(DataTypes.FLOAT)));
        expected = new BytesRef[]{ new BytesRef("1.0"), new BytesRef("2.0"), new BytesRef("3.0") };
        function = createFunction(ToStringArrayFunction.NAME, new ArrayType(DataTypes.STRING), arguments);
        arrayFunction = (ToStringArrayFunction) functions.get(function.info().ident());

        result = arrayFunction.normalizeSymbol(function);
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));
    }

    @Test
    public void testEvaluate() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("int_array", new ArrayType(DataTypes.INTEGER))
        );
        Object[] expected = new BytesRef[]{ new BytesRef("1"), new BytesRef("2"), new BytesRef("3") };
        Function function = createFunction(ToStringArrayFunction.NAME, new ArrayType(DataTypes.STRING), arguments);
        ToStringArrayFunction arrayFunction = (ToStringArrayFunction) functions.get(function.info().ident());

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
        expectedException.expectMessage("Argument must be a collection type");
        functions.get(new FunctionIdent(ToStringArrayFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING)));
    }

    @Test
    public void testInvalidArgumentInnerType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array inner type 'object' not supported for conversion");
        functions.get(new FunctionIdent(ToStringArrayFunction.NAME, ImmutableList.<DataType>of(new ArrayType(DataTypes.OBJECT))));
    }

}
