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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;

public class ToBooleanArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testArrayDifferentTypesToBoolean() throws Exception {
        Object[] expected = new Boolean[] { Boolean.TRUE, Boolean.FALSE };
        Object[] actual;

        actual = eval(new String[]{"t", "f"}, DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(new BytesRef[]{new BytesRef("t"), new BytesRef("f")}, DataTypes.STRING);
        assertThat(actual, is(expected));

        actual = eval(new Long[] { 1L, 0L }, DataTypes.LONG);
        assertThat(actual, is(expected));
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion to 'boolean_array'");
        functions.get(new FunctionIdent(CastFunctionResolver.FunctionNames.TO_BOOLEAN_ARRAY, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }


    private Object[] eval(final Object objects, DataType innerType) {
        final DataType arrayType = new ArrayType(innerType);
        ToArrayFunction impl = (ToArrayFunction)functions.get(
                new FunctionIdent(CastFunctionResolver.FunctionNames.TO_BOOLEAN_ARRAY, ImmutableList.of(arrayType)));

        Literal input = new Literal() {
            @Override
            public Object value() {
                return objects;
            }

            @Override
            public DataType valueType() {
                return arrayType;
            }
        };
        Symbol normalized = impl.normalizeSymbol(new Function(impl.info(), Arrays.<Symbol>asList(input)));
        Object[] integers = impl.evaluate(new Input[]{input});

        assertThat(integers, is(((Input) normalized).value()));
        return integers;
    }
}