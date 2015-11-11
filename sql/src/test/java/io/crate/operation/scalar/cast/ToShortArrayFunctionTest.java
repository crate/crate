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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;

public class ToShortArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testArrayDifferentTypes() throws Exception {
        Object[] expected = new Short[] { 10, 20, 30 };
        Object[] actual;

        actual = eval(new String[]{"10", "20", "30"}, DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(new BytesRef[]{new BytesRef("10"), new BytesRef("20"), new BytesRef("30")}, DataTypes.STRING);
        assertThat(actual, is(expected));

        actual = eval(new Double[] { 10.5d, 20.3d, 30d }, DataTypes.LONG);
        assertThat(actual, is(expected));
    }


    private Object[] eval(final Object objects, DataType innerType) {
        final DataType arrayType = new ArrayType(innerType);
        ToArrayFunction impl = getFunction(CastFunctionResolver.FunctionNames.TO_SHORT_ARRAY, arrayType);

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
        Symbol normalized = impl.normalizeSymbol(new Function(impl.info(), Collections.<Symbol>singletonList(input)));
        Object[] integers = impl.evaluate(input);

        assertThat(integers, is(((Input) normalized).value()));
        return integers;
    }
}