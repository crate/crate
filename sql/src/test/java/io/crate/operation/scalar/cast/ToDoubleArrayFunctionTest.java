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

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ToDoubleArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testArrayDifferentTypesToDouble() throws Exception {
        Object[] expected = new Double[]{10.1d, 20.3d, 30.4d};
        Object[] actual;

        actual = eval(new String[]{"10.1", "20.3", "30.4"}, DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(new BytesRef[]{new BytesRef("10.1"), new BytesRef("20.3"), new BytesRef("30.4")}, DataTypes.STRING);
        assertThat(actual, is(expected));

        expected = new Double[]{10.0d, 20.0d, 30.0d};
        actual = eval(new Long[]{10L, 20L, 30L}, DataTypes.LONG);
        assertThat(actual, is(expected));
    }


    private Object[] eval(final Object objects, DataType innerType) {
        return ArrayCastTest.evalArrayCast(functions, CastFunctionResolver.FunctionNames.TO_DOUBLE_ARRAY, objects, innerType);
    }
}
