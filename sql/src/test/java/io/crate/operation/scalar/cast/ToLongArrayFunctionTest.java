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
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.StmtCtx;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ToLongArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testArrayDifferentTypesToInt() throws Exception {
        Object[] expected = new Long[] { 10L, 20L, 30L };
        Object[] actual;

        actual = eval(new String[]{"10", "20", "30"}, DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(new BytesRef[]{new BytesRef("10"), new BytesRef("20"), new BytesRef("30")}, DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(new Integer[] { 10, 20, 30 }, DataTypes.INTEGER);
        assertThat(actual, is(expected));
    }

    @Test
    public void testListDifferentTypesToInt() throws Exception {
        Object[] expected = new Long[] { 10L, 20L, 30L };
        Object[] actual;

        actual = eval($("10", "20", "30"), DataTypes.STRING);
        assertThat(actual, is(expected));

        actual = eval($(new BytesRef("10"), new BytesRef("20"), new BytesRef("30")), DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval($( 10, 20, 30 ), DataTypes.INTEGER);
        assertThat(actual, is(expected));
    }

    @Test
    public void testInvalidValueToInt() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("cannot cast ['foobar', '20', '30'] to type long_array");
        eval($("foobar", "20", "30"), DataTypes.STRING);
    }

    @Test
    public void testWithNullValueToInt() throws Exception {
        Object[] expected = new Long[] { 10L, null, 30L };
        Object[] actual;
        actual = eval($("10", null, "30"), DataTypes.STRING);
        assertThat(actual, is(expected));
    }

    @Test
    public void testNormalizeWithReference() throws Exception {
        final DataType arrayType = new ArrayType(DataTypes.STRING);
        ToArrayFunction impl = (ToArrayFunction) functions.get(
                new FunctionIdent(CastFunctionResolver.FunctionNames.TO_LONG_ARRAY, ImmutableList.of(arrayType)));

        Reference foo = TestingHelpers.createReference("foo", arrayType);
        Symbol symbol = impl.normalizeSymbol(new Function(impl.info(), Collections.<Symbol>singletonList(foo)), new StmtCtx());
        assertThat(symbol, instanceOf(Function.class));
    }

    @Test
    public void testNullToInt() throws Exception {
        assertThat(eval(null, DataTypes.LONG), Matchers.nullValue());
        assertThat(eval(null, DataTypes.STRING), Matchers.nullValue());
    }

    private Object[] eval(final Object objects, DataType innerType) {
        return ArrayCastTest.evalArrayCast(functions, CastFunctionResolver.FunctionNames.TO_LONG_ARRAY, objects, innerType);
    }
}
