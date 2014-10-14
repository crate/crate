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
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class ToLongArrayFunctionTest {

    private Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

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

        actual = eval(Arrays.asList("10", "20", "30"), DataTypes.STRING);
        assertThat(actual, is(expected));

        actual = eval(Arrays.asList(new BytesRef("10"), new BytesRef("20"), new BytesRef("30")), DataTypes.STRING);
        assertThat(actual, is(expected));
        actual = eval(Arrays.asList( 10, 20, 30 ), DataTypes.INTEGER);
        assertThat(actual, is(expected));
    }

    @Test
    public void testInvalidValueToInt() throws Exception {
        expectedException.expect(NumberFormatException.class);
        expectedException.expectMessage("For input string: \"foobar\"");
        eval(Arrays.asList("foobar", "20", "30"), DataTypes.STRING);
    }

    @Test
    public void testWithNullValueToInt() throws Exception {
        Object[] expected = new Long[] { 10L, null, 30L };
        Object[] actual;
        actual = eval(Arrays.asList("10", null, "30"), DataTypes.STRING);
        assertThat(actual, is(expected));
    }

    @Test
    public void testNormalizeWithReference() throws Exception {
        final DataType arrayType = new ArrayType(DataTypes.STRING);
        ToLongArrayFunction impl = (ToLongArrayFunction)functions.get(
                new FunctionIdent(ToLongArrayFunction.NAME, ImmutableList.of(arrayType)));

        Reference foo = TestingHelpers.createReference("foo", arrayType);
        Symbol symbol = impl.normalizeSymbol(new Function(impl.info(), ImmutableList.<Symbol>of(foo)));
        assertThat(symbol, instanceOf(Function.class));
    }

    @Test
    public void testNullToInt() throws Exception {
        assertThat(eval(null, DataTypes.LONG), Matchers.nullValue());
        assertThat(eval(null, DataTypes.STRING), Matchers.nullValue());
    }

    private Object[] eval(final Object objects, DataType innerType) {
        final DataType arrayType = new ArrayType(innerType);
        ToLongArrayFunction impl = (ToLongArrayFunction)functions.get(
                new FunctionIdent(ToLongArrayFunction.NAME, ImmutableList.of(arrayType)));

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
        Symbol normalized = impl.normalizeSymbol(new Function(impl.info(), ImmutableList.<Symbol>of(input)));
        Object[] values = impl.evaluate(new Input[]{input});

        assertThat(values, is(((Input) normalized).value()));
        return values;
    }
}