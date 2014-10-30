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
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ToBooleanArrayFunctionTest {

    private Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

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
        expectedException.expectMessage("Argument must be an array type");
        functions.get(new FunctionIdent(ToBooleanArrayFunction.NAME, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }


    private Object[] eval(final Object objects, DataType innerType) {
        final DataType arrayType = new ArrayType(innerType);
        ToBooleanArrayFunction impl = (ToBooleanArrayFunction)functions.get(
                new FunctionIdent(ToBooleanArrayFunction.NAME, ImmutableList.of(arrayType)));

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