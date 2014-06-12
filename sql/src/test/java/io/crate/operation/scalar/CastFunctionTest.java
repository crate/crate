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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CastFunctionTest {

    private Functions functions;

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {

        FunctionImplementation castStringToInteger = functions.get(new FunctionIdent(CastFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.INTEGER)));

        Function function = new Function(castStringToInteger.info(), Arrays.<Symbol>asList(Literal.newLiteral("123"), Literal.newLiteral(0)));
        Symbol result = castStringToInteger.normalizeSymbol(function);
        assertLiteralSymbol(result, 123);

        FunctionImplementation castIntegerToString = functions.get(new FunctionIdent(CastFunction.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.STRING)));

        function = new Function(castIntegerToString.info(), Arrays.<Symbol>asList(Literal.newLiteral(123), Literal.newLiteral("")));
        result = castIntegerToString.normalizeSymbol(function);
        assertLiteralSymbol(result, "123");
    }

    @Test (expected = NumberFormatException.class)
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbolInvalidCast() throws Exception {
        FunctionImplementation castStringToInteger = functions.get(new FunctionIdent(CastFunction.NAME, ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.INTEGER)));
        Function function = new Function(castStringToInteger.info(), Arrays.<Symbol>asList(Literal.newLiteral("cratedata"), Literal.newLiteral(0)));
        castStringToInteger.normalizeSymbol(function);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        FunctionIdent ident = new FunctionIdent(CastFunction.NAME,
                                ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.INTEGER));

        Scalar<Object, Object> format = (Scalar<Object, Object>) functions.get(ident);
        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("123");
            }
        };
        Object result = format.evaluate(arg1);
        assertThat((Integer)result, is(123));

        ident = new FunctionIdent(CastFunction.NAME,
                                ImmutableList.<DataType>of(DataTypes.FLOAT, DataTypes.INTEGER));

        format = (Scalar<Object, Object>) functions.get(ident);
        arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return 1.2f;
            }
        };
        result = format.evaluate(arg1);
        assertThat((Integer)result, is(1));
    }

    @Test (expected = NumberFormatException.class)
    @SuppressWarnings("unchecked")
    public void testEvaluateInvalidCast() throws Exception {
        FunctionIdent ident = new FunctionIdent(CastFunction.NAME,
                                ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.INTEGER));

        Scalar<Object, Object> format = (Scalar<Object, Object>) functions.get(ident);
        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("cratedata");
            }
        };

        format.evaluate(arg1);
    }
}
