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

import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SubscriptFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluate() throws Exception {
        final Literal<Object[]> term = Literal.newLiteral(
                new BytesRef[]{ new BytesRef("Youri"), new BytesRef("Ruben") },
                new ArrayType(DataTypes.STRING));
        final Literal<Integer> termIndex = Literal.newLiteral(1);
        final BytesRef expected = new BytesRef("Youri");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("names", term.valueType()),
                termIndex
        );
        Function function = createFunction(SubscriptFunction.NAME, DataTypes.STRING, arguments);
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Input[] args = new Input[2];
        args[0] = new Input<Object>() {
            @Override
            public Object value() {
                return term.value();
            }
        };
        args[1] = new Input<Object>() {
            @Override
            public Object value() {
                return termIndex.value();
            }
        };

        assertEquals(expected, subscriptFunction.evaluate(args));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        final Literal<Object[]> term = Literal.newLiteral(
                new BytesRef[]{ new BytesRef("Youri"), new BytesRef("Ruben") },
                new ArrayType(DataTypes.STRING));
        final Literal<Integer> termIndex = Literal.newLiteral(1);
        final BytesRef expected = new BytesRef("Youri");
        List<Symbol> arguments = Arrays.<Symbol>asList(
                term,
                termIndex
        );
        Function function = createFunction(SubscriptFunction.NAME, DataTypes.STRING, arguments);
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Symbol result = subscriptFunction.normalizeSymbol(function);
        assertLiteralSymbol(result, expected.utf8ToString());

        arguments = Arrays.<Symbol>asList(
                createReference("text", term.valueType()),
                termIndex
        );
        function = createFunction(SubscriptFunction.NAME, DataTypes.STRING, arguments);
        subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        result = subscriptFunction.normalizeSymbol(function);
        assertThat(result, instanceOf(Function.class));
        assertThat((Function)result, is(function));
    }

    @Test
    public void testIndexOutOfRange() throws Exception {
        final Literal<Object[]> term = Literal.newLiteral(
                new BytesRef[]{ new BytesRef("Youri"), new BytesRef("Ruben") },
                new ArrayType(DataTypes.STRING));
        List<Symbol> arguments = Arrays.<Symbol>asList(
                term,
                Literal.newLiteral(3)
        );
        Function function = createFunction(SubscriptFunction.NAME, DataTypes.STRING, arguments);
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Symbol result = subscriptFunction.normalizeSymbol(function);
        assertThat(result, isLiteral(null, DataTypes.STRING));
    }

}
