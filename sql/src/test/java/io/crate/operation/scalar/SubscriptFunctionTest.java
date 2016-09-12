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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.StmtCtx;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SubscriptFunctionTest extends AbstractScalarFunctionsTest {

    private final StmtCtx stmtCtx = new StmtCtx();

    @Test
    public void testEvaluate() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("subscript(['Youri', 'Ruben'], cast(1 as integer))");
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Input[] args = new Input[] {
                ((Input) function.arguments().get(0)),
                ((Input) function.arguments().get(1))
        };
        BytesRef expected = new BytesRef("Youri");
        assertEquals(expected, subscriptFunction.evaluate(args));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("subscript(['Youri', 'Ruben'], cast(1 as integer))");
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Symbol actual = subscriptFunction.normalizeSymbol(function, stmtCtx);
        assertThat(actual, isLiteral(new BytesRef("Youri")));


        function = (Function) sqlExpressions.asSymbol("subscript(tags, cast(1 as integer))");

        Symbol result = subscriptFunction.normalizeSymbol(function, stmtCtx);
        assertThat(result, instanceOf(Function.class));
        assertThat((Function)result, is(function));
    }

    @Test
    public void testIndexOutOfRange() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("subscript(['Youri', 'Ruben'], cast(3 as integer))");
        SubscriptFunction subscriptFunction = (SubscriptFunction) functions.get(function.info().ident());

        Symbol result = subscriptFunction.normalizeSymbol(function, stmtCtx);
        assertThat(result, isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testNotRegisteredForSets() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        FunctionIdent functionIdent = new FunctionIdent(SubscriptFunction.NAME,
            Arrays.<DataType>asList(new SetType(DataTypes.INTEGER), DataTypes.INTEGER));
        functions.get(functionIdent);
    }
}
