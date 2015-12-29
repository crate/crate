/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.core.Is.is;

public class FormatFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("format('%tY', cast('2014-03-02' as timestamp))");
        FunctionImplementation format = getFunctionFromArgs(FormatFunction.NAME, function.arguments());
        Symbol result = format.normalizeSymbol(function);

        assertThat(result, isLiteral("2014"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("format('%s bla %s', name, age)");
        Scalar<BytesRef, Object> format = (Scalar<BytesRef, Object>) functions.get(function.info().ident());

        Input<Object> arg1 = ((Literal) function.arguments().get(0));
        Input arg2 = Literal.newLiteral("Arthur");
        Input arg3 = Literal.newLiteral(38L);

        BytesRef result = format.evaluate(arg1, arg2, arg3);
        assertThat(result.utf8ToString(), is("Arthur bla 38"));

        arg3 = Literal.newLiteral(42L);

        result = format.evaluate(arg1, arg2, arg3);
        assertThat(result.utf8ToString(), is("Arthur bla 42"));

    }
}
