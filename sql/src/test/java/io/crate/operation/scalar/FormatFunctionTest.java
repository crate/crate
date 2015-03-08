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

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.core.Is.is;

public class FormatFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeSymbol() throws Exception {
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%tY"),
                Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-03-02")));
        Function function = createFunction(FormatFunction.NAME, DataTypes.STRING, args);

        FunctionImplementation format = functions.get(function.info().ident());
        Symbol result = format.normalizeSymbol(function);

        assertLiteralSymbol(result, "2014");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEvaluate() throws Exception {
        final Literal<BytesRef> formatString = Literal.newLiteral("%s bla %s");

        List<Symbol> args = Arrays.<Symbol>asList(
            formatString,
            createReference("name", DataTypes.STRING),
            createReference("age", DataTypes.LONG)
        );
        Function function = createFunction(FormatFunction.NAME, DataTypes.STRING, args);
        Scalar<BytesRef, Object> format = (Scalar<BytesRef, Object>) functions.get(function.info().ident());

        Input<Object> arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return formatString.value();
            }
        };
        Input<Object> arg2 = new Input<Object>() {
            @Override
            public Object value() {
                return "Arthur";
            }
        };
        Input<Object> arg3 = new Input<Object>() {
            @Override
            public Object value() {
                return 38L;
            }
        };

        BytesRef result = format.evaluate(arg1, arg2, arg3);
        assertThat(result.utf8ToString(), is("Arthur bla 38"));

        arg1 = new Input<Object>() {
            @Override
            public Object value() {
                return formatString.value();
            }
        };
        arg2 = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("Arthur");
            }
        };
        arg3 = new Input<Object>() {
            @Override
            public Object value() {
                return 42L;
            }
        };

        result = format.evaluate(arg1, arg2, arg3);
        assertThat(result.utf8ToString(), is("Arthur bla 42"));

    }
}
