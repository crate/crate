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

package io.crate.operation.scalar.string;

import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.*;

public class UpperFunctionTest extends AbstractScalarFunctionsTest {

    public Symbol normalizeForArgs(List<Symbol> args) {
        Function function = createFunction(UpperFunction.NAME, DataTypes.STRING, args);
        FunctionImplementation impl = functions.get(function.info().ident());
        impl = ((Scalar) impl).compile(function.arguments());

        return impl.normalizeSymbol(function);
    }

    public Object evaluateForArgs(List<Symbol> args) {
        Function function = createFunction(UpperFunction.NAME, DataTypes.STRING, args);
        Scalar impl = (Scalar) functions.get(function.info().ident());
        impl = impl.compile(function.arguments());

        Input[] inputs = new Input[args.size()];
        for (int i = 0; i < args.size(); i++) {
            inputs[i] = (Input) args.get(i);
        }

        return impl.evaluate(inputs);
    }

    @Test
    public void testNormalizeDefault() throws Exception {
        List<Symbol> args = Lists.<Symbol>newArrayList(
                Literal.newLiteral("abcdefghijklmnopqrstuvwxyzäöüαβγ")
        );
        assertThat(
                normalizeForArgs(args),
                isLiteral("ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜΑΒΓ"));
    }

    @Test
    public void testEvaluateNull() throws Exception {
        Literal stringNull = Literal.newLiteral(DataTypes.STRING, null);
        List<List<Symbol>> argLists = Arrays.asList(
                Arrays.<Symbol>asList(stringNull)
        );

        for (List<Symbol> argList : argLists) {
            Object value = evaluateForArgs(argList);
            assertThat(value, is(nullValue()));
        }
    }

    public void testNormalizeSymbol() throws Exception {
        Function function = (Function) sqlExpressions.asSymbol("upper('SomeString')");
        UpperFunction upperFunction = (UpperFunction) functions.get(function.info().ident());
        Symbol result = upperFunction.normalizeSymbol(function);
        assertThat(result, isLiteral(new BytesRef("SOMESTRING")));

        function = (Function) sqlExpressions.asSymbol("upper(name)");
        upperFunction = (UpperFunction) functions.get(function.info().ident());
        result = upperFunction.normalizeSymbol(function);
        assertThat(result, instanceOf(Function.class));
        assertThat((Function)result, is(function));
    }
}
