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

package io.crate.operation.scalar.regex;

import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ReplaceFunctionTest {

    private Functions functions;

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    @Test
    public void testCompile() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral("(ba)");
        final Literal<BytesRef> replacement = Literal.newLiteral("Crate");
        final BytesRef term = new BytesRef("foobarbequebaz bar");
        final BytesRef expected = new BytesRef("fooCraterbequebaz bar");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                replacement
        );
        Function function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        ReplaceFunction regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexMatcher.class));
        assertEquals(expected, regexpImpl.regexMatcher().replace(term, replacement.value()));

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                replacement,
                Literal.newLiteral("usn")
        );
        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexMatcher.class));
        assertEquals(expected, regexpImpl.regexMatcher().replace(term, replacement.value()));
    }

    @Test
    public void testEvaluate() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral("(ba)");
        final Literal<BytesRef> replacement = Literal.newLiteral("Crate");
        final BytesRef expected = new BytesRef("fooCraterbequebaz bar");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                replacement
        );
        Function function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        ReplaceFunction regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        Input[] args = new Input[3];
        args[0] = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("foobarbequebaz bar");
            }
        };
        args[1] = new Input<Object>() {
            @Override
            public Object value() {
                return pattern.value();
            }
        };
        args[2] = new Input<Object>() {
            @Override
            public Object value() {
                return replacement.value();
            }
        };

        assertEquals(expected, regexpImpl.evaluate(args));
    }

    @Test
    public void testEvaluateWithFlags() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral("(ba)");
        final Literal<BytesRef> replacement = Literal.newLiteral("Crate");
        final Literal<BytesRef> flags = Literal.newLiteral("usn g");
        final BytesRef expected = new BytesRef("fooCraterbequebaz bar");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                replacement,
                flags
        );
        Function function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        ReplaceFunction regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        Input[] args = new Input[4];
        args[0] = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("foobarbequebaz bar");
            }
        };
        args[1] = new Input<Object>() {
            @Override
            public Object value() {
                return pattern.value();
            }
        };
        args[2] = new Input<Object>() {
            @Override
            public Object value() {
                return replacement.value();
            }
        };
        args[3] = new Input<Object>() {
            @Override
            public Object value() {
                return flags.value();
            }
        };

        assertEquals(expected, regexpImpl.evaluate(args));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        final BytesRef expected = new BytesRef("fooCraterbequebaz bar");
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral("foobarbequebaz bar"),
                Literal.newLiteral("(ba)"),
                Literal.newLiteral("Crate")
        );
        Function function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        ReplaceFunction regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        assertLiteralSymbol(result, expected.utf8ToString());

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                Literal.newLiteral("(ba)"),
                Literal.newLiteral("Crate")
        );
        function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        result = regexpImpl.normalizeSymbol(function);
        assertThat(result, instanceOf(Function.class));
        assertThat((Function)result, is(function));
    }

    @Test
    public void testNormalizeSymbolWithFlags() throws Exception {
        final BytesRef expected = new BytesRef("fooCraterbequebaz bar");
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral("foobarbequebaz bar"),
                Literal.newLiteral("(ba)"),
                Literal.newLiteral("Crate"),
                Literal.newLiteral("us n")
        );
        Function function = createFunction(ReplaceFunction.NAME, DataTypes.STRING, arguments);
        ReplaceFunction regexpImpl = (ReplaceFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        assertLiteralSymbol(result, expected.utf8ToString());
    }
}
