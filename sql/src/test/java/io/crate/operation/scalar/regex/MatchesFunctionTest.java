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
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MatchesFunctionTest {

    private Functions functions;

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    @Test
    public void testCompile() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral(".*(ba).*");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexMatcher.class));
        assertEquals(true, regexpImpl.regexMatcher().match(new BytesRef("foobarbequebaz bar")));
        assertThat(regexpImpl.regexMatcher().groups(), arrayContaining(new BytesRef("foobarbequebaz bar"), new BytesRef("ba")));

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                Literal.newLiteral("usn")
        );
        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexMatcher.class));
        assertEquals(true, regexpImpl.regexMatcher().match(new BytesRef("foobarbequebaz bar")));
        assertThat(regexpImpl.regexMatcher().groups(),
                arrayContaining(new BytesRef("foobarbequebaz bar"), new BytesRef("ba")));
    }

    @Test
    public void testEvaluateWithCompile() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral(".*(ba).*");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        Input[] args = new Input[2];
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

        BytesRef[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining(new BytesRef("foobarbequebaz bar"),new BytesRef( "ba")));
    }

    @Test
    public void testEvaluate() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                createReference("pattern", DataTypes.STRING)
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        Input[] args = new Input[2];
        args[0] = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef("foobarbequebaz bar");
            }
        };
        args[1] = new Input<Object>() {
            @Override
            public Object value() {
                return new BytesRef(".*(ba).*");
            }
        };

        BytesRef[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining(new BytesRef("foobarbequebaz bar"), new BytesRef("ba")));
    }

    @Test
    public void testEvaluateWithFlags() throws Exception {
        final Literal<BytesRef> flags = Literal.newLiteral("usn");
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                createReference("pattern", DataTypes.STRING)
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

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
                return new BytesRef(".*(ba).*");
            }
        };
        args[2] = new Input<Object>() {
            @Override
            public Object value() {
                return flags.value();
            }
        };

        BytesRef[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining(new BytesRef("foobarbequebaz bar"), new BytesRef("ba")));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral("foobarbequebaz bar"),
                Literal.newLiteral(".*(ba).*")
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        BytesRef[] expected = new BytesRef[]{
                new BytesRef("foobarbequebaz bar"),
                new BytesRef("ba")};
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                Literal.newLiteral(".*(ba).*")
        );
        function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        result = regexpImpl.normalizeSymbol(function);
        assertThat(result, instanceOf(Function.class));
        assertThat((Function)result, is(function));
    }

    @Test
    public void testNormalizeSymbolWithFlags() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral("foobarbequebaz bar"),
                Literal.newLiteral(".*(ba).*"),
                Literal.newLiteral("us n")
        );
        Function function = createFunction(MatchesFunction.NAME, DataTypes.STRING, arguments);
        MatchesFunction regexpImpl = (MatchesFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        BytesRef[] expected = new BytesRef[]{
                new BytesRef("foobarbequebaz bar"),
                new BytesRef("ba")};
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));
    }
}
