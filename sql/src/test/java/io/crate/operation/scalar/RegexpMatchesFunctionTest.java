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

import io.crate.metadata.Functions;
import io.crate.operation.Input;
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
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RegexpMatchesFunctionTest {

    private Functions functions;

    @Before
    public void setUp() {
        functions = new ModulesBuilder()
                .add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    @Test
    public void testRegexMatcher() throws Exception {
        String pattern = "ba";
        int flags = 0;
        String text = "foobarbequebaz";
        RegexpMatchesFunction.RegexMatcher regexMatcher = new RegexpMatchesFunction.RegexMatcher(pattern, flags);
        assertEquals(false, regexMatcher.match(new BytesRef(text)));

        pattern = ".*ba.*";
        regexMatcher = new RegexpMatchesFunction.RegexMatcher(pattern, flags);
        assertEquals(true, regexMatcher.match(new BytesRef(text)));
        assertThat(regexMatcher.groups(), arrayContaining("foobarbequebaz"));

        pattern = ".*(ba).*";
        regexMatcher = new RegexpMatchesFunction.RegexMatcher(pattern, flags);
        assertEquals(true, regexMatcher.match(new BytesRef(text)));
        assertThat(regexMatcher.groups(), arrayContaining("foobarbequebaz", "ba"));

        pattern = ".*?(\\w+?)(ba).*";
        regexMatcher = new RegexpMatchesFunction.RegexMatcher(pattern, flags);
        assertEquals(true, regexMatcher.match(new BytesRef(text)));
        assertThat(regexMatcher.groups(), arrayContaining("foobarbequebaz", "foo", "ba"));
    }

    @Test
    public void testCompile() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral(".*(ba).*");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern
        );
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexpMatchesFunction.RegexMatcher.class));
        assertEquals(true, regexpImpl.regexMatcher().match(new BytesRef("foobarbequebaz bar")));
        assertThat(regexpImpl.regexMatcher().groups(), arrayContaining("foobarbequebaz bar", "ba"));

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern,
                Literal.newLiteral("usn")
        );
        regexpImpl.compile(arguments);

        assertThat(regexpImpl.regexMatcher(), instanceOf(RegexpMatchesFunction.RegexMatcher.class));
        assertEquals(true, regexpImpl.regexMatcher().match(new BytesRef("foobarbequebaz bar")));
        assertThat(regexpImpl.regexMatcher().groups(), arrayContaining("foobarbequebaz bar", "ba"));
    }

    @Test
    public void testEvaluateWithCompile() throws Exception {
        final Literal<BytesRef> pattern = Literal.newLiteral(".*(ba).*");

        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                pattern
        );
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

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

        String[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining("foobarbequebaz bar", "ba"));
    }

    @Test
    public void testEvaluate() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                createReference("pattern", DataTypes.STRING)
        );
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

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

        String[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining("foobarbequebaz bar", "ba"));
    }

    @Test
    public void testEvaluateWithFlags() throws Exception {
        final Literal<BytesRef> flags = Literal.newLiteral("usn");
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                createReference("pattern", DataTypes.STRING)
        );
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

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

        String[] result = regexpImpl.evaluate(args);

        assertThat(result, arrayContaining("foobarbequebaz bar", "ba"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                Literal.newLiteral("foobarbequebaz bar"),
                Literal.newLiteral(".*(ba).*")
        );
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        String[] expected = new String[2];
        expected[0] = "foobarbequebaz bar";
        expected[1] = "ba";
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));

        arguments = Arrays.<Symbol>asList(
                createReference("text", DataTypes.STRING),
                Literal.newLiteral(".*(ba).*")
        );
        function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

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
        Function function = createFunction(RegexpMatchesFunction.NAME, DataTypes.STRING, arguments);
        RegexpMatchesFunction regexpImpl = (RegexpMatchesFunction) functions.get(function.info().ident());

        Symbol result = regexpImpl.normalizeSymbol(function);
        String[] expected = new String[2];
        expected[0] = "foobarbequebaz bar";
        expected[1] = "ba";
        assertLiteralSymbol(result, expected, new ArrayType(DataTypes.STRING));
    }
}
