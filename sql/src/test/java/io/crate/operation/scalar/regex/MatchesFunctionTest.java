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

import io.crate.analyze.symbol.Literal;
import io.crate.metadata.Scalar;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class MatchesFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testCompile() throws Exception {
        Matcher<Scalar> matcher = new BaseMatcher<Scalar>() {
            @Override
            public boolean matches(Object item) {
                MatchesFunction regexpImpl = (MatchesFunction) item;
                // ensure that the RegexMatcher was created due to compilation
                return regexpImpl.regexMatcher() != null;
            }

            @Override
            public void describeTo(Description description) {
            }
        };
        assertCompile("regexp_matches(name, '.*(ba).*')", (s) -> matcher);
    }

    @Test
    public void testEvaluateWithCompile() throws Exception {
        BytesRef[] expected = new BytesRef[]{new BytesRef("ba")};
        assertEvaluate("regexp_matches(name, '.*(ba).*')", expected, Literal.of("foobarbequebaz bar"));
    }

    @Test
    public void testEvaluate() throws Exception {
        BytesRef[] expected = new BytesRef[]{new BytesRef("ba")};
        assertEvaluate("regexp_matches(name, regex_pattern)", expected,
            Literal.of("foobarbequebaz bar"),
            Literal.of(".*(ba).*"));
    }

    @Test
    public void testEvaluateWithFlags() throws Exception {
        BytesRef[] expected = new BytesRef[]{new BytesRef("ba")};
        assertEvaluate("regexp_matches(name, regex_pattern, 'us')", expected,
            Literal.of("foobarbequebaz bar"),
            Literal.of(".*(ba).*"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("regexp_matches('foobarbequebaz bar', '.*(ba).*')",
            isLiteral(new BytesRef[]{new BytesRef("ba")}, new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testNormalizeSymbolWithFlags() throws Exception {
        assertNormalize("regexp_matches('foobarbequebaz bar', '.*(ba).*', 'us')",
            isLiteral(new BytesRef[]{new BytesRef("ba")}, new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testNormalizeSymbolWithInvalidFlags() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The regular expression flag is unknown: n");
        assertNormalize("regexp_matches('foobar', 'foo', 'n')", null);
    }

    @Test
    public void testNormalizeSymbolWithInvalidNumberOfArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: regexp_matches(string)");
        assertNormalize("regexp_matches('foobar')", null);
    }
}
