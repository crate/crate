/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar.regex;

import io.crate.expression.symbol.Literal;
import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class RegexpReplaceFunctionTest extends ScalarTestCase {

    @Test
    public void test_replace_no_match() throws Exception {
        assertEvaluate("regexp_replace(name, 'crate', 'crate')", "foobarbequebaz", Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_replace() throws Exception {
        assertEvaluate("regexp_replace(name, 'ba', 'Crate')", "fooCraterbequebaz", Literal.of("foobarbequebaz"));
        assertEvaluate("regexp_replace(name, '(ba).*(ba)', 'First$1Second$2')", "fooFirstbaSecondbaz", Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_replace_global() throws Exception {
        assertEvaluate("regexp_replace(name, 'ba', 'Crate', 'g')", "fooCraterbequeCratez", Literal.of("foobarbequebaz"));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate(
            "regexp_replace(name, '(ba)', 'Crate')",
            "fooCraterbequebaz bar",
            Literal.of("foobarbequebaz bar"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("regexp_replace('foobarbequebaz bar', '(ba)', 'Crate')", isLiteral("fooCraterbequebaz bar"));
        assertNormalize("regexp_replace(name, '(ba)', 'Crate')", isFunction(RegexpReplaceFunction.NAME));
    }

    @Test
    public void testEvalWithFlags() throws Exception {
        assertEvaluate("regexp_replace('st. cloud', '[^a-z]', '', 'g')", "stcloud");
        assertEvaluate("regexp_replace(name, '[^a-z]', '', 'g')", "stcloud", Literal.of("st. cloud"));
    }

    @Test
    public void testNormalizeSymbolWithFlags() throws Exception {
        assertNormalize("regexp_replace('foobarbequebaz bar', '(ba)', 'Crate', 'us')", isLiteral("fooCraterbequebaz bar"));
    }

    @Test
    public void testNormalizeSymbolWithInvalidFlags() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The regular expression flag is unknown: n");
        assertNormalize("regexp_replace('foobar', 'foo', 'bar', 'n')", isLiteral(""));
    }

    @Test
    public void testNormalizeSymbolWithInvalidNumberOfArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        assertNormalize("regexp_replace('foobar')", isLiteral(""));
    }

    @Test
    public void testNormalizeSymbolWithInvalidArgumentType() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: regexp_replace('foobar', '.*', _array(1, 2))," +
                                        " no overload found for matching argument types: (text, text, integer_array).");

        assertNormalize("regexp_replace('foobar', '.*', [1,2])", isLiteral(""));
    }
}
