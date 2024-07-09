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

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;


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
        assertThatThrownBy(() ->
                assertNormalize("regexp_replace('foobar', 'foo', 'bar', 'n')", isLiteral("")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: n");
    }

    @Test
    public void testNormalizeSymbolWithInvalidNumberOfArguments() throws Exception {
        assertThatThrownBy(() -> assertNormalize("regexp_replace('foobar')", isLiteral("")))
            .isExactlyInstanceOf(UnsupportedFunctionException.class);
    }

    @Test
    public void testNormalizeSymbolWithInvalidArgumentType() {
        assertThatThrownBy(() ->
                assertNormalize("regexp_replace('foobar', '.*', [1,2])", isLiteral("")))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: regexp_replace('foobar', '.*', [1, 2]), " +
                                    "no overload found for matching argument types: (text, text, integer_array).");
    }
}
