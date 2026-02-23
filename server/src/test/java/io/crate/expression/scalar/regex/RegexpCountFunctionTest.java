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

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class RegexpCountFunctionTest extends ScalarTestCase {

    @Test
    public void test_count_no_match() {
        assertEvaluate("regexp_count(name, 'crate')", 0, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_count_basic() {
        assertEvaluate("regexp_count(name, 'ba(?:r|z)')", 2, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_count_non_overlapping() {
        assertEvaluate("regexp_count('aaaa', 'a{2}')", 2);
    }

    @Test
    public void test_count_with_start() {
        assertEvaluate("regexp_count('abcabc', 'abc', 2)", 1);
        assertEvaluate("regexp_count('abcabc', 'abc', 1)", 2);
        assertEvaluate("regexp_count('aaaa', 'aa', 2)", 1);
    }

    @Test
    public void test_count_start_beyond_length() {
        assertEvaluate("regexp_count('abc', 'a', 5)", 0);
    }

    @Test
    public void test_count_start_zero_or_negative_throws_error() {
        assertThatThrownBy(() -> assertEvaluate("regexp_count('abcabc', 'abc', 0)", 2))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`start` must be greater than or equal to 1");
        assertThatThrownBy(() -> assertEvaluate("regexp_count('abcabc', 'abc', -2)", 2))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`start` must be greater than or equal to 1");
        assertThatThrownBy(() -> assertEvaluate("regexp_count('abc', 'a', -2147483648)", 1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`start` must be greater than or equal to 1");
    }

    @Test
    public void test_count_with_flags() {
        assertEvaluate("regexp_count('AaA', 'a', 1, 'i')", 3);
    }

    @Test
    public void test_g_flag_is_rejected() {
        assertThatThrownBy(() -> assertNormalize("regexp_count('aba', 'a', 1, 'g')", isLiteral(0)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: g");
    }

    @Test
    public void test_g_flag_is_rejected_during_compile() {
        assertThatThrownBy(() -> assertCompile("regexp_count(name, 'a', 1, 'g')", ignored -> ignored2 -> {
        }))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: g");
    }

    @Test
    public void test_nulls() {
        assertEvaluateNull("regexp_count(null, 'a')");
        assertEvaluateNull("regexp_count('abc', null)");
        assertEvaluateNull("regexp_count('abc', 'a', null)");
        assertEvaluateNull("regexp_count('aaa', 'a', 1, null)");
    }

    @Test
    public void test_normalize_symbol() {
        assertNormalize("regexp_count('AbA', 'a', 1, 'i')", isLiteral(2));
    }

    @Test
    public void test_invalid_flags() {
        assertThatThrownBy(() -> assertNormalize("regexp_count('foobar', 'foo', 1, 'n')", isLiteral(0)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: n");
    }

    @Test
    public void test_invalid_number_of_arguments() {
        assertThatThrownBy(() -> assertEvaluateNull("regexp_count('foobar')"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class);
        assertThatThrownBy(() -> assertCompile("regexp_count('foobar')", ignored -> ignored2 -> {
        }))
            .isExactlyInstanceOf(UnsupportedFunctionException.class);
    }
}
