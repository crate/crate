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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class RegexpMatchFunctionTest extends ScalarTestCase {

    @Test
    public void test_no_match() {
        assertEvaluateNull("regexp_match(name, 'crate')", Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_match_without_groups() {
        assertEvaluate("regexp_match(name, 'bar')", List.of("bar"), Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_match_with_groups() {
        assertEvaluate("regexp_match(name, '(bar)(beque)')", List.of("bar", "beque"), Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_match_with_unmatched_group() {
        assertEvaluate("regexp_match('b', '(a)?b')", Arrays.asList(new Object[] { null }), Literal.of("b"));
    }

    @Test
    public void test_flags() {
        assertEvaluate("regexp_match(name, '(BAR)', 'i')", List.of("bar"), Literal.of("foobarbequebaz"));
        assertEvaluateNull("regexp_match(name, '(BAR)', '')", Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_global_flag_is_rejected() {
        assertThatThrownBy(() -> assertEvaluate("regexp_match('aba', 'a', 'g')", new String[]{"a"}))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: g");
    }

    @Test
    public void test_global_flag_is_rejected_during_compile() {
        assertThatThrownBy(() -> assertCompile("regexp_match(name, 'a', 'g')", ignored -> ignored2 -> {
        }))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The regular expression flag is unknown: g");
    }

    @Test
    public void test_nulls() {
        assertEvaluateNull("regexp_match(null, 'pattern')");
        assertEvaluateNull("regexp_match('abc', null)");
        assertEvaluateNull("regexp_match('abc', 'pattern', null)");
    }

    @Test
    public void test_compile() {
        assertCompile("regexp_match(name, '(bar)(beque)')", scalar -> compiledScalar -> {
            assertThat(scalar).isInstanceOf(RegexpMatchFunction.class);
            assertThat(compiledScalar).isInstanceOf(RegexpMatchFunction.CompiledRegexpMatch.class);
        });
    }
}
