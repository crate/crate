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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.testing.Asserts;
import io.crate.types.DataTypes;

public class SubstrFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("substr('cratedata', 0)", isLiteral("cratedata"));
        assertNormalize("substr('cratedata', 6)", isLiteral("data"));
        assertNormalize("substr('cratedata', 10)", isLiteral(""));
        assertNormalize("substr('cratedata', 1, 1)", isLiteral("c"));
        assertNormalize("substr('cratedata', 3, 2)", isLiteral("at"));
        assertNormalize("substr('cratedata', 6, 10)", isLiteral("data"));
        assertNormalize("substr('cratedata', 6, 0)", isLiteral(""));
        assertNormalize("substr('cratedata', 10, -1)", isLiteral(""));
    }

    @Test
    public void testSubstring() throws Exception {
        assertThat(SubstrFunction.substring("cratedata", 2, 5), is("ate"));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("substr('cratedata', 6, 2)", "da");
    }

    @Test
    public void testEvaluateWithArgsAsNonLiterals() throws Exception {
        assertEvaluate("substr('cratedata', id, id)", "crate", Literal.of(1L), Literal.of(5L));
    }

    @Test
    public void testEvaluateWithArgsAsNonLiteralsIntShort() throws Exception {
        assertEvaluate("substr(name, id, id)", "crate",
            Literal.of("cratedata"),
            Literal.of(DataTypes.SHORT, (short) 1),
            Literal.of(DataTypes.SHORT, (short) 5));
    }

    @Test
    public void testNullInputs() throws Exception {
        assertEvaluateNull("substr(name, id, id)",
            Literal.of(DataTypes.STRING, null),
            Literal.of(1),
            Literal.of(1));
        assertEvaluateNull("substr(name, id, id)",
            Literal.of("crate"),
            Literal.of(DataTypes.INTEGER, null),
            Literal.of(1));
        assertEvaluateNull("substr(name, id, id)",
            Literal.of("crate"),
            Literal.of(1),
            Literal.of(DataTypes.SHORT, null));
    }

    @Test
    public void testInvalidArgs() throws Exception {
        assertThatThrownBy(() -> assertNormalize("substr('foo', [1, 2])", s -> Asserts.assertThat(s).isNull()))
            .isExactlyInstanceOf(UnsupportedFunctionException.class);
    }

    @Test
    public void test_substring_is_alias_for_substr() {
        assertNormalize("substring('crate', 1, 3)", isLiteral("cra"));
    }

    @Test
    public void test_non_generic_substring_syntax_is_alias_for_substr() {
        assertNormalize("substring('crate' FROM 3)", isLiteral("ate"));
    }

    @Test
    public void test_substring_from_no_parenthesis() throws Exception {
        assertEvaluate("substring('foobar' FROM 'o.b')", "oob");
    }

    @Test
    public void test_substring_from_parenthesis() throws Exception {
        assertEvaluate("substring('foobar' FROM 'o(.)b')", "o");
    }

    @Test
    public void test_substring_from_uses_first_matching_group() throws Exception {
        assertEvaluate("substring('foobar' FROM '(.)oo([a-z])')", "f");
    }

    @Test
    public void test_substring_from_capture_full_pattern() throws Exception {
        assertEvaluate("substring(name FROM '((.)oo([a-z]))')", "foob", Literal.of("foobar"));
    }

    @Test
    public void test_substring_from_no_pattern_match() throws Exception {
        assertEvaluate("substring('foobar' FROM 'nomatch')", x -> assertThat(x).isNull());
    }

    @Test
    public void test_substring_from_null_values() throws Exception {
        assertNormalize("substring(null::string FROM null::string)", isLiteral(null));
        assertNormalize("substring('foobar' FROM null::string)", isLiteral(null));
        assertNormalize("substring(null::string FROM 'pattern')", isLiteral(null));

        Literal<String> nullString = Literal.of((String) null);
        assertEvaluate("substring(name FROM null::string)", x -> assertThat(x).isNull(), nullString);
        assertEvaluate("substring(name FROM null::string)", x -> assertThat(x).isNull(), nullString);
        assertEvaluate("substring(name FROM 'pattern')", x -> assertThat(x).isNull(), nullString);
    }
}

