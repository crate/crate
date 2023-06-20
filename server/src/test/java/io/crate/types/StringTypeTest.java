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

package io.crate.types;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;

public class StringTypeTest extends ESTestCase {

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_implicit_cast_boolean_to_text() {
        assertThat(DataTypes.STRING.implicitCast(true), is("t"));
        assertThat(DataTypes.STRING.implicitCast(false), is("f"));
    }

    @Test
    public void test_implicit_cast_regproc_to_text() {
        assertThat(
            DataTypes.STRING.implicitCast(Regproc.of("func")),
            is("func"));
    }

    @Test
    public void test_implicit_cast_long_to_text() {
        assertThat(DataTypes.STRING.implicitCast(123L), is("123"));
    }

    @Test
    public void test_create_text_type_with_length_limit() {
        var stringDataType = StringType.of(10);
        assertThat(stringDataType.unbound(), is(false));
        assertThat(stringDataType.lengthLimit(), is(10));
    }

    @Test
    public void test_explicit_cast_text_with_length_truncates_exceeding_length_chars() {
        assertThat(StringType.of(1).explicitCast("abcde", SESSION_SETTINGS), is("a"));
        assertThat(StringType.of(2).explicitCast("a    ", SESSION_SETTINGS), is("a "));
    }

    @Test
    public void test_explicit_cast_text_with_length_on_literals_of_length_lte_length() {
        assertThat(StringType.of(5).explicitCast("abc", SESSION_SETTINGS), is("abc"));
        assertThat(StringType.of(1).explicitCast("a", SESSION_SETTINGS), is("a"));
    }

    @Test
    public void test_explicit_cast_text_without_length() {
        assertThat(StringType.INSTANCE.explicitCast("abc", SESSION_SETTINGS), is("abc"));
    }

    @Test
    public void test_explicit_cast_text_without_length_on_literals_of_length_lte_length() {
        assertThat(StringType.INSTANCE.explicitCast("abc", SESSION_SETTINGS), is("abc"));
        assertThat(StringType.INSTANCE.explicitCast("a", SESSION_SETTINGS), is("a"));
    }

    @Test
    public void test_implicit_cast_text_without_length() {
        assertThat(StringType.INSTANCE.implicitCast("abc"), is("abc"));
    }

    @Test
    public void test_implicit_cast_text_with_length_ignores_length_limit() {
        assertThat(StringType.of(1).implicitCast("abcde"), is("abcde"));
        assertThat(StringType.of(2).implicitCast("a    "), is("a    "));
    }

    @Test
    public void test_text_type_with_length_serialization_roundtrip_before_4_2_0() throws IOException {
        var actualType = StringType.of(1);

        var out = new BytesStreamOutput();
        out.setVersion(Version.V_4_1_0);
        DataTypes.toStream(actualType, out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_4_1_0);
        assertThat(DataTypes.fromStream(in), is(StringType.INSTANCE));
    }

    @Test
    public void test_text_type_with_length_serialization_roundtrip() throws IOException {
        var actualType = StringType.of(1);

        var out = new BytesStreamOutput();
        DataTypes.toStream(actualType, out);

        var in = out.bytes().streamInput();
        assertThat(DataTypes.fromStream(in), is(actualType));
    }

    @Test
    public void test_value_for_insert_text_with_length_on_literals_of_length_lte_length() {
        assertThat(StringType.of(4).sanitizeValue("abcd"), is("abcd"));
        assertThat(StringType.of(3).sanitizeValue("a"), is("a"));
    }

    @Test
    public void test_value_for_insert_text_without_length_does_not_change_input_value() {
        assertThat(StringType.INSTANCE.sanitizeValue("abcd"), is("abcd"));
    }

    @Test
    public void test_value_for_insert_text_with_length_trims_exceeding_chars_if_they_are_whitespaces() {
        assertThat(StringType.of(2).sanitizeValue("a    "), is("a "));
        assertThat(StringType.of(2).sanitizeValue("ab  "), is("ab"));
    }

    @Test
    public void test_value_for_insert_text_with_length_for_literal_exceeding_length_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'abcd' is too long for the text type of length: 3");
        StringType.of(3).sanitizeValue("abcd");
    }

    @Test
    public void test_create_text_type_with_negative_length_limit_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The text type length must be at least 1, received: -1");
        StringType.of(-1);
    }

    @Test
    public void test_create_text_type_with_zero_length_limit_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The text type length must be at least 1, received: 0");
        StringType.of(0);
    }

    @Test
    public void test_create_text_type_with_wrong_number_of_parameters_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The text type can only have a single parameter value, received: 2");
        StringType.of(List.of(1, 2));
    }
}
