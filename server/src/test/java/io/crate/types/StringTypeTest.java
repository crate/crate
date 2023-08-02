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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;

public class StringTypeTest extends DataTypeTestCase<String> {

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Override
    public DataType<String> getType() {
        return StringType.of(randomIntBetween(1, 40));
    }

    @Test
    public void test_implicit_cast_boolean_to_text() {
        assertThat(DataTypes.STRING.implicitCast(true)).isEqualTo("t");
        assertThat(DataTypes.STRING.implicitCast(false)).isEqualTo("f");
    }

    @Test
    public void test_implicit_cast_regproc_to_text() {
        assertThat(DataTypes.STRING.implicitCast(Regproc.of("func"))).isEqualTo("func");
    }

    @Test
    public void test_implicit_cast_long_to_text() {
        assertThat(DataTypes.STRING.implicitCast(123L)).isEqualTo("123");
    }

    @Test
    public void test_create_text_type_with_length_limit() {
        var stringDataType = StringType.of(10);
        assertThat(stringDataType.unbound()).isEqualTo(false);
        assertThat(stringDataType.lengthLimit()).isEqualTo(10);
    }

    @Test
    public void test_explicit_cast_text_with_length_truncates_exceeding_length_chars() {
        assertThat(StringType.of(1).explicitCast("abcde", SESSION_SETTINGS)).isEqualTo("a");
        assertThat(StringType.of(2).explicitCast("a    ", SESSION_SETTINGS)).isEqualTo("a ");
    }

    @Test
    public void test_explicit_cast_text_with_length_on_literals_of_length_lte_length() {
        assertThat(StringType.of(5).explicitCast("abc", SESSION_SETTINGS)).isEqualTo("abc");
        assertThat(StringType.of(1).explicitCast("a", SESSION_SETTINGS)).isEqualTo("a");
    }

    @Test
    public void test_explicit_cast_text_without_length() {
        assertThat(StringType.INSTANCE.explicitCast("abc", SESSION_SETTINGS)).isEqualTo("abc");
    }

    @Test
    public void test_explicit_cast_text_without_length_on_literals_of_length_lte_length() {
        assertThat(StringType.INSTANCE.explicitCast("abc", SESSION_SETTINGS)).isEqualTo("abc");
        assertThat(StringType.INSTANCE.explicitCast("a", SESSION_SETTINGS)).isEqualTo("a");
    }

    @Test
    public void test_implicit_cast_text_without_length() {
        assertThat(StringType.INSTANCE.implicitCast("abc")).isEqualTo("abc");
    }

    @Test
    public void test_implicit_cast_text_with_length_ignores_length_limit() {
        assertThat(StringType.of(1).implicitCast("abcde")).isEqualTo("abcde");
        assertThat(StringType.of(2).implicitCast("a    ")).isEqualTo("a    ");
    }

    @Test
    public void test_text_type_with_length_serialization_roundtrip_before_4_2_0() throws IOException {
        var actualType = StringType.of(1);

        var out = new BytesStreamOutput();
        out.setVersion(Version.V_4_1_0);
        DataTypes.toStream(actualType, out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_4_1_0);
        assertThat(DataTypes.fromStream(in)).isEqualTo(StringType.INSTANCE);
    }

    @Test
    public void test_value_for_insert_text_with_length_on_literals_of_length_lte_length() {
        assertThat(StringType.of(4).valueForInsert("abcd")).isEqualTo("abcd");
        assertThat(StringType.of(3).valueForInsert("a")).isEqualTo("a");
    }

    @Test
    public void test_value_for_insert_text_without_length_does_not_change_input_value() {
        assertThat(StringType.INSTANCE.valueForInsert("abcd")).isEqualTo("abcd");
    }

    @Test
    public void test_value_for_insert_text_with_length_trims_exceeding_chars_if_they_are_whitespaces() {
        assertThat(StringType.of(2).valueForInsert("a    ")).isEqualTo("a ");
        assertThat(StringType.of(2).valueForInsert("ab  ")).isEqualTo("ab");
    }

    @Test
    public void test_value_for_insert_text_with_length_for_literal_exceeding_length_throws_exception() {
        assertThatThrownBy(() -> StringType.of(3).valueForInsert("abcd"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("'abcd' is too long for the text type of length: 3");
    }

    @Test
    public void test_create_text_type_with_negative_length_limit_throws_exception() {
        assertThatThrownBy(() -> StringType.of(-1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The text type length must be at least 1, received: -1");
    }

    @Test
    public void test_create_text_type_with_zero_length_limit_throws_exception() {
        assertThatThrownBy(() -> StringType.of(0))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The text type length must be at least 1, received: 0");
    }

    @Test
    public void test_create_text_type_with_wrong_number_of_parameters_throws_exception() {
        assertThatThrownBy(() -> StringType.of(List.of(1, 2)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The text type can only have a single parameter value, received: 2");
    }
}
