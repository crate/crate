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

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class StringTypeTest extends CrateUnitTest {

    @Test
    public void test_convert_boolean_to_text() {
        assertThat(DataTypes.STRING.value(true), is("t"));
        assertThat(DataTypes.STRING.value(false), is("f"));
    }

    @Test
    public void test_convert_long_to_text() {
        assertThat(DataTypes.STRING.value(123L), is("123"));
    }

    @Test
    public void test_create_text_type_with_length_limit() {
        var stringDataType = StringType.of(10);
        assertThat(stringDataType.unbound(), is(false));
        assertThat(stringDataType.lengthLimit(), is(10));
    }

    @Test
    public void test_text_type_without_length_limit_on_string_literal() {
        assertThat(StringType.INSTANCE.value("abc"), is("abc"));
    }

    @Test
    public void test_text_type_with_length_on_string_literal_of_length_gt_length_limit_truncates_chars() {
        assertThat(StringType.of(1).value("abcde"), is("a"));
        assertThat(StringType.of(2).value("a    "), is("a "));
    }

    @Test
    public void test_text_type_with_length_on_string_literal_of_length_lte_length() {
        assertThat(StringType.of(5).value("abc"), is("abc"));
        assertThat(StringType.of(1).value("a"), is("a"));
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
