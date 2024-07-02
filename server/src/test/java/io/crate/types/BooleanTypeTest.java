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

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.Test;


public class BooleanTypeTest extends DataTypeTestCase<Boolean> {

    @Override
    public DataType<Boolean> getType() {
        return BooleanType.INSTANCE;
    }

    @Test
    public void test_cast_text_to_boolean() {
        assertThat(BooleanType.INSTANCE.implicitCast("t")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("T")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("false")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("fAlSe")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("FALSE")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("f")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("F")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("no")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("nO")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("NO")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("n")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("N")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("off")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("Off")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("OFF")).isFalse();
        assertThat(BooleanType.INSTANCE.implicitCast("0")).isFalse();

        assertThat(BooleanType.INSTANCE.implicitCast("true")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("trUe")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("TRUE")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("t")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("T")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("yes")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("yEs")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("YES")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("y")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("Y")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("on")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("On")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("ON")).isTrue();
        assertThat(BooleanType.INSTANCE.implicitCast("1")).isTrue();
    }

    @Test
    public void test_sanitize_boolean_value() {
        assertThat(BooleanType.INSTANCE.sanitizeValue(Boolean.FALSE)).isFalse();
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(BooleanType.INSTANCE.sanitizeValue(1)).isTrue();
    }

    @Test
    public void test_cast_unsupported_text_to_boolean_throws_exception() {
        assertThatThrownBy(() -> BooleanType.INSTANCE.implicitCast("hello"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Can't convert \"hello\" to boolean");
    }

    @Test
    public void test_cast_map_to_boolean_throws_exception() {
        assertThatThrownBy(() -> BooleanType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to boolean");
    }

    @Override
    public void test_reference_resolver_docvalues_off() throws Exception {
        assumeFalse("BooleanType cannot disable column store", true);
    }

    @Override
    public void test_reference_resolver_index_and_docvalues_off() throws Exception {
        assumeFalse("BooleanType cannot disable column store", true);
    }
}
