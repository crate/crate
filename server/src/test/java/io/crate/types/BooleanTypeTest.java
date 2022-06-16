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

import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;


public class BooleanTypeTest extends ESTestCase {

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_cast_text_to_boolean() {
        assertThat(BooleanType.INSTANCE.implicitCast("t", SESSION_SETTINGS), is(true));
        assertThat(BooleanType.INSTANCE.implicitCast("false", SESSION_SETTINGS), is(false));
        assertThat(BooleanType.INSTANCE.implicitCast("FALSE", SESSION_SETTINGS), is(false));
        assertThat(BooleanType.INSTANCE.implicitCast("f", SESSION_SETTINGS), is(false));
        assertThat(BooleanType.INSTANCE.implicitCast("F", SESSION_SETTINGS), is(false));
        assertThat(BooleanType.INSTANCE.implicitCast("true", SESSION_SETTINGS), is(true));
        assertThat(BooleanType.INSTANCE.implicitCast("TRUE", SESSION_SETTINGS), is(true));
        assertThat(BooleanType.INSTANCE.implicitCast("t", SESSION_SETTINGS), is(true));
        assertThat(BooleanType.INSTANCE.implicitCast("T", SESSION_SETTINGS), is(true));
    }

    @Test
    public void test_sanitize_boolean_value() {
        assertThat(BooleanType.INSTANCE.sanitizeValue(Boolean.FALSE), is(false));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(BooleanType.INSTANCE.sanitizeValue(1), is(true));
    }

    @Test
    public void test_cast_unsupported_text_to_boolean_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't convert \"hello\" to boolean");
        BooleanType.INSTANCE.implicitCast("hello", SESSION_SETTINGS);
    }

    @Test
    public void test_cast_map_to_boolean_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to boolean");
        BooleanType.INSTANCE.implicitCast(Map.of(), SESSION_SETTINGS);
    }
}
