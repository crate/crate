/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.testing.Asserts.assertThat;

import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;

public class CharacterTypeTest extends DataTypeTestCase<String> {

    @Override
    public DataType<String> getType() {
        return CharacterType.INSTANCE;
    }

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_value_for_insert_adds_blank_padding() {
        assertThat(CharacterType.of(5).valueForInsert("a")).isEqualTo("a    ");
    }

    @Test
    public void test_implicit_cast_adds_blank_padding() {
        assertThat(CharacterType.of(5).implicitCast("a")).isEqualTo("a    ");
    }

    @Test
    public void test_default_length() {
        assertThat(CharacterType.INSTANCE.lengthLimit()).isEqualTo(1);
    }

    @Test
    public void test_explicit_cast_truncates_overflow_chars() {
        assertThat(CharacterType.of(1).explicitCast("foo", SESSION_SETTINGS)).isEqualTo("f");
        assertThat(CharacterType.of(1).explicitCast(true, SESSION_SETTINGS)).isEqualTo("t");
        assertThat(CharacterType.of(1).explicitCast(12, SESSION_SETTINGS)).isEqualTo("1");
        assertThat(CharacterType.of(1).explicitCast(-12, SESSION_SETTINGS)).isEqualTo("-");
    }
}
