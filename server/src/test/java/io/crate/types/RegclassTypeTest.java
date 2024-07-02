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

import static io.crate.testing.Asserts.assertThat;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;

public class RegclassTypeTest extends DataTypeTestCase<Regclass> {

    @Override
    public DataType<Regclass> getType() {
        return RegclassType.INSTANCE;
    }

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    private Regclass explicitCast(Object value) {
        return RegclassType.INSTANCE.explicitCast(value, SESSION_SETTINGS);
    }

    @Test
    public void test_cannot_cast_long_outside_int_range_to_regclass() {
        Assertions.assertThatThrownBy(() -> RegclassType.INSTANCE.implicitCast(Integer.MAX_VALUE + 42L))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("2147483689 is outside of `int` range and cannot be cast to the regclass type");
    }

    @Test
    public void test_cast_from_string_uses_current_schema() {
        var sessionSettings = new SessionSettings("crate", SearchPath.createSearchPathFrom("my_schema"));
        var regclass = RegclassType.INSTANCE.explicitCast("my_table", sessionSettings);

        assertThat(regclass.toString()).isEqualTo("2034491507");
    }

    @Test
    public void test_cast_from_quoted_string_identifier() {
        var regclassQuotedIdentifier = explicitCast("\"my_table\"");
        var regclass = explicitCast("my_table");

        assertThat(regclassQuotedIdentifier).isEqualTo(regclass);
    }

    @Test
    public void test_cast_from_string_unquoted_ignores_capital_case() {
        var regclassQuotedIdentifier = explicitCast("\"mytable\"");
        var regclass = explicitCast("myTable");

        assertThat(regclassQuotedIdentifier).isEqualTo(regclass);
    }

    @Test
    public void test_cast_from_string_quoted_does_not_ignore_capital_case() {
        var regclassQuotedIdentifier = explicitCast("\"myTable\"");
        var regclass = explicitCast("mytable");

        assertThat(regclassQuotedIdentifier).isNotEqualTo(regclass);
    }

    @Test
    public void test_cast_from_string_raise_exception_if_not_valid_relation_name() {
        Assertions.assertThatThrownBy(() -> explicitCast("\"\"myTable\"\""))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessageContaining("Relation name \"\"\"myTable\"\"\" is invalid");
    }
}
