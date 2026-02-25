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
import org.elasticsearch.cluster.metadata.Metadata;
import org.jspecify.annotations.Nullable;
import org.junit.Test;

import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.RelationLookup;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;

public class RegclassTypeTest extends DataTypeTestCase<Regclass> {

    @Override
    protected DataDef<Regclass> getDataDef() {
        return DataDef.fromType(RegclassType.INSTANCE);
    }

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_cannot_cast_long_outside_int_range_to_regclass() {
        Assertions.assertThatThrownBy(() -> RegclassType.INSTANCE.explicitCast(
                Integer.MAX_VALUE + 42L,
                SESSION_SETTINGS,
                null
            ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("2147483689 is outside of `int` range and cannot be cast to the regclass type");
    }

    @Test
    public void test_cast_from_string_uses_current_schema() {
        RelationLookup relationLookup = new RelationLookup() {
            @Override
            public int getRelationOid(RelationName relationName) {
                if (relationName.equals(new RelationName("my_schema", "my_table"))) {
                    return 123;
                }
                return Metadata.OID_UNASSIGNED;
            }

            @Override
            public @Nullable RelationName getRelationName(int oid) {
                return null;
            }
        };
        var sessionSettings = new SessionSettings("crate", SearchPath.createSearchPathFrom("my_schema"));
        var regclass = RegclassType.INSTANCE.explicitCast("my_table", sessionSettings, relationLookup);
        assertThat(regclass.oid()).isEqualTo(123);
    }

    @Test
    public void test_cast_from_quoted_string_identifier() {
        RelationLookup relationLookup = new RelationLookup() {
            @Override
            public int getRelationOid(RelationName relationName) {
                if (relationName.equals(new RelationName("doc", "my_table"))) {
                    return 123;
                }
                return Metadata.OID_UNASSIGNED;
            }

            @Override
            public @Nullable RelationName getRelationName(int oid) {
                return null;
            }
        };
        var regclass = RegclassType.INSTANCE.explicitCast("\"my_table\"", SESSION_SETTINGS, relationLookup);
        assertThat(regclass.oid()).isEqualTo(123);
    }

    @Test
    public void test_cast_from_string_unquoted_ignores_capital_case() {
        RelationLookup relationLookup = new RelationLookup() {
            @Override
            public int getRelationOid(RelationName relationName) {
                if (relationName.equals(new RelationName("doc", "my_table"))) {
                    return 123;
                }
                return Metadata.OID_UNASSIGNED;
            }

            @Override
            public @Nullable RelationName getRelationName(int oid) {
                return null;
            }
        };
        var regclass = RegclassType.INSTANCE.explicitCast("my_Table", SESSION_SETTINGS, relationLookup);
        assertThat(regclass.oid()).isEqualTo(123);
    }

    @Test
    public void test_cast_from_string_raise_exception_if_not_valid_relation_name() {
        Assertions.assertThatThrownBy(() -> RegclassType.INSTANCE.explicitCast("\"\"myTable\"\"", SESSION_SETTINGS, null))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessageContaining("Relation name \"\"\"myTable\"\"\" is invalid");
    }
}
