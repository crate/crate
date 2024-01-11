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

package io.crate.auth;

import static io.crate.role.metadata.RolesHelper.userOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.ESTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;

public class AccessControlMaySeeTest extends ESTestCase {

    private List<List<Object>> validationCallArguments;
    private Role user;
    private AccessControl accessControl;

    @Before
    public void setUpUserAndValidator() {
        validationCallArguments = new ArrayList<>();
        user = userOf("normal");
        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(user);
            }

            @Override
            public boolean hasAnyPrivilege(Role user, Securable securable, @Nullable String ident) {
                validationCallArguments.add(CollectionUtils.arrayAsArrayList(securable, ident, user.name()));
                return true;
            }
        };
        accessControl = new AccessControlImpl(roles, new CoordinatorSessionSettings(user));
    }

    private void assertAskedAnyForCluster() {
        assertThat(validationCallArguments).satisfiesExactly(
            s -> assertThat(s).containsExactly(Securable.CLUSTER, null, user.name()));
    }

    private void assertAskedAnyForSchema(String ident) {
        assertThat(validationCallArguments).satisfiesExactly(
            s -> assertThat(s).containsExactly(Securable.SCHEMA, ident, user.name()));
    }

    private void assertAskedAnyForTable(String ident) {
        assertThat(validationCallArguments).contains(List.of(Securable.TABLE, ident, user.name()));
    }

    @Test
    public void testTableScopeException() throws Exception {
        accessControl.ensureMaySee(new RelationValidationException(List.of(
            RelationName.fromIndexName("users"),
            RelationName.fromIndexName("my_schema.foo")
        ), "bla"));
        assertAskedAnyForTable("doc.users");
        assertAskedAnyForTable("my_schema.foo");
    }

    @Test
    public void testSchemaScopeException() throws Exception {
        accessControl.ensureMaySee(new SchemaUnknownException("my_schema"));
        assertAskedAnyForSchema("my_schema");
    }

    @Test
    public void testClusterScopeException() throws Exception {
        accessControl.ensureMaySee(new UnsupportedFeatureException("unsupported"));
        assertAskedAnyForCluster();
    }

    @Test
    public void testUnscopedException() throws Exception {
        accessControl.ensureMaySee(new UnhandledServerException("unhandled"));
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_ColumnUnknownException_with_null_RelationName() {
        accessControl.ensureMaySee(
            ColumnUnknownException.ofUnknownRelation("The object `{x=10}` does not contain the key `y`"));
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_ColumnUnknownException_originated_from_built_in_table_function() {
        // select x from empty_row();
        accessControl.ensureMaySee(
            ColumnUnknownException.ofTableFunctionRelation(
                "Column x unknown", new RelationName(null, "empty_row")));
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_ColumnUnknownException_originated_from_udf_table_function() {
        // select x from my_schema.empty_row();
        accessControl.ensureMaySee(
            ColumnUnknownException.ofTableFunctionRelation(
                "Column x unknown", new RelationName("my_schema", "empty_row")));
        assertAskedAnyForSchema("my_schema");
    }

    @Test
    public void test_ColumnUnknownException_originated_from_table() {
        // select x from empty_row;
        accessControl.ensureMaySee(
            new ColumnUnknownException(
                new ColumnIdent("x"), new RelationName("doc", "empty_row")));
        assertAskedAnyForTable("doc.empty_row");
    }

    @Test
    public void test_UnsupportedFunctionException_with_null_schema() {
        // select * from unknown_function();
        accessControl.ensureMaySee(new UnsupportedFunctionException("Unknown Function: unknown_function()", null));
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_UnsupportedFunctionException_with_non_null_schema() {
        // select * from doc.unknown_function();
        accessControl.ensureMaySee(new UnsupportedFunctionException("Unknown Function: doc.unknown_function()", "doc"));
        assertAskedAnyForSchema("doc");
    }
}
