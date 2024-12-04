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

package io.crate.expression.scalar.postgres;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;

public class PgTableIsVisibleFunctionTest extends ScalarTestCase {

    @Test
    public void test_returns_null_for_null_input() {
        assertEvaluateNull("pg_catalog.pg_table_is_visible(null)");
    }

    @Test
    public void test_return_false_for_unknown_table() {
        Schemas schemas = mock(Schemas.class);
        when(schemas.getRelation(anyInt())).thenReturn(null);
        sqlExpressions = new SqlExpressions(tableSources, null, Role.CRATE_USER, List.of(), schemas);

        assertEvaluate("pg_table_is_visible(-1)", false);
    }

    @Test
    public void test_table_is_not_visible_without_one_of_DQL_DML_DDL() throws IOException {
        String[] searchPaths = new String[]{"my_schema"};
        Schemas schemas = SQLExecutor.of(clusterService)
            .addTable("create table my_schema.my_table(id int)")
            .schemas();
        sqlExpressions = new SqlExpressions(tableSources, null, RolesHelper.userOf("dummy_user"), List.of(), schemas, searchPaths);

        final RelationName usersTable = new RelationName("my_schema", "my_table");
        final int usersTableOid = OidHash.relationOid(OidHash.Type.TABLE, usersTable);

        assertEvaluate("pg_table_is_visible(" + usersTableOid + ")", false);
    }

    @Test
    public void test_table_is_visible_with_DQL_or_DML_or_DDL_on_table() throws IOException {
        String[] searchPaths = new String[]{"my_schema"};
        Schemas schemas = SQLExecutor.of(clusterService)
            .addTable("create table my_schema.my_table(id int)")
            .schemas();

        // DQL
        sqlExpressions = new SqlExpressions(tableSources,
            null,
            RolesHelper.userOf(
                "dummy_user",
                Set.of(new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "my_schema.my_table", "crate")),
                null
                ),
            List.of(),
            schemas,
            searchPaths);
        final RelationName usersTable = new RelationName("my_schema", "my_table");
        final int usersTableOid = OidHash.relationOid(OidHash.Type.TABLE, usersTable);
        assertEvaluate("pg_table_is_visible(" + usersTableOid + ")", true);

        // DML
        sqlExpressions = new SqlExpressions(tableSources,
            null,
            RolesHelper.userOf(
                "dummy_user",
                Set.of(new Privilege(Policy.GRANT, Permission.DML, Securable.TABLE, "my_schema.my_table", "crate")),
                null
            ),
            List.of(),
            schemas,
            searchPaths);
        assertEvaluate("pg_table_is_visible(" + usersTableOid + ")", true);

        // DDL
        sqlExpressions = new SqlExpressions(tableSources,
            null,
            RolesHelper.userOf(
                "dummy_user",
                Set.of(new Privilege(Policy.GRANT, Permission.DDL, Securable.TABLE, "my_schema.my_table", "crate")),
                null
            ),
            List.of(),
            schemas,
            searchPaths);
        assertEvaluate("pg_table_is_visible(" + usersTableOid + ")", true);
    }

    @Test
    public void test_table_is_not_visible_if_same_name_table_earlier_in_search_path_is_visible() throws IOException {
        String[] searchPaths = new String[]{"my_schema1", "my_schema2"};
        Schemas schemas = SQLExecutor.of(clusterService)
            .addTable("create table my_schema1.my_table(id int)")
            .addTable("create table my_schema2.my_table(id int)")
            .schemas();
        sqlExpressions = new SqlExpressions(tableSources,
            null,
            RolesHelper.userOf(
                "dummy_user",
                Set.of(
                    new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "my_schema1.my_table", "crate"),
                    new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "my_schema2.my_table", "crate")
                ),
                null
            ),
            List.of(),
            schemas,
            searchPaths);

        // my_schema1.my_table is visible
        final RelationName mySchema1MyTable = new RelationName("my_schema1", "my_table");
        final int mySchema1MyTableOid = OidHash.relationOid(OidHash.Type.TABLE, mySchema1MyTable);
        assertEvaluate("pg_table_is_visible(" + mySchema1MyTableOid + ")", true);

        // since my_schema1.my_table is visible(my_schema1 appears earlier in searchPaths),
        // my_schema2.my_table is not visible
        final RelationName mySchema2MyTable = new RelationName("my_schema2", "my_table");
        final int mySchema2MyTableOid = OidHash.relationOid(OidHash.Type.TABLE, mySchema2MyTable);
        assertEvaluate("pg_table_is_visible(" + mySchema2MyTableOid + ")", false);
    }

    @Test
    public void test_table_is_visible_if_same_name_table_earlier_in_search_path_is_not_visible() throws IOException {
        String[] searchPaths = new String[]{"my_schema1", "my_schema2"};
        Schemas schemas = SQLExecutor.of(clusterService)
            .addTable("create table my_schema1.my_table(id int)")
            .addTable("create table my_schema2.my_table(id int)")
            .schemas();
        sqlExpressions = new SqlExpressions(tableSources,
            null,
            RolesHelper.userOf(
                "dummy_user",
                Set.of(
                    new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "my_schema2.my_table", "crate")
                ),
                null
            ),
            List.of(),
            schemas,
            searchPaths);

        // my_schema1.my_table is NOT visible, user is missing privileges
        final RelationName mySchema1MyTable = new RelationName("my_schema1", "my_table");
        final int mySchema1MyTableOid = OidHash.relationOid(OidHash.Type.TABLE, mySchema1MyTable);
        assertEvaluate("pg_table_is_visible(" + mySchema1MyTableOid + ")", false);

        // since my_schema1.my_table is NOT visible, my_schema2.my_table is visible
        final RelationName mySchema2MyTable = new RelationName("my_schema2", "my_table");
        final int mySchema2MyTableOid = OidHash.relationOid(OidHash.Type.TABLE, mySchema2MyTable);
        assertEvaluate("pg_table_is_visible(" + mySchema2MyTableOid + ")", true);
    }
}
