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

package io.crate.expression.scalar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.SqlExpressions;

public class HasTablePrivilegeFunctionTest extends ScalarTestCase {

    private static final Role TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL =
        RolesHelper.userOf("testUserWithSysSummitsDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.summits", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_HEALTH_TABLE_DQL =
        RolesHelper.userOf("testUserWithSysHealthDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.health", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_HEALTH_TABLE_DML =
        RolesHelper.userOf("testUserWithSysHealthDML", Set.of(
                new Privilege(Policy.GRANT, Permission.DML, Securable.TABLE, "sys.health", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_DOC_USERS_TABLE_DQL =
        RolesHelper.userOf("testUserWithUsersTableDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "doc.users", Role.CRATE_USER.name())),
            null);

    @Test
    public void test_user_with_sys_health_table_dql_do_not_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_HEALTH_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysHealthDQL', 'sys.summits', 'SELECT')", false);
    }

    @Test
    public void test_user_with_sys_summits_table_dql_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysSummitsDQL', 'sys.summits', 'SELECT')", true);
    }

    @Test
    public void test_has_table_privilege_function_without_user_parameter() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.summits', 'SELECT')", true);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_HEALTH_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.summits', 'SELECT')", false);
    }

    @Test
    public void test_has_table_privilege_function_with_table_as_oid() {
        final RelationName usersTable = new RelationName("doc", "users");
        final int usersTableOid = OidHash.relationOid(OidHash.Type.TABLE, usersTable);

        Schemas schemas = mock(Schemas.class);
        when(schemas.getRelation(usersTableOid)).thenReturn(usersTable);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_DOC_USERS_TABLE_DQL, List.of(), schemas);
        assertEvaluate("has_table_privilege(" + usersTableOid + ", 'SELECT')", true);
    }

    @Test
    public void test_has_table_privilege_function_with_system_table_as_oid() {
        final RelationName sysHealth = new RelationName("sys", "health");
        final int sysHealthOid = OidHash.relationOid(OidHash.Type.TABLE, sysHealth);

        Schemas schemas = mock(Schemas.class);
        when(schemas.getRelation(sysHealthOid)).thenReturn(sysHealth);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_HEALTH_TABLE_DML, List.of(), schemas);
        assertEvaluate("has_table_privilege(" + sysHealthOid + ", 'UPDATE')", true);
    }

    @Test
    public void test_return_true_if_any_of_the_listed_privileges_is_held() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.summits', 'SELECT, INSERT')", true); // INSERT(DML) is not held but still returns 'true'
    }

    @Test
    public void test_throws_if_listed_privileges_contain_invalid_privilege() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertThatThrownBy(() -> assertEvaluate("has_table_privilege('sys.summits', 'SELECT, TRUNCATE')", true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unrecognized permission: truncate");
    }

    @Test
    public void test_unknown_tables_with_different_privilege_classes() {
        Schemas schemas = mock(Schemas.class);
        when(schemas.getRelation(anyInt())).thenReturn(null);

        int tableOid = OidHash.relationOid(OidHash.Type.TABLE, new RelationName("my_schema", "unknown_table"));

        // super user
        sqlExpressions = new SqlExpressions(tableSources, null, Role.CRATE_USER, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", true);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", true);

        // a user with cluster dql
        Privilege create = new Privilege(Policy.GRANT, Permission.DQL, Securable.CLUSTER, null, "crate");
        var user = RolesHelper.userOf("test", Set.of(create), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", true);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", true);

        // a user with cluster ddl(NOT dql)
        Privilege create2 = new Privilege(Policy.GRANT, Permission.DDL, Securable.CLUSTER, null, "crate");
        var user2 = RolesHelper.userOf("test", Set.of(create2), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user2, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", false);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", false);

        // a user with 'my_schema' dql
        Privilege create3 = new Privilege(Policy.GRANT, Permission.DQL, Securable.SCHEMA, "my_schema", "crate");
        var user3 = RolesHelper.userOf("test", Set.of(create3), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user3, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", true);
        // NOTE: this returns false because cannot retrieve 'my_schema' from the tableOid
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", false);

        // a user with 'my_schema' ddl(NOT dql)
        Privilege create4 = new Privilege(Policy.GRANT, Permission.DDL, Securable.SCHEMA, "my_schema", "crate");
        var user4 = RolesHelper.userOf("test", Set.of(create4), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user4, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", false);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", false);

        // returns false otherwise
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_DOC_USERS_TABLE_DQL, List.of(), schemas);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'SELECT')", false);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'SELECT')", false);
        assertEvaluate("has_table_privilege('my_schema.unknown_table', 'INSERT')", false);
        assertEvaluate("has_table_privilege(" + tableOid + ", 'INSERT')", false);
    }
}
