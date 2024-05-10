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

    private static final Role TEST_USER_WITH_CLUSTER_DQL =
        RolesHelper.userOf("testUserWithClusterDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.CLUSTER, null, Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_SCHEMA_DQL =
        RolesHelper.userOf("testUserWithSysDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.SCHEMA, "sys", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL =
        RolesHelper.userOf("testUserWithSysSummitsDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.summits", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_HEALTH_TABLE_DQL =
        RolesHelper.userOf("testUserWithSysHealthDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.cluster", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_DOC_USERS_TABLE_DQL =
        RolesHelper.userOf("testUserWithUsersTableDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "doc.users", Role.CRATE_USER.name())),
            null);

    @Test
    public void test_user_with_sys_health_table_dql_do_not_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_HEALTH_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysHealthDQL', 'sys.summits', 'USAGE')", false);
    }

    @Test
    public void test_user_with_sys_summits_table_dql_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysSummitsDQL', 'sys.summits', 'USAGE')", true);
    }

    @Test
    public void test_user_with_sys_schema_dql_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SCHEMA_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysDQL', 'sys.summits', 'USAGE')", true);
    }

    @Test
    public void test_user_with_cluster_dql_have_privilege_on_sys_summits_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_CLUSTER_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithClusterDQL', 'sys.summits', 'USAGE')", true);
    }

    @Test
    public void test_has_table_privilege_function_without_user_parameter() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_SUMMITS_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.summits', 'USAGE')", true);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_HEALTH_TABLE_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.summits', 'USAGE')", false);
    }

    @Test
    public void test_has_table_privilege_function_with_table_parameter_as_oid() {
        final RelationName usersTable = new RelationName("doc", "users");
        final int usersTableOid = OidHash.relationOid(OidHash.Type.TABLE, usersTable);

        Schemas schemas = mock(Schemas.class);
        when(schemas.oidToName(usersTableOid)).thenReturn(usersTable.fqn());

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_DOC_USERS_TABLE_DQL, List.of(), schemas);
        assertEvaluate("has_table_privilege(" + usersTableOid + ", 'USAGE')", true);
    }
}
