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

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.SqlExpressions;

public class HasTablePrivilegeFunctionTest extends ScalarTestCase {

    private static final Role TEST_USER_WITH_DQL_ON_CLUSTER =
        RolesHelper.userOf("testUserWithClusterDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.CLUSTER, null, Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_DQL =
        RolesHelper.userOf("testUserWithSysDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.SCHEMA, "sys", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_PRIVILEGES_DQL =
        RolesHelper.userOf("testUserWithSysPrivilegesDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.privileges", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_SYS_CLUSTER_DQL =
        RolesHelper.userOf("testUserWithSysClusterDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "sys.cluster", Role.CRATE_USER.name())),
            null);

    private static final Role TEST_USER_WITH_USERS_TABLE_DQL =
        RolesHelper.userOf("testUserWithUsersTableDQL", Set.of(
                new Privilege(Policy.GRANT, Permission.DQL, Securable.TABLE, "doc.users", Role.CRATE_USER.name())),
            null);

    // TODO bug?
    @Test
    public void test_user_with_cluster_dql_do_not_have_privilege_on_unknown_table() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_DQL_ON_CLUSTER, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithClusterDQL', 'sys.unknown_table', 'USAGE')", false);
    }

    // TODO bug?
    @Test
    public void test_user_with_sys_schema_dql_do_not_have_privilege_on_unknown_table_in_sys_schema() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysDQL', 'sys.unknown_table', 'USAGE')", false);
    }

    @Test
    public void test_user_with_sys_cluster_dql_do_not_have_privilege_on_sys_privileges() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_CLUSTER_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysClusterDQL', 'sys.privileges', 'USAGE')", false);
    }

    @Test
    public void test_user_with_sys_privileges_dql_have_privilege_on_sys_privileges() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_PRIVILEGES_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysPrivilegesDQL', 'sys.privileges', 'USAGE')", true);
    }

    @Test
    public void test_user_with_sys_schema_dql_have_privilege_on_sys_privileges() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithSysDQL', 'sys.privileges', 'USAGE')", true);
    }

    @Test
    public void test_user_with_cluster_dql_have_privilege_on_sys_privileges() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_DQL_ON_CLUSTER, List.of(), null);
        assertEvaluate("has_table_privilege('testUserWithClusterDQL', 'sys.privileges', 'USAGE')", true);
    }

    @Test
    public void test_without_user_parameter() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_PRIVILEGES_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.privileges', 'USAGE')", true);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_SYS_CLUSTER_DQL, List.of(), null);
        assertEvaluate("has_table_privilege('sys.privileges', 'USAGE')", false);
    }

    @Test
    public void test_table_parameter_as_oid() {
        // todo : drop doc?
        int usersTableOid = OidHash.relationOid(OidHash.Type.TABLE, new RelationName("doc", "users"));
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_USERS_TABLE_DQL, List.of(), clusterService);
        assertEvaluate("has_table_privilege(" + usersTableOid + ", 'USAGE')", true);
    }



    // test system schema/tables as oids since they are not part of cluster state

    // test doc schema?
}
