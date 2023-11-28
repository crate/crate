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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isNotSameInstance;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.MissingPrivilegeException;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.testing.Asserts;
import io.crate.testing.SqlExpressions;
import io.crate.user.Privilege;
import io.crate.user.Role;

public class HasSchemaPrivilegeFunctionTest extends ScalarTestCase {

    private static final Role TEST_USER = Role.userOf("test");
    private static final Role TEST_USER_WITH_AL_ON_CLUSTER =
        Role.userOf("testUserWithClusterAL",
                Set.of(new Privilege(Privilege.State.GRANT, Privilege.Type.AL, Privilege.Clazz.CLUSTER, "crate", Role.CRATE_USER.name())),
                null);
    private static final Role TEST_USER_WITH_DQL_ON_SYS =
        Role.userOf("testUserWithSysDQL",
                Set.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "sys.privileges", Role.CRATE_USER.name())),
                null);

    @Before
    public void prepare() {
        sqlExpressions = new SqlExpressions(
            tableSources, null, randomFrom(TEST_USER_WITH_AL_ON_CLUSTER, TEST_USER_WITH_DQL_ON_SYS, Role.CRATE_USER), List.of(TEST_USER));
    }

    @Test
    public void test_no_user_compile_gets_new_instance() {
        assertCompileAsSuperUser("has_schema_privilege(name, 'USAGE')", isNotSameInstance());
    }

    @Test
    public void test_user_is_literal_compile_gets_new_instance() {
        // Using name column as schema name since having 3 literals leads to skipping even compilation and returning computed Literal
        assertCompileAsSuperUser("has_schema_privilege('crate', name, 'USAGE')", isNotSameInstance());
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("has_schema_privilege(null, 'pg_catalog', 'USAGE')");
        assertEvaluateNull("has_schema_privilege('test', null, 'USAGE')");
        assertEvaluateNull("has_schema_privilege('test', 'pg_catalog', null)");
    }

    @Test
    public void test_throws_error_when_user_is_not_found() {
        assertThatThrownBy(
            () -> assertEvaluate("has_schema_privilege('not_existing_user', 'pg_catalog', ' USAGE')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("User not_existing_user does not exist");
    }

    @Test
    public void test_throws_error_when_invalid_privilege() {
        assertThatThrownBy(
            () -> assertEvaluate("has_schema_privilege('test', 'pg_catalog', ' USAGE, CREATE, SELECT')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unrecognized privilege type: select");
        assertThatThrownBy(
            () -> assertEvaluate("has_schema_privilege('test', 'pg_catalog', '')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unrecognized privilege type: ");
    }

    @Test
    public void test_throws_error_when_user_without_related_privileges_is_checking_for_other_user() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(TEST_USER_WITH_AL_ON_CLUSTER));
        assertThatThrownBy(
            () -> assertEvaluate("has_schema_privilege('testUserWithClusterAL', 'pg_catalog', ' USAGE, CREATE, SELECT')", null))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }

    @Test
    public void test_throws_error_when_user_without_related_privileges_is_checking_for_other_user_for_compiled() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(TEST_USER_WITH_AL_ON_CLUSTER));
        assertThatThrownBy(
            () -> assertCompile("has_schema_privilege('testUserWithClusterAL', name, 'USAGE')",
                                TEST_USER, () -> List.of(TEST_USER, TEST_USER_WITH_AL_ON_CLUSTER),
                                s -> s1 -> Asserts.fail("should fail with MissingPrivilegeException")))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }

    @Test
    public void test_user_without_permission_doesnt_have_privilege_for_regular_schema() {
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege('test', 'doc', 'CREATE')", false);
    }

    @Test
    public void test_user_with_DQL_permission_has_USAGE_but_not_CREATE_privilege_for_regular_schema() {
        Privilege usage = new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate");
        var user = Role.userOf("test", Set.of(usage), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user);
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('test', 'doc', 'create, USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('test', 'doc', 'CREATE')", false);

        // Same as above but we take current user (which is test so outcome is same)
        assertEvaluate("has_schema_privilege('doc', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('doc', 'create, USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('doc', 'CREATE')", false);
    }

    @Test
    public void test_user_with_DDL_permission_has_CREATE_but_not_USAGE_privilege_for_regular_schema() {
        // having CREATE doesn't mean having USAGE - checked in PG13 as well.
        Privilege create = new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc", "crate");
        var user = Role.userOf("test", Set.of(create), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user);
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('test', 'doc', 'CREATE')", true);

        // Same as above but we take current user (which is test so outcome is same)
        assertEvaluate("has_schema_privilege('doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege('doc', 'USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('doc', 'CREATE')", true);
    }

    @Test
    public void test_user_without_permission_has_USAGE_but_not_CREATE_privilege_for_public_schemas() {
        assertEvaluate("has_schema_privilege('test', '" + InformationSchemaInfo.NAME + "', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('test', '" + PgCatalogSchemaInfo.NAME + "', 'USAGE')", true);

        assertEvaluate("has_schema_privilege('test', '" + InformationSchemaInfo.NAME + "', 'CREATE')", false);
        assertEvaluate("has_schema_privilege('test', '" + PgCatalogSchemaInfo.NAME + "', 'CREATE')", false);

        // Same as above but we take current user (which is test so outcome is same)
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
        assertEvaluate("has_schema_privilege('" + InformationSchemaInfo.NAME + "', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('" + PgCatalogSchemaInfo.NAME + "', 'USAGE')", true);

        assertEvaluate("has_schema_privilege('" + InformationSchemaInfo.NAME + "', 'CREATE')", false);
        assertEvaluate("has_schema_privilege('" + PgCatalogSchemaInfo.NAME + "', 'CREATE')", false);
    }

    @Test
    public void test_same_results_for_name_and_oid() {
        int schemaOid = OidHash.schemaOid("doc");
        int userOid = OidHash.userOid("test");
        // Testing all 6 possible signatures.
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege('test', " + schemaOid + ", 'USAGE')", false);

        assertEvaluate("has_schema_privilege(" + userOid + ", 'doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege(" + userOid + "," + schemaOid + ", 'USAGE')", false);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
        assertEvaluate("has_schema_privilege('doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege(" + schemaOid + ", 'USAGE')", false);
    }
}
