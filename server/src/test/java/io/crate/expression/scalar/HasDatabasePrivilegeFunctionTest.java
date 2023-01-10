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

import io.crate.Constants;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.testing.Asserts;
import io.crate.testing.SqlExpressions;
import io.crate.user.Privilege;
import io.crate.user.User;

public class HasDatabasePrivilegeFunctionTest extends ScalarTestCase {

    private static final User TEST_USER = User.of("test");
    private static final User TEST_USER_WITH_CREATE =
        User.of("testWithCreate",
                Set.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc", User.CRATE_USER.name())),
                null);
    private static final User TEST_USER_WITH_AL_ON_CLUSTER =
        User.of("testUserWithClusterAL",
                Set.of(new Privilege(Privilege.State.GRANT, Privilege.Type.AL, Privilege.Clazz.CLUSTER, "crate", User.CRATE_USER.name())),
                null);
    private static final User TEST_USER_WITH_DQL_ON_SYS =
        User.of("testUserWithSysDQL",
                Set.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "sys.privileges", User.CRATE_USER.name())),
                null);

    @Before
    public void prepare() {
        sqlExpressions = new SqlExpressions(
            tableSources, null, randomFrom(User.CRATE_USER, TEST_USER_WITH_AL_ON_CLUSTER, TEST_USER_WITH_DQL_ON_SYS),
            List.of(User.CRATE_USER, TEST_USER, TEST_USER_WITH_CREATE));
    }

    @Test
    public void test_no_user_compile_gets_new_instance() {
        assertCompileAsSuperUser("has_database_privilege(name, 'CONNECT')", isNotSameInstance());
    }

    @Test
    public void test_user_is_literal_compile_gets_new_instance() {
        // Using name column as schema name since having 3 literals leads to skipping even compilation and returning computed Literal
        assertCompileAsSuperUser("has_database_privilege('crate', name, 'CONNECT')", isNotSameInstance());
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("has_database_privilege(null, 'crate', 'TEMP')");
        assertEvaluateNull("has_database_privilege('test', null, ' TEMPORARY , CREATE')");
        assertEvaluateNull("has_database_privilege('test', 'crate', null)");
    }

    @Test
    public void test_throws_error_when_user_is_not_found() {
        assertThatThrownBy(
            () -> assertEvaluate("has_database_privilege('not_existing_user', 'crate', ' CONNECT')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("User not_existing_user does not exist");
    }

    @Test
    public void test_throws_error_when_invalid_privilege() {
        assertThatThrownBy(
            () -> assertEvaluate("has_database_privilege('test', 'pg_catalog', 'TEMP , CREATE , SELECT')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unrecognized privilege type: select");
        assertThatThrownBy(
            () -> assertEvaluate("has_database_privilege('test', 'pg_catalog', '')", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unrecognized privilege type: ");
    }

    @Test
    public void test_throws_error_when_user_is_not_super_user_checking_for_other_user() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(TEST_USER_WITH_AL_ON_CLUSTER));
        assertThatThrownBy(
            () -> assertEvaluate("has_database_privilege('testUserWithClusterAL', 'crate', 'CREATE')", null))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }

    @Test
    public void test_throws_error_when_user_is_not_super_user_checking_for_other_user_for_compiled() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(TEST_USER_WITH_AL_ON_CLUSTER));
        assertThatThrownBy(
            () -> assertCompile("has_database_privilege('testUserWithClusterAL', name, 'CREATE')",
                                TEST_USER, () -> List.of(TEST_USER, TEST_USER_WITH_AL_ON_CLUSTER),
                                s -> s1 -> Asserts.fail("should fail with MissingPrivilegeException")))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }

    @Test
    public void test_no_privilege_for_db_other_than_crate() {
        assertThatThrownBy(
            () -> assertEvaluate("has_database_privilege('test', 'myDB', 'CONNECT')", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("database \"myDB\" does not exist");
    }

    @Test
    public void test_no_privilege_other_than_connect() {
        assertEvaluate("has_database_privilege('test', 'crate', 'CONNECT')", true);
        assertEvaluate("has_database_privilege('test', 'crate', 'TEMP')", false);
        assertEvaluate("has_database_privilege('test', 'crate', 'CREATE')", false);
        assertEvaluate("has_database_privilege('test', 'crate', 'CREATE, TEMP')", false);
        assertEvaluate("has_database_privilege('test', 'crate', 'CREATE, TEMP, CONNECT')", true);
    }

    @Test
    public void test_create_privilege() {
        assertEvaluate("has_database_privilege('crate', 'crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege('testWithCreate', 'crate', 'CONNECT')", true);
        assertEvaluate("has_database_privilege('testWithCreate', 'crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege('testWithCreate', 'crate', 'CREATE, CONNECT, create')", true);
        assertEvaluate("has_database_privilege('testWithCreate', 'crate', 'CONNECT, temp, CREATE, TEMP')", true);

        // Same as above but with session user
        sqlExpressions = new SqlExpressions(tableSources, null, User.CRATE_USER);
        assertEvaluate("has_database_privilege('crate', 'CREATE')", true);
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_CREATE);
        assertEvaluate("has_database_privilege('crate', 'CONNECT')", true);
        assertEvaluate("has_database_privilege('crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege('crate', 'CREATE, CONNECT, create')", true);
        assertEvaluate("has_database_privilege('crate', 'CONNECT, temp, CREATE, TEMP')", true);
    }

    @Test
    public void test_same_results_for_name_and_oid() {
        int dbOid = Constants.DB_OID;
        int userOid = OidHash.userOid("test");
        int crateUserOid = OidHash.userOid(User.CRATE_USER.name());
        // Testing all 6 possible signatures, for a normal user but also for superuser.
        assertEvaluate("has_database_privilege('crate', 'crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege('test', 'crate', 'CREATE')", false);
        assertEvaluate("has_database_privilege('crate', " + dbOid + ", 'CREATE')", true);
        assertEvaluate("has_database_privilege('test', " + dbOid + ", 'CREATE')", false);

        assertEvaluate("has_database_privilege(" + crateUserOid + ", 'crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege(" + userOid + ", 'crate', 'CONNECT')", true);
        assertEvaluate("has_database_privilege(" + crateUserOid + "," + dbOid + ", 'CREATE')", true);
        assertEvaluate("has_database_privilege(" + userOid + "," + dbOid + ", 'CONNECT')", true);

        sqlExpressions = new SqlExpressions(tableSources, null, User.CRATE_USER);
        assertEvaluate("has_database_privilege('crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege(" + dbOid + ", 'CREATE')", true);

        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER_WITH_CREATE);
        assertEvaluate("has_database_privilege('crate', 'CREATE')", true);
        assertEvaluate("has_database_privilege(" + dbOid + ", 'CREATE')", true);
    }
}
