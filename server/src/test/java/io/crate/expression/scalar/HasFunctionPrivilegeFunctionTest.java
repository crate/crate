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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.RoleUnknownException;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.Asserts;
import io.crate.testing.SqlExpressions;

public class HasFunctionPrivilegeFunctionTest extends ScalarTestCase {

    private static final Role TEST_USER = RolesHelper.userOf("test");

    @Before
    public void prepare() {
        sqlExpressions = new SqlExpressions(
            tableSources, null, randomFrom(Role.CRATE_USER, TEST_USER),
            List.of(Role.CRATE_USER, TEST_USER), null);
    }

    @Test
    public void test_builtin_functions() {
        assertEvaluate("pg_catalog.has_function_privilege('abs()', 'EXECUTE')", true);
        assertEvaluate("pg_catalog.has_function_privilege('abs(10)', 'EXECUTE')", true);
        assertEvaluate("pg_catalog.has_function_privilege('abs(integer)', 'EXECUTE')", true);
        // currently, we don't use the function args to resolve the function, as PostgreSQL does, only it's name
        assertEvaluate("pg_catalog.has_function_privilege('abs(integer, float)', 'EXECUTE')", true);
        assertEvaluate("pg_catalog.has_function_privilege('abs()', 'EXECUTE,   EXECUTE  ')", true);
        assertEvaluate("pg_catalog.has_function_privilege('test', '\"abs\"()', 'EXECUTE')", true);
    }

    @Test
    public void test_functions_in_pg_catalog() {
        assertEvaluate("pg_catalog.has_function_privilege('pg_catalog.pg_sleep()', 'EXECUTE')", true);
        assertEvaluate("pg_catalog.has_function_privilege('\"pg_catalog\".\"pg_sleep\"()', 'EXECUTE,   EXECUTE  ')", true);
        assertEvaluate("pg_catalog.has_function_privilege('pg_catalog.pg_sleep(123)', 'EXECUTE')", true);
        assertEvaluate("pg_catalog.has_function_privilege('test', 'pg_catalog.pg_sleep( 123  )', 'EXECUTE')", true);
        // resolve function from SearchPath
        assertEvaluate("pg_catalog.has_function_privilege('pg_sleep()', 'EXECUTE')", true);
    }

    @Test
    public void test_throws_error_when_invalid_privilege() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
        assertThatThrownBy(() ->
            assertEvaluate("pg_catalog.has_function_privilege('abs()', 'CREATE')", true))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unrecognized permission: create");
        assertThatThrownBy(() ->
            assertEvaluate("pg_catalog.has_function_privilege('abs()', 'EXECUTE,   CREATE  ')", true))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unrecognized permission: create");
        assertThatThrownBy(() ->
            assertEvaluate("pg_catalog.has_function_privilege('test', 'abs()', 'DELETE')", true))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unrecognized permission: delete");
    }

    @Test
    public void test_with_oid() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
        // replace() function
        assertEvaluate("has_function_privilege(866866358, 'EXECUTE')", true);
        assertEvaluate("has_function_privilege('test', 866866358, 'EXECUTE')", true);
        // pg_catalog.generate_series() function
        assertEvaluate("has_function_privilege(405848532, 'EXECUTE')", true);
        assertEvaluate("has_function_privilege('test', 405848532, 'EXECUTE')", true);
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("has_function_privilege(null, 'EXECUTE')");
        assertEvaluateNull("has_function_privilege('abs()', null)");
        assertEvaluateNull("has_function_privilege('test', 'abs()', null)");
        assertEvaluateNull("has_function_privilege(null, 'abs()', null)");
    }

    @Test
    public void test_throws_error_when_user_is_not_found() {
        assertThatThrownBy(
            () -> assertEvaluate("has_function_privilege('not_existing_user', 'abs()', ' EXECUTE')", null))
            .isExactlyInstanceOf(RoleUnknownException.class)
            .hasMessage("Role 'not_existing_user' does not exist");
    }

    @Test
    public void test_throws_error_when_user_is_not_super_user_checking_for_other_user() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(Role.CRATE_USER), null);
        assertThatThrownBy(
            () -> assertEvaluate("has_function_privilege('crate', 'abs()', 'EXECUTE')", null))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }

    @Test
    public void test_throws_error_when_user_is_not_super_user_checking_for_other_user_for_compiled() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, List.of(Role.CRATE_USER), null);
        assertThatThrownBy(
            () -> assertCompile("has_function_privilege('crate', 'abs()', 'EXECUTE')",
                                TEST_USER, () -> List.of(TEST_USER, Role.CRATE_USER),
                                s -> s1 -> Asserts.fail("should fail with MissingPrivilegeException")))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'test'");
    }
}
