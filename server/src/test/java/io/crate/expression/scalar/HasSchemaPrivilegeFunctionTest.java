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

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.testing.Asserts;
import io.crate.testing.SqlExpressions;
import io.crate.user.Privilege;
import io.crate.user.StubUserManager;
import io.crate.user.User;

public class HasSchemaPrivilegeFunctionTest extends ScalarTestCase {

    private static User TEST_USER = User.of("test");

    @Before
    private void prepare() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
    }

    @Test
    public void test_no_user_compile_gets_new_instance() {
        assertCompile("has_schema_privilege(name, 'USAGE')", isNotSameInstance(), new StubUserManager());
    }

    @Test
    public void test_user_is_literal_compile_gets_new_instance() {
        // Using name column as schema name since having 3 literals leads to skipping even compilation and returning computed Literal
        assertCompile("has_schema_privilege('crate', name, 'USAGE')", isNotSameInstance());
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("has_schema_privilege(null, 'pg_catalog', 'USAGE')");
        assertEvaluateNull("has_schema_privilege('test', null, 'USAGE')");
        assertEvaluateNull("has_schema_privilege('test', 'pg_catalog', null)");
    }

    @Test
    public void test_throws_error_when_user_is_not_found() {
        sqlExpressions = new SqlExpressions(tableSources, null, null, null);

        Asserts.assertThrowsMatches(
            () -> assertEvaluate("has_schema_privilege('not_existing_user', 'pg_catalog', ' USAGE')", null),
            IllegalArgumentException.class,
            "User not_existing_user does not exist"
        );
    }

    @Test
    public void test_throws_error_when_invalid_privilege() {
        Asserts.assertThrowsMatches(
            () -> assertEvaluate("has_schema_privilege('test', 'pg_catalog', ' USAGE, CREATE, SELECT')", null),
            IllegalArgumentException.class,
            "Unrecognized privilege type: select"
        );
    }

    @Test
    public void test_user_without_permission_doesnt_have_privilege_for_regular_schema() {
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege('test', 'doc', 'CREATE')", false);
    }

    @Test
    public void test_user_with_DQL_permission_has_USAGE_but_not_CREATE_privilege_for_regular_schema() {
        Privilege usage = new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate");
        var user = User.of("test", Set.of(usage), null);
        sqlExpressions = new SqlExpressions(tableSources, null, user);
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('test', 'doc', 'USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('test', 'doc', 'CREATE')", false);

        // Same as above but we take current user (which is test so outcome is same)
        assertEvaluate("has_schema_privilege('doc', 'USAGE')", true);
        assertEvaluate("has_schema_privilege('doc', 'USAGE, CREATE')", true); // true if has at least one privilege
        assertEvaluate("has_schema_privilege('doc', 'CREATE')", false);
    }

    @Test
    public void test_user_with_DDL_permission_has_CREATE_but_not_USAGE_privilege_for_regular_schema() {
        // having CREATE doesn't mean having USAGE - checked in PG13 as well.
        Privilege create = new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc", "crate");
        var user = User.of("test", Set.of(create), null);
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

        assertEvaluate("has_schema_privilege('doc', 'USAGE')", false);
        assertEvaluate("has_schema_privilege(" + schemaOid + ", 'USAGE')", false);

    }

}
