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

package io.crate.role;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.exceptions.MissingPrivilegeException;
import io.crate.role.metadata.RolesHelper;

public class PrivilegesTest extends ESTestCase {

    private static final Role USER = RolesHelper.userOf("ford");
    private static final Roles ROLES = () -> List.of(USER);

    @Test
    public void testExceptionIsThrownIfUserHasNotRequiredPrivilege() throws Exception {
        assertThatThrownBy(() -> ensureUserHasPrivilege(Privilege.Permission.DQL, Privilege.Securable.CLUSTER, null))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing 'DQL' privilege for user 'ford'");
    }

    @Test
    public void testNoExceptionIsThrownIfUserHasNotRequiredPrivilegeOnInformationSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        ensureUserHasPrivilege(Privilege.Permission.DQL, Privilege.Securable.SCHEMA, "information_schema");
    }

    @Test
    public void testExceptionIsThrownIfUserHasNotAnyPrivilege() throws Exception {
        assertThatThrownBy(() -> ensureUserHasPrivilege(Privilege.Securable.CLUSTER, null))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'ford'");
    }

    @Test
    public void testUserWithNoPrivilegeCanAccessInformationSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        ensureUserHasPrivilege(Privilege.Securable.SCHEMA, "information_schema");
        ensureUserHasPrivilege(Privilege.Securable.TABLE, "information_schema.table");
        ensureUserHasPrivilege(Privilege.Securable.TABLE, "information_schema.views");
    }

    @Test
    public void testUserWithNoPrivilegesCanAccessPgCatalogSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `pg_catalog`
        ensureUserHasPrivilege(Privilege.Securable.SCHEMA, "pg_catalog");
        ensureUserHasPrivilege(Privilege.Securable.TABLE, "pg_catalog.pg_am");
        ensureUserHasPrivilege(Privilege.Securable.TABLE, "pg_catalog.pg_database");
    }

    private static void ensureUserHasPrivilege(Privilege.Securable clazz, String ident) {
        Privileges.ensureUserHasPrivilege(ROLES, USER, clazz, ident);
    }

    private static void ensureUserHasPrivilege(Privilege.Permission type, Privilege.Securable clazz, String ident) {
        Privileges.ensureUserHasPrivilege(ROLES, USER, type, clazz, ident);
    }
}
