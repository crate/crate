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

package io.crate.user;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.exceptions.MissingPrivilegeException;

public class PrivilegesTest extends ESTestCase {

    private static final Role USER = Role.userOf("ford");

    @Test
    public void testExceptionIsThrownIfUserHasNotRequiredPrivilege() throws Exception {
        assertThatThrownBy(() ->
            Privileges.ensureUserHasPrivilege(Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, USER))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing 'DQL' privilege for user 'ford'");
    }

    @Test
    public void testNoExceptionIsThrownIfUserHasNotRequiredPrivilegeOnInformationSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        Privileges.ensureUserHasPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "information_schema",
                                          USER);
    }

    @Test
    public void testExceptionIsThrownIfUserHasNotAnyPrivilege() throws Exception {
        assertThatThrownBy(() -> Privileges.ensureUserHasPrivilege(Privilege.Clazz.CLUSTER, null, USER))
            .isExactlyInstanceOf(MissingPrivilegeException.class)
            .hasMessage("Missing privilege for user 'ford'");
    }

    @Test
    public void testUserWithNoPrivilegeCanAccessInformationSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.SCHEMA, "information_schema", USER);
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, "information_schema.table", USER);
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, "information_schema.views", USER);
    }

    @Test
    public void testUserWithNoPrivilegesCanAccessPgCatalogSchema() throws Exception {
        //ensureUserHasPrivilege will not throw an exception if the schema is `pg_catalog`
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.SCHEMA, "pg_catalog", USER);
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, "pg_catalog.pg_am", USER);
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, "pg_catalog.pg_database", USER);
    }
}
