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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.role.Privilege;
import io.crate.role.PrivilegeState;
import io.crate.role.RoleManager;

public class SysPrivilegesIntegrationTest extends BaseRolesIntegrationTest {

    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate")));
    private static final List<String> USERNAMES = Arrays.asList("ford", "arthur", "normal");


    @Before
    public void setUpNodesUsersAndPrivileges() throws Exception {
        for (String userName : USERNAMES) {
            executeAsSuperuser("create user " + userName);
        }

        RoleManager roleManager = cluster().getInstance(RoleManager.class);
        Long rowCount = roleManager.applyPrivileges(USERNAMES, PRIVILEGES).get(5, TimeUnit.SECONDS);
        assertThat(rowCount).isEqualTo(6);
    }

    @After
    public void dropUsersAndPrivileges() {
        for (String userName : USERNAMES) {
            executeAsSuperuser("drop user " + userName);
        }
    }

    @Test
    public void testTableColumns() {
        executeAsSuperuser("select column_name, data_type from information_schema.columns" +
                " where table_name='privileges' and table_schema='sys'");
        assertThat(response).hasRows(
            "class| text",
            "grantee| text",
            "grantor| text",
            "ident| text",
            "state| text",
            "type| text");
    }

    @Test
    public void testListingAsSuperUser() {
        executeAsSuperuser("select * from sys.privileges order by grantee, type");
        assertThat(response).hasRows(
            "CLUSTER| arthur| crate| NULL| GRANT| DML",
            "CLUSTER| arthur| crate| NULL| GRANT| DQL",
            "CLUSTER| ford| crate| NULL| GRANT| DML",
            "CLUSTER| ford| crate| NULL| GRANT| DQL",
            "CLUSTER| normal| crate| NULL| GRANT| DML",
            "CLUSTER| normal| crate| NULL| GRANT| DQL");
    }

    @Test
    public void testListingAsUserWithPrivilege() {
        executeAsSuperuser("select * from sys.privileges order by grantee, type");
        assertThat(response).hasRows(
            "CLUSTER| arthur| crate| NULL| GRANT| DML",
            "CLUSTER| arthur| crate| NULL| GRANT| DQL",
            "CLUSTER| ford| crate| NULL| GRANT| DML",
            "CLUSTER| ford| crate| NULL| GRANT| DQL",
            "CLUSTER| normal| crate| NULL| GRANT| DML",
            "CLUSTER| normal| crate| NULL| GRANT| DQL");
    }
}
