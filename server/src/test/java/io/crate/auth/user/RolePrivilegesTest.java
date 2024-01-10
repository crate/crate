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

package io.crate.auth.user;

import static io.crate.testing.Asserts.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.role.Privilege;
import io.crate.role.PrivilegeState;
import io.crate.role.RolePrivileges;

public class RolePrivilegesTest extends ESTestCase {

    private static final Collection<Privilege> PRIVILEGES_CLUSTER_DQL = Set.of(
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")
    );

    private static final Collection<Privilege> PRIVILEGES_SCHEMA_DQL = Set.of(
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate")
    );

    private static final Collection<Privilege> PRIVILEGES_TABLE_DQL = Set.of(
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1", "crate")
    );

    private static final RolePrivileges USER_PRIVILEGES_CLUSTER = new RolePrivileges(PRIVILEGES_CLUSTER_DQL);
    private static final RolePrivileges USER_PRIVILEGES_SCHEMA = new RolePrivileges(PRIVILEGES_SCHEMA_DQL);
    private static final RolePrivileges USER_PRIVILEGES_TABLE = new RolePrivileges(PRIVILEGES_TABLE_DQL);


    @Test
    public void testMatchPrivilegesEmpty() throws Exception {
        RolePrivileges rolePrivileges = new RolePrivileges(Collections.emptyList());
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null)).isMissing();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc")).isMissing();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.TABLE, "doc.t1")).isMissing();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.CLUSTER, null)).isMissing();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.SCHEMA, "doc")).isMissing();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isMissing();
    }

    @Test
    public void testMatchPrivilegeNoType() throws Exception {
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null)).isMissing();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc")).isMissing();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DDL, Privilege.Clazz.TABLE, "doc.t1")).isMissing();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.CLUSTER, null)).isGranted();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.SCHEMA, "doc")).isGranted();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isGranted();
    }

    @Test
    public void testMatchPrivilegeType() throws Exception {
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null)).isGranted();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.CLUSTER, null)).isGranted();
    }

    @Test
    public void testMatchPrivilegeSchema() throws Exception {
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc")).isGranted();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.SCHEMA, "doc")).isGranted();
        assertThat(USER_PRIVILEGES_SCHEMA.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc")).isGranted();
        assertThat(USER_PRIVILEGES_SCHEMA.matchPrivilegeOfAnyType(Privilege.Clazz.SCHEMA, "doc")).isGranted();
    }

    @Test
    public void testMatchPrivilegeTable() throws Exception {
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(USER_PRIVILEGES_CLUSTER.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(USER_PRIVILEGES_SCHEMA.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(USER_PRIVILEGES_SCHEMA.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(USER_PRIVILEGES_TABLE.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(USER_PRIVILEGES_TABLE.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isGranted();
    }

    @Test
    public void testMatchPrivilegeDenyResultsInNoMatch() throws Exception {
        Collection<Privilege> privileges = Set.of(
            new Privilege(PrivilegeState.DENY, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")
        );
        RolePrivileges rolePrivileges = new RolePrivileges(privileges);
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null)).isDenied();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc")).isDenied();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1")).isDenied();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.CLUSTER, null)).isDenied();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.SCHEMA, "doc")).isDenied();
        assertThat(rolePrivileges.matchPrivilegeOfAnyType(Privilege.Clazz.TABLE, "doc.t1")).isDenied();
    }

    @Test
    public void testMatchPrivilegeComplexSetIncludingDeny() throws Exception {
        Collection<Privilege> privileges = Set.of(
            new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(PrivilegeState.DENY, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate"),
            new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1", "crate")
        );
        RolePrivileges rolePrivileges = new RolePrivileges(privileges);
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1")).isGranted();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t2")).isDenied();
        assertThat(rolePrivileges.matchPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "my_schema")).isGranted();
    }
}
