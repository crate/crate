/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.user;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class PrivilegeTest extends CrateUnitTest {

    private static final Collection<Privilege> PRIVILEGES_CLUSTER_DQL = Sets.newHashSet(
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")
    );

    private static final Collection<Privilege> PRIVILEGES_SCHEMA_DQL = Sets.newHashSet(
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate")
    );

    private static final Collection<Privilege> PRIVILEGES_TABLE_DQL = Sets.newHashSet(
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1", "crate")
    );

    @Test
    public void testMatchPrivilegesEmpty() throws Exception {
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc"), is(false));
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Type.DDL, Privilege.Clazz.TABLE, "doc.t1"), is(false));
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Clazz.SCHEMA, "doc"), is(false));
        assertThat(Privilege.matchPrivilege(Collections.emptyList(), Privilege.Clazz.TABLE, "doc.t1"), is(false));
    }

    @Test
    public void testMatchPrivilegeNoType() throws Exception {
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DDL, Privilege.Clazz.SCHEMA, "doc"), is(false));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DDL, Privilege.Clazz.TABLE, "doc.t1"), is(false));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.CLUSTER, null), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.SCHEMA, "doc"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
    }

    @Test
    public void testMatchPrivilegeType() throws Exception {
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.CLUSTER, null), is(true));
    }

    @Test
    public void testMatchPrivilegeSchema() throws Exception {
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.SCHEMA, "doc"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_SCHEMA_DQL, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_SCHEMA_DQL, Privilege.Clazz.SCHEMA, "doc"), is(true));
    }

    @Test
    public void testMatchPrivilegeTable() throws Exception {
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_CLUSTER_DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_SCHEMA_DQL, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_SCHEMA_DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_TABLE_DQL, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(PRIVILEGES_TABLE_DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
    }

    @Test
    public void testMatchPrivilegeDenyResultsInNoMatch() throws Exception {
        Collection<Privilege> privileges = Sets.newHashSet(
            new Privilege(Privilege.State.DENY, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")
        );
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc"), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1"), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Clazz.SCHEMA, "doc"), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Clazz.TABLE, "doc.t1"), is(false));
    }

    @Test
    public void testMatchPrivilegeComplexSetIncludingDeny() throws Exception {
        Collection<Privilege> privileges = Sets.newHashSet(
            new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.DENY, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "doc", "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1", "crate")
        );
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t1"), is(true));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.TABLE, "doc.t2"), is(false));
        assertThat(Privilege.matchPrivilege(privileges, Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "my_schema"), is(true));
    }
}
