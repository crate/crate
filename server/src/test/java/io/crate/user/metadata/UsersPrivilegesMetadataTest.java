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

package io.crate.user.metadata;

import io.crate.user.Privilege;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class UsersPrivilegesMetadataTest extends ESTestCase {

    private static final Privilege GRANT_DQL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege GRANT_DML =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege REVOKE_DQL =
        new Privilege(Privilege.State.REVOKE, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege REVOKE_DML =
        new Privilege(Privilege.State.REVOKE, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege DENY_DQL =
        new Privilege(Privilege.State.DENY, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege GRANT_TABLE_DQL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.TABLE, "testSchema.test", "crate");
    private static final Privilege GRANT_TABLE_DDL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.TABLE, "testSchema.test2", "crate");
    private static final Privilege GRANT_VIEW_DQL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.VIEW, "testSchema.view1", "crate");
    private static final Privilege GRANT_VIEW_DDL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.VIEW, "testSchema.view2", "crate");
    private static final Privilege GRANT_VIEW_DML =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.VIEW, "view3", "crate");
    private static final Privilege GRANT_SCHEMA_DML =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.SCHEMA, "testSchema", "crate");

    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(GRANT_DQL, GRANT_DML));
    private static final List<String> USERNAMES = Arrays.asList("Ford", "Arthur");
    private static final String USER_WITHOUT_PRIVILEGES = "noPrivilegesUser";
    private static final String USER_WITH_DENIED_DQL = "userWithDeniedDQL";
    private static final String USER_WITH_SCHEMA_AND_TABLE_PRIVS = "userWithTableAndSchemaPrivs";
    private UsersPrivilegesMetadata usersPrivilegesMetadata;

    static UsersPrivilegesMetadata createMetadata() {
        Map<String, Set<Privilege>> usersPrivileges = new HashMap<>();
        for (String userName : USERNAMES) {
            usersPrivileges.put(userName, new HashSet<>(PRIVILEGES));
        }
        usersPrivileges.put(USER_WITHOUT_PRIVILEGES, new HashSet<>());
        usersPrivileges.put(USER_WITH_DENIED_DQL, new HashSet<>(Collections.singletonList(DENY_DQL)));
        usersPrivileges.put(USER_WITH_SCHEMA_AND_TABLE_PRIVS, new HashSet<>(
            Arrays.asList(GRANT_SCHEMA_DML, GRANT_TABLE_DQL, GRANT_TABLE_DDL, GRANT_VIEW_DQL, GRANT_VIEW_DML, GRANT_VIEW_DDL)));

        return new UsersPrivilegesMetadata(usersPrivileges);
    }

    @Before
    public void setUpPrivileges() {
        usersPrivilegesMetadata = createMetadata();
    }

    @Test
    public void testStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        usersPrivilegesMetadata.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersPrivilegesMetadata usersPrivilegesMetadata2 = new UsersPrivilegesMetadata(in);
        assertEquals(usersPrivilegesMetadata, usersPrivilegesMetadata2);
    }

    @Test
    public void testXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        usersPrivilegesMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder)
        );
        parser.nextToken(); // start object
        UsersPrivilegesMetadata usersPrivilegesMetadata2 = UsersPrivilegesMetadata.fromXContent(parser);
        assertEquals(usersPrivilegesMetadata, usersPrivilegesMetadata2);

        // a metadata custom must consume its surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testApplyPrivilegesSameExists() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(USERNAMES, new HashSet<>(PRIVILEGES));
        assertThat(rowCount, is(0L));
    }

    @Test
    public void testRevokeWithoutGrant() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList(USER_WITHOUT_PRIVILEGES),
            Collections.singletonList(REVOKE_DML)
        );
        assertThat(rowCount, is(0L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges(USER_WITHOUT_PRIVILEGES), empty());
    }

    @Test
    public void testRevokeWithGrant() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList("Arthur"),
            Collections.singletonList(REVOKE_DML)
        );

        assertThat(rowCount, is(1L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges("Arthur"), contains(GRANT_DQL));
    }


    @Test
    public void testRevokeWithGrantOfDifferentGrantor() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList("Arthur"),
            Collections.singletonList(new Privilege(Privilege.State.REVOKE, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "hoschi"))
        );

        assertThat(rowCount, is(1L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges("Arthur"), contains(GRANT_DQL));
    }

    @Test
    public void testDenyGrantedPrivilegeForUsers() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            USERNAMES,
            Collections.singletonList(DENY_DQL)
        );
        assertThat(rowCount, is(2L));
    }

    @Test
    public void testDenyUngrantedPrivilegeStoresTheDeny() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList(USER_WITHOUT_PRIVILEGES),
            Collections.singletonList(DENY_DQL)
        );
        assertThat(rowCount, is(1L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges(USER_WITHOUT_PRIVILEGES), contains(DENY_DQL));
    }

    @Test
    public void testRevokeDenyPrivilegeRemovesIt() throws Exception {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList(USER_WITH_DENIED_DQL),
            Collections.singletonList(REVOKE_DQL)
        );
        assertThat(rowCount, is(1L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges(USER_WITH_DENIED_DQL), empty());
    }

    @Test
    public void testDenyExistingDeniedPrivilegeIsNoOp() {
        long rowCount = usersPrivilegesMetadata.applyPrivileges(
            Collections.singletonList(USER_WITH_DENIED_DQL),
            new HashSet<>(Collections.singletonList(DENY_DQL))
        );
        assertThat(rowCount, is(0L));
        assertThat(usersPrivilegesMetadata.getUserPrivileges(USER_WITH_DENIED_DQL), contains(DENY_DQL));
    }

    @Test
    public void testTablePrivilegesAreTransferred() throws Exception {
        UsersPrivilegesMetadata usersMetadata = UsersPrivilegesMetadata.maybeCopyAndReplaceTableIdents(
            usersPrivilegesMetadata, GRANT_TABLE_DQL.ident().ident(), "testSchema.testing");

        assertThat(usersMetadata, notNullValue());

        Set<Privilege> updatedPrivileges = usersMetadata.getUserPrivileges(USER_WITH_SCHEMA_AND_TABLE_PRIVS);
        Optional<Privilege> targetPrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().ident().equals("testSchema.testing"))
            .findAny();
        assertThat(targetPrivilege.isPresent(), is(true));

        Optional<Privilege> sourcePrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().ident().equals("testSchema.test"))
            .findAny();
        assertThat(sourcePrivilege.isPresent(), is(false));

        // unrelated table privileges must be still available
        Optional<Privilege> otherTablePrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().ident().equals("testSchema.test2"))
            .findAny();
        assertThat(otherTablePrivilege.isPresent(), is(true));

        Optional<Privilege> schemaPrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().clazz().equals(Privilege.Clazz.SCHEMA))
            .findAny();
        assertThat(schemaPrivilege.isPresent() && schemaPrivilege.get().equals(GRANT_SCHEMA_DML), is(true));
    }

    @Test
    public void testDropTablePrivileges() {
        long affectedPrivileges = usersPrivilegesMetadata.dropTableOrViewPrivileges(GRANT_TABLE_DQL.ident().ident());
        assertThat(affectedPrivileges, is(1L));

        Set<Privilege> updatedPrivileges = usersPrivilegesMetadata.getUserPrivileges(USER_WITH_SCHEMA_AND_TABLE_PRIVS);
        Optional<Privilege> sourcePrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().ident().equals("testSchema.test"))
            .findAny();
        assertThat(sourcePrivilege.isPresent(), is(false));
    }

    @Test
    public void testDropViewPrivileges() {
        long affectedPrivileges = usersPrivilegesMetadata.dropTableOrViewPrivileges(GRANT_VIEW_DQL.ident().ident());
        assertThat(affectedPrivileges, is(1L));

        Set<Privilege> updatedPrivileges = usersPrivilegesMetadata.getUserPrivileges(USER_WITH_SCHEMA_AND_TABLE_PRIVS);
        Optional<Privilege> sourcePrivilege = updatedPrivileges.stream()
            .filter(p -> p.ident().ident().equals("testSchema.view1"))
            .findAny();
        assertThat(sourcePrivilege.isPresent(), is(false));
    }
}
