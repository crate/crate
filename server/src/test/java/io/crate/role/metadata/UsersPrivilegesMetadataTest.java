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

package io.crate.role.metadata;

import static io.crate.role.PrivilegesModifierTest.DENY_DQL;
import static io.crate.role.PrivilegesModifierTest.GRANT_SCHEMA_DML;
import static io.crate.role.PrivilegesModifierTest.GRANT_TABLE_DDL;
import static io.crate.role.PrivilegesModifierTest.GRANT_TABLE_DQL;
import static io.crate.role.PrivilegesModifierTest.GRANT_VIEW_DDL;
import static io.crate.role.PrivilegesModifierTest.GRANT_VIEW_DML;
import static io.crate.role.PrivilegesModifierTest.GRANT_VIEW_DQL;
import static io.crate.role.PrivilegesModifierTest.PRIVILEGES;
import static io.crate.role.PrivilegesModifierTest.USERNAMES;
import static io.crate.role.PrivilegesModifierTest.USER_WITHOUT_PRIVILEGES;
import static io.crate.role.PrivilegesModifierTest.USER_WITH_DENIED_DQL;
import static io.crate.role.PrivilegesModifierTest.USER_WITH_SCHEMA_AND_TABLE_PRIVS;
import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.role.Privilege;

public class UsersPrivilegesMetadataTest extends ESTestCase {

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
        assertThat(usersPrivilegesMetadata2).isEqualTo(usersPrivilegesMetadata);
    }

    @Test
    public void testXContent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

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
        assertThat(usersPrivilegesMetadata2).isEqualTo(usersPrivilegesMetadata);

        // a metadata custom must consume its surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

}
