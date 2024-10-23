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
import static io.crate.role.PrivilegesModifierTest.GRANT_AL;
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
import static io.crate.role.PrivilegesModifierTest.USER_WITH_TABLE_VIEW_SCHEMA_AL_PRIVS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
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
        usersPrivileges.put(USER_WITH_TABLE_VIEW_SCHEMA_AL_PRIVS, new HashSet<>(
            Arrays.asList(
                GRANT_SCHEMA_DML,
                GRANT_TABLE_DQL, GRANT_TABLE_DDL,
                GRANT_VIEW_DQL, GRANT_VIEW_DML, GRANT_VIEW_DDL,
                GRANT_AL)
        ));

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
}
