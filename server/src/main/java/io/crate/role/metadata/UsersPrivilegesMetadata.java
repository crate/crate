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

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.role.Privilege;

@Deprecated(since = "5.6.0")
public class UsersPrivilegesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "users_privileges";

    private final Map<String, Set<Privilege>> usersPrivileges;

    @VisibleForTesting
    public UsersPrivilegesMetadata() {
        usersPrivileges = new HashMap<>();
    }

    @VisibleForTesting
    public UsersPrivilegesMetadata(Map<String, Set<Privilege>> usersPrivileges) {
        this.usersPrivileges = usersPrivileges;
    }


    @Nullable
    public Set<Privilege> getUserPrivileges(String userName) {
        return usersPrivileges.get(userName);
    }

    private void createPrivileges(String userName, Set<Privilege> privileges) {
        usersPrivileges.put(userName, privileges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UsersPrivilegesMetadata that = (UsersPrivilegesMetadata) o;
        return Objects.equals(usersPrivileges, that.usersPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usersPrivileges);
    }

    public UsersPrivilegesMetadata(StreamInput in) throws IOException {
        int numUsersPrivileges = in.readVInt();
        usersPrivileges = HashMap.newHashMap(numUsersPrivileges);
        for (int i = 0; i < numUsersPrivileges; i++) {
            String userName = in.readString();
            int numPrivileges = in.readVInt();
            Set<Privilege> privileges = HashSet.newHashSet(numPrivileges);
            for (int j = 0; j < numPrivileges; j++) {
                privileges.add(new Privilege(in));
            }
            usersPrivileges.put(userName, privileges);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(usersPrivileges.size());
        for (Map.Entry<String, Set<Privilege>> entry : usersPrivileges.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (Privilege privilege : entry.getValue()) {
                privilege.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Set<Privilege>> entry : usersPrivileges.entrySet()) {
            builder.startArray(entry.getKey());
            for (Privilege privilege : entry.getValue()) {
                privilege.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder;
    }

    /**
     * UsersPrivilegesMetadata has the form of:
     *
     * users_privileges: {
     *   "user1": [
     *      {"state": 1, "type": 2, "class": 3, "ident": "some_table", "grantor": "grantor_username"},
     *      {"state": 2, "type": 1, "class": 3, "ident": "some_schema", "grantor": "grantor_username"},
     *      ...
     *     ]
     *   "user2": [
     *     ...
     *   ],
     *   ...
     * }
     */
    public static UsersPrivilegesMetadata fromXContent(XContentParser parser) throws IOException {
        UsersPrivilegesMetadata metadata = new UsersPrivilegesMetadata();
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String userName = parser.currentName();
            Set<Privilege> privileges = metadata.getUserPrivileges(userName);
            if (privileges == null) {
                privileges = new HashSet<>();
                metadata.createPrivileges(userName, privileges);
            }
            XContentParser.Token token;
            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        privileges.add(Privilege.fromXContent(parser));
                    }
                }
            }
        }
        return metadata;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_1;
    }
}
