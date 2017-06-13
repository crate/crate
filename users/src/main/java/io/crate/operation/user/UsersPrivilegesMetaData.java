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

package io.crate.operation.user;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.user.Privilege;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class UsersPrivilegesMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom {

    static final String TYPE = "users_privileges";
    static final UsersPrivilegesMetaData PROTO = new UsersPrivilegesMetaData();

    static UsersPrivilegesMetaData copyOf(@Nullable UsersPrivilegesMetaData oldMetaData) {
        if (oldMetaData == null) {
            return new UsersPrivilegesMetaData();
        }
        return new UsersPrivilegesMetaData(new HashMap<>(oldMetaData.usersPrivileges));
    }

    private final Map<String, Set<Privilege>> usersPrivileges;

    @VisibleForTesting
    UsersPrivilegesMetaData() {
        usersPrivileges = new HashMap<>();
    }

    @VisibleForTesting
    UsersPrivilegesMetaData(Map<String, Set<Privilege>> usersPrivileges) {
        this.usersPrivileges = usersPrivileges;
    }

    @Nullable
    Set<Privilege> getUserPrivileges(String userName) {
        return usersPrivileges.get(userName);
    }

    void createPrivileges(String userName, Set<Privilege> privileges) {
        usersPrivileges.put(userName, privileges);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UsersPrivilegesMetaData that = (UsersPrivilegesMetaData) o;
        return Objects.equals(usersPrivileges, that.usersPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usersPrivileges);
    }

    @Override
    public MetaData.Custom readFrom(StreamInput in) throws IOException {
        int numUsersPrivileges = in.readVInt();
        Map<String, Set<Privilege>> usersPrivileges = new HashMap<>(numUsersPrivileges);
        for (int i = 0; i < numUsersPrivileges; i++) {
            String userName = in.readString();
            int numPrivileges = in.readVInt();
            Set<Privilege> privileges = new HashSet<>(numPrivileges);
            for (int j = 0; j < numPrivileges; j++) {
                Privilege privilege = new Privilege();
                privilege.readFrom(in);
                privileges.add(privilege);
            }
            usersPrivileges.put(userName, privileges);
        }

        return new UsersPrivilegesMetaData(usersPrivileges);
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
        builder.startObject(TYPE);
        for (Map.Entry<String, Set<Privilege>> entry : usersPrivileges.entrySet()) {
            builder.startArray(entry.getKey());
            for (Privilege privilege : entry.getValue()) {
                privilegeToXContent(privilege, builder);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
        Map<String, Set<Privilege>> usersPrivileges = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals(TYPE)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse users privileges");
            }
            XContentParser.Token token;
            String userName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                Set<Privilege> privileges = new HashSet<>();
                if (token == XContentParser.Token.FIELD_NAME) {
                    userName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
                        privileges.add(privilegeFromXContent(parser, token));
                    }
                    usersPrivileges.put(userName, privileges);
                } else {
                    throw new ElasticsearchParseException("failed to parse users privileges");
                }
            }
            return new UsersPrivilegesMetaData(usersPrivileges);
        }
        throw new ElasticsearchParseException("failed to parse users privileges");
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    private static void privilegeToXContent(Privilege privilege, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("state", privilege.state().ordinal());
        builder.field("type", privilege.type().ordinal());
        builder.field("class", privilege.clazz().ordinal());
        builder.field("ident", privilege.ident());
        builder.field("grantor", privilege.grantor());
        builder.endObject();
    }

    private Privilege privilegeFromXContent(XContentParser parser, XContentParser.Token currentToken) throws IOException {
        if (currentToken == XContentParser.Token.START_OBJECT) {
            Privilege.State state = null;
            Privilege.Type type = null;
            Privilege.Clazz clazz = null;
            String ident = null;
            String grantor = null;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    currentToken = parser.nextToken();
                    switch (currentFieldName) {
                        case "state":
                            if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                                throw new ElasticsearchParseException(
                                    "failed to parse privilege, 'state' value is not a number [{}]", currentToken);
                            }
                            state = Privilege.State.values()[parser.intValue()];
                            break;
                        case "type":
                            if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                                throw new ElasticsearchParseException(
                                    "failed to parse privilege, 'type' value is not a number [{}]", currentToken);
                            }
                            type = Privilege.Type.values()[parser.intValue()];
                            break;
                        case "class":
                            if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                                throw new ElasticsearchParseException(
                                    "failed to parse privilege, 'class' value is not a number [{}]", currentToken);
                            }
                            clazz = Privilege.Clazz.values()[parser.intValue()];
                            break;
                        case "ident":
                            if (currentToken != XContentParser.Token.VALUE_STRING
                                && currentToken != XContentParser.Token.VALUE_NULL) {
                                throw new ElasticsearchParseException(
                                    "failed to parse privilege, 'ident' value is not a string or null [{}]", currentToken);
                            }
                            ident = parser.textOrNull();
                            break;
                        case "grantor":
                            if (currentToken != XContentParser.Token.VALUE_STRING) {
                                throw new ElasticsearchParseException(
                                    "failed to parse privilege, 'grantor' value is not a string [{}]", currentToken);
                            }
                            grantor = parser.text();
                            break;
                        default:
                            throw new ElasticsearchParseException("failed to parse privilege");
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse privilege");
                }
            }
            return new Privilege(state, type, clazz, ident, grantor);
        }
        throw new ElasticsearchParseException("failed to parse privilege");
    }
}
