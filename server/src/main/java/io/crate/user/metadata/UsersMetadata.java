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

import io.crate.user.SecureHash;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UsersMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "users";

    private final Map<String, SecureHash> users;

    public UsersMetadata() {
        this.users = new HashMap<>();
    }

    public UsersMetadata(Map<String, SecureHash> users) {
        this.users = users;
    }

    public static UsersMetadata newInstance(@Nullable UsersMetadata instance) {
        if (instance == null) {
            return new UsersMetadata();
        }
        return new UsersMetadata(new HashMap<>(instance.users));
    }

    public boolean contains(String name) {
        return users.containsKey(name);
    }

    public void put(String name, @Nullable SecureHash secureHash) {
        users.put(name, secureHash);
    }

    public void remove(String name) {
        users.remove(name);
    }

    public List<String> userNames() {
        return new ArrayList<>(users.keySet());
    }

    public Map<String, SecureHash> users() {
        return users;
    }

    public UsersMetadata(StreamInput in) throws IOException {
        int numUsers = in.readVInt();
        users = new HashMap<>(numUsers);
        for (int i = 0; i < numUsers; i++) {
            String userName = in.readString();
            SecureHash secureHash = in.readOptionalWriteable(SecureHash::readFrom);
            users.put(userName, secureHash);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(users.size());
        for (Map.Entry<String, SecureHash> user : users.entrySet()) {
            out.writeString(user.getKey());
            out.writeOptionalWriteable(user.getValue());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("users");
        for (Map.Entry<String, SecureHash> entry : users.entrySet()) {
            builder.startObject(entry.getKey());
            if (entry.getValue() != null) {
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * UsersMetadata v2 has the form of:
     *
     * users: {
     *   "user1": {
     *     "secure_hash": {
     *       "iterations": INT,
     *       "hash": BYTE[],
     *       "salt": BYTE[]
     *     }
     *   },
     *   "user2": {
     *     "secure_hash": null
     *   },
     *   ...
     * }
     *
     * UsersMetadata v1 has the form of:
     *
     * users: [
     *   "user1",
     *   "user2",
     *   ...
     * ]
     *
     */
    public static UsersMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, SecureHash> users = new HashMap<>();
        XContentParser.Token token = parser.nextToken();

        if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals("users")) {
            token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                // UsersMetadata v1
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
                    users.put(parser.text(), null); // old users do not have passwords
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // UsersMetadata v2
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String userName = parser.currentName();
                    if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        users.put(userName, SecureHash.fromXContent(parser));
                    }
                }
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse users, expected an object token at the end");
            }
        }
        return new UsersMetadata(users);
    }


    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UsersMetadata that = (UsersMetadata) o;
        return users.equals(that.users);
    }

    @Override
    public int hashCode() {
        return Objects.hash(users);
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
