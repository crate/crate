/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.metadata;

import io.crate.user.SecureHash;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
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

public class UsersMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "users";

    private final Map<String, SecureHash> users;

    public UsersMetaData() {
        this.users = new HashMap<>();
    }

    public UsersMetaData(Map<String, SecureHash> users) {
        this.users = users;
    }

    public static UsersMetaData newInstance(@Nullable UsersMetaData instance) {
        if (instance == null) {
            return new UsersMetaData();
        }
        return new UsersMetaData(new HashMap<>(instance.users));
    }

    public boolean contains(String name) {
        return users.containsKey(name);
    }

    public void add(String name, @Nullable SecureHash attributes) {
        users.put(name, attributes);
    }

    public void remove(String name) {
        users.remove(name);
    }

    public List<String> users() {
        return new ArrayList<>(users.keySet());
    }

    public Map<String, SecureHash> userHashes() {
        return users;
    }

    public UsersMetaData(StreamInput in) throws IOException {
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
        for (Map.Entry<String, SecureHash> entry : users.entrySet()) {
            builder.startObject(entry.getKey());
            if (entry.getValue() != null) {
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * UsersMetaData has the form of:
     *
     * {
     *     "user1": {
     *         "secure_hash": {
     *             "iterations": INT,
     *             "hash": BYTE[],
     *             "salt": BYTE[]
     *         }
     *     },
     *     ...
     * }
     */
    public static UsersMetaData fromXContent(XContentParser parser) throws IOException {
        Map<String, SecureHash> users = new HashMap<>();

        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String userName = parser.currentName();
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {  // user
                users.put(userName, secureHashFromXContent(parser));
            }
        }
        return new UsersMetaData(users);
    }

    private static SecureHash secureHashFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token currentToken;
        int iterations = 0;
        byte[] hash = new byte[0];
        byte[] salt = new byte[0];
        boolean hasPassword = false;

        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) { // secure_hash
                    hasPassword = true;
                    if (currentToken == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        currentToken = parser.nextToken();
                        switch (currentFieldName) {
                            case "iterations":
                                if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                                    throw new ElasticsearchParseException(
                                        "failed to parse SecureHash, 'iterations' value is not a number [{}]", currentToken);
                                }
                                iterations = parser.intValue();
                                break;
                            case "hash":
                                if (currentToken.isValue()) {
                                    throw new ElasticsearchParseException(
                                        "failed to parse SecureHash, 'hash' does not contain any value [{}]", currentToken);
                                }
                                hash = parser.binaryValue();
                                break;
                            case "salt":
                                if (currentToken.isValue()) {
                                    throw new ElasticsearchParseException(
                                        "failed to parse SecureHash, 'salt' does not contain any value [{}]", currentToken);
                                }
                                salt = parser.binaryValue();
                                break;
                            default:
                                throw new ElasticsearchParseException("failed to parse secure_hash");
                        }
                    }
                }
            }
        }

        if (hasPassword) {
            return SecureHash.of(iterations, salt, hash);
        }
        return null;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UsersMetaData that = (UsersMetaData) o;
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
}
