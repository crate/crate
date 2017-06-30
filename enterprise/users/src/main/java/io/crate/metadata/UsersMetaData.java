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
import java.util.List;
import java.util.Objects;

public class UsersMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "users";

    private final List<String> users;

    public UsersMetaData() {
        this.users = new ArrayList<>();
    }

    public UsersMetaData(List<String> users) {
        this.users = users;
    }

    public static UsersMetaData newInstance(@Nullable UsersMetaData instance) {
        if (instance == null) {
            return new UsersMetaData();
        }
        return new UsersMetaData(new ArrayList<>(instance.users));
    }

    public boolean contains(String name) {
        return users.contains(name);
    }

    public void add(String name) {
        users.add(name);
    }

    public void remove(String name) {
        users.remove(name);
    }

    public List<String> users() {
        return users;
    }

    public UsersMetaData(StreamInput in) throws IOException {
        int numUsers = in.readVInt();
        users = new ArrayList<>(numUsers);
        for (int i = 0; i < numUsers; i++) {
            users.add(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringList(users);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("users");
        for (String user : users) {
            builder.value(user);
        }
        builder.endArray();
        return builder;
    }

    public static UsersMetaData fromXContent(XContentParser parser) throws IOException {
        List<String> users = new ArrayList<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals("users")) {
            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
                    users.add(parser.text());
                }
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse users, expected an object token at the end");
            }
        }
        return new UsersMetaData(users);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UsersMetaData that =(UsersMetaData)o;
        return Objects.equals(users, that.users);
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
