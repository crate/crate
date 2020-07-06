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

import io.crate.test.integration.CrateUnitTest;
import io.crate.user.SecureHash;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class UsersMetadataTest extends CrateUnitTest {

    @Test
    public void testUsersMetadataStreaming() throws IOException {
        UsersMetadata users = new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY);
        BytesStreamOutput out = new BytesStreamOutput();
        users.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersMetadata users2 = new UsersMetadata(in);
        assertEquals(users, users2);
    }

    @Test
    public void testUsersMetadataToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        UsersMetadata users = new UsersMetadata(UserDefinitions.DUMMY_USERS);
        users.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken(); // start object
        UsersMetadata users2 = UsersMetadata.fromXContent(parser);
        assertEquals(users, users2);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testUsersMetadataFromLegacyXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // Generate legacy (v1) XContent of UsersMetadata
        // { "users": [ "Ford", "Arthur" ] }
        builder.startObject();
        builder.startArray("users");
        builder.value("Ford");
        builder.value("Arthur");
        builder.endArray();
        builder.endObject();

        HashMap<String, SecureHash> expectedUsers = new HashMap<>();
        expectedUsers.put("Ford", null);
        expectedUsers.put("Arthur", null);

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken(); // start object
        UsersMetadata users = UsersMetadata.fromXContent(parser);
        assertEquals(users, new UsersMetadata(expectedUsers));

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testUsersMetadataWithoutAttributesToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        UsersMetadata users = new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY);
        users.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken(); // start object
        UsersMetadata users2 = UsersMetadata.fromXContent(parser);
        assertEquals(users, users2);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testUserMetadataWithAttributesStreaming() throws Exception {
        UsersMetadata writeUserMeta = new UsersMetadata(UserDefinitions.DUMMY_USERS);
        BytesStreamOutput out = new BytesStreamOutput();
        writeUserMeta.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersMetadata readUserMeta = new UsersMetadata(in);

        assertThat(writeUserMeta.users(), is(readUserMeta.users()));
    }
}
