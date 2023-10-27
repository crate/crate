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

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.HashMap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.user.SecureHash;

public class UsersMetadataTest extends ESTestCase {

    @Test
    public void testUsersMetadataStreaming() throws IOException {
        UsersMetadata users = new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY);
        BytesStreamOutput out = new BytesStreamOutput();
        users.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersMetadata users2 = new UsersMetadata(in);
        assertThat(users2).isEqualTo(users);
    }

    @Test
    public void testUsersMetadataToXContent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

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
        assertThat(users2).isEqualTo(users);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void testUsersMetadataFromLegacyXContent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

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
        assertThat(new UsersMetadata(expectedUsers)).isEqualTo(users);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void testUsersMetadataWithoutAttributesToXContent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

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
        assertThat(users2).isEqualTo(users);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void testUserMetadataWithAttributesStreaming() throws Exception {
        UsersMetadata writeUserMeta = new UsersMetadata(UserDefinitions.DUMMY_USERS);
        BytesStreamOutput out = new BytesStreamOutput();
        writeUserMeta.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersMetadata readUserMeta = new UsersMetadata(in);

        assertThat(readUserMeta.users()).isEqualTo(writeUserMeta.users());
    }
}
