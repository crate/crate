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

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.user.Privilege;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.nullValue;

public class UsersPrivilegesMetaDataTest extends CrateUnitTest {

    private Map<String, Set<Privilege>> usersPrivileges;

    @Before
    public void setUpPrivileges() {
        usersPrivileges = new HashMap<>();
        usersPrivileges.put("Arthur", ImmutableSet.<Privilege>builder()
            .add(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"))
            .add(new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate"))
            .build());
        usersPrivileges.put("Ford", ImmutableSet.<Privilege>builder()
            .add(new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null, "crate"))
            .build());
    }

    @Test
    public void testStreaming() throws IOException {
        UsersPrivilegesMetaData usersPrivilegesMetaData = new UsersPrivilegesMetaData(usersPrivileges);
        BytesStreamOutput out = new BytesStreamOutput();
        usersPrivilegesMetaData.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UsersPrivilegesMetaData usersPrivilegesMetaData2 = new UsersPrivilegesMetaData(in);
        assertEquals(usersPrivilegesMetaData, usersPrivilegesMetaData2);
    }

    @Test
    public void testXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        UsersPrivilegesMetaData usersPrivilegesMetaData = new UsersPrivilegesMetaData(usersPrivileges);
        usersPrivilegesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), builder.bytes());
        parser.nextToken(); // start object
        UsersPrivilegesMetaData usersPrivilegesMetaData2 = UsersPrivilegesMetaData.fromXContent(parser);
        assertEquals(usersPrivilegesMetaData, usersPrivilegesMetaData2);

        // a metadata custom must consume its surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken(), nullValue());
    }
}
