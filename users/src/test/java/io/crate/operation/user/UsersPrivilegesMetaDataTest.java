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

import com.google.common.collect.ImmutableSet;
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
        UsersPrivilegesMetaData usersPrivilegesMetaData2 = (UsersPrivilegesMetaData) new UsersPrivilegesMetaData().readFrom(in);
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

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken(); // start object
        UsersPrivilegesMetaData usersPrivilegesMetaData2 = (UsersPrivilegesMetaData)new UsersPrivilegesMetaData().fromXContent(parser);
        assertEquals(usersPrivilegesMetaData, usersPrivilegesMetaData2);
    }
}
