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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.fdw.ServersMetadata.Server;

public class ServersMetadataTest extends ESTestCase {

    @Test
    public void test_xcontent_roundtrip() throws Exception {
        Map<String, Map<String, Object>> pg1Users = Map.of(
            "crate",
            Map.of(
                "user", "trillian",
                "password", "secret"
            )
        );
        Map<String, Object> pg1Options = Map.of("url", "jdbc:postgresql://localhost:5432");
        Server pg1 = new Server("pg1", "jdbc", "crate", pg1Users, pg1Options);
        Server pg2 = new Server("pg2", "jdbc", "arthur", Map.of(), Map.of());
        var servers = new ServersMetadata(Map.of(pg1.name(), pg1, pg2.name(), pg2));

        XContentBuilder builder = JsonXContent.builder();
        builder.startObject();
        servers.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken();
        ServersMetadata deserializedServers = ServersMetadata.fromXContent(parser);

        assertThat(deserializedServers).isEqualTo(servers);
        assertThat(parser.nextToken())
            .as("Must not have any left-over tokens")
            .isNull();
    }
}

