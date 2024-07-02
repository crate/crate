/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class SubscriptionsMetadataTest extends ESTestCase {

    public static SubscriptionsMetadata createMetadata() {
        Map<String, Subscription> map = Map.of(
            "sub1",
            new Subscription(
                "user1",
                ConnectionInfo.fromURL("crate://example.com:4310?user=valid_user&password=123"),
                List.of("pub1"),
                Settings.EMPTY,
                Map.of(
                    RelationName.fromIndexName("doc.t1"),
                    new Subscription.RelationState(Subscription.State.INITIALIZING, null)
                )
            ),
            "my_subscription",
            new Subscription(
                "user2",
                ConnectionInfo.fromURL("crate://localhost"),
                List.of("some_publication", "another_publication"),
                Settings.builder().put("enable", "true").build(),
                Map.of(
                    RelationName.fromIndexName("doc.t1"),
                    new Subscription.RelationState(Subscription.State.FAILED, "Subscription failed on restore")
                )
            )
        );
        return new SubscriptionsMetadata(map);
    }

    @Test
    public void testStreaming() throws IOException {
        SubscriptionsMetadata subs = createMetadata();
        BytesStreamOutput out = new BytesStreamOutput();
        subs.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        var subs2 = new SubscriptionsMetadata(in);
        assertThat(subs2).isEqualTo(subs);

    }

    @Test
    public void testToXContent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        var subs = createMetadata();
        subs.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // start object
        var subs2 = SubscriptionsMetadata.fromXContent(parser);
        assertThat(subs2).isEqualTo(subs);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }
}
