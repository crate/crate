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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class SubscriptionsMetadataTest extends ESTestCase {

    private static TableOrPartition target(String table) {
        return new TableOrPartition(RelationName.fromIndexName(table), null);
    }

    public static SubscriptionsMetadata createMetadata() {
        Map<String, Subscription> map = Map.of(
            "sub1",
            new Subscription(
                "user1",
                ConnectionInfo.fromURL("crate://example.com:4310?user=valid_user&password=123"),
                List.of("pub1"),
                Settings.EMPTY,
                Map.of(
                    target("doc.t1"),
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
                    target("doc.t1"),
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
    public void test_streaming_partition_targets() throws IOException {
        var target = new TableOrPartition(new RelationName("doc", "t1"), "04132");
        var subscription = new Subscription(
            "user1",
            ConnectionInfo.fromURL("crate://example.com:4310"),
            List.of("pub1"),
            Settings.EMPTY,
            Map.of(target, new Subscription.RelationState(Subscription.State.INITIALIZING, null))
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);
        subscription.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_5_0);
        var streamed = new Subscription(in);

        assertThat(streamed.relations()).containsOnlyKeys(target);
    }

    @Test
    public void test_streaming_table_targets_to_older_version() throws IOException {
        var target = new TableOrPartition(new RelationName("doc", "t1"), null);
        var subscription = new Subscription(
            "user1",
            ConnectionInfo.fromURL("crate://example.com:4310"),
            List.of("pub1"),
            Settings.EMPTY,
            Map.of(target, new Subscription.RelationState(Subscription.State.INITIALIZING, null))
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_0);
        subscription.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_4_0);
        var streamed = new Subscription(in);

        assertThat(streamed.relations()).containsOnlyKeys(target);
    }

    @Test
    public void test_cannot_stream_partition_targets_to_older_version() {
        var target = new TableOrPartition(new RelationName("doc", "t1"), "04132");
        var subscription = new Subscription(
            "user1",
            ConnectionInfo.fromURL("crate://example.com:4310"),
            List.of("pub1"),
            Settings.EMPTY,
            Map.of(target, new Subscription.RelationState(Subscription.State.INITIALIZING, null))
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_0);

        assertThatThrownBy(() -> subscription.writeTo(out))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cannot write partition subscription target to a node before");
    }
}
