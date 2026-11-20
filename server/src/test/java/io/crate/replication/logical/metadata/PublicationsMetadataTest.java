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
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class PublicationsMetadataTest extends ESTestCase {

    private static TableOrPartition target(String schema, String table) {
        return new TableOrPartition(new RelationName(schema, table), null);
    }

    public static PublicationsMetadata createMetadata() {
        Map<String, Publication> map = Map.of(
            "pub1",
            new Publication(
                "user1",
                false,
                List.of(target("s1", "t1"), target("s1", "t2"))
            ),
            "myPub2",
            new Publication("user2", true, List.of())
        );
        return new PublicationsMetadata(map);
    }

    @Test
    public void testStreaming() throws IOException {
        PublicationsMetadata pubs = createMetadata();
        BytesStreamOutput out = new BytesStreamOutput();
        pubs.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PublicationsMetadata pubs2 = new PublicationsMetadata(in);
        assertThat(pubs2).isEqualTo(pubs);

    }

    @Test
    public void test_streaming_partition_targets() throws IOException {
        var target = new TableOrPartition(new RelationName("doc", "t1"), "04132");
        var publication = new Publication(
            "user1",
            false,
            List.of(target)
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_6_0);
        publication.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_6_0);
        var streamed = new Publication(in);

        assertThat(streamed.targets()).containsExactly(target);
    }

    @Test
    public void test_streaming_table_targets_to_older_version() throws IOException {
        var target = new TableOrPartition(new RelationName("doc", "t1"), null);
        var publication = new Publication(
            "user1",
            false,
            List.of(target)
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);
        publication.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_5_0);
        var streamed = new Publication(in);

        assertThat(streamed.targets()).containsExactly(target);
    }

    @Test
    public void test_cannot_stream_partition_targets_to_older_version() {
        var publication = new Publication(
            "user1",
            false,
            List.of(new TableOrPartition(new RelationName("doc", "t1"), "04132"))
        );
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);

        assertThatThrownBy(() -> publication.writeTo(out))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cannot write partition publication target to a node before");
    }
}
