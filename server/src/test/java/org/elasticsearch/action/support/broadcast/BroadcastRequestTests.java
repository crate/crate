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

package org.elasticsearch.action.support.broadcast;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.retention.SyncRetentionLeasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest.Target;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class BroadcastRequestTests extends ESTestCase {

    @Test
    public void test_streaming_table_oid() throws Exception {
        var partition = new PartitionName(new RelationName("doc", "tbl"), List.of());
        for (var version : List.of(Version.V_6_4_0, Version.V_6_5_0)) {
            for (int tableOid : new int[] { OID_UNASSIGNED, 1234 }) {
                var request = new BroadcastRequest(List.of(new Target(partition.relationName(), tableOid, partition.values())));
                var out = new BytesStreamOutput();
                out.setVersion(version);
                request.writeTo(out);
                var in = out.bytes().streamInput();
                in.setVersion(version);
                var streamed = new BroadcastRequest(in);
                assertThat(streamed.targets()).hasSize(1);
                assertThat(streamed.targets().get(0).relationName()).isEqualTo(partition.relationName());
                assertThat(streamed.targets().get(0).partitionValues()).isEqualTo(partition.values());
                assertThat(streamed.targets().getFirst().tableOid()).isEqualTo(
                    version.onOrAfter(Version.V_6_5_0) ? tableOid : OID_UNASSIGNED);
                assertThat(in.available()).isZero();
            }
        }
    }

    @Test
    public void test_multiple_targets_streaming_for_all_request_types() throws Exception {
        var relation = new RelationName("doc", "parted");
        var targets = List.of(
            new Target(relation, 1234, List.of("first")),
            new Target(relation, 1234, Collections.singletonList(null)),
            new Target(new RelationName("doc", "other"), 5678, List.of())
        );
        for (var version : List.of(Version.V_5_10_0, Version.V_6_0_0, Version.V_6_4_0, Version.V_6_5_0)) {
            var expected = version.onOrAfter(Version.V_6_5_0) ? targets : targets.stream()
                .map(t -> new Target(t.relationName(), OID_UNASSIGNED, t.partitionValues())).toList();
            assertThat(roundTrip(new BroadcastRequest(targets), BroadcastRequest::new, version).targets()).isEqualTo(expected);
            assertThat(roundTrip(new RefreshRequest(targets), RefreshRequest::new, version).targets()).isEqualTo(expected);
            assertThat(roundTrip(new SyncRetentionLeasesRequest(targets), SyncRetentionLeasesRequest::new, version).targets())
                .isEqualTo(expected);
            var merge = new ForceMergeRequest(targets).maxNumSegments(2).onlyExpungeDeletes(true).flush(false);
            var streamed = roundTrip(merge, ForceMergeRequest::new, version);
            assertThat(streamed.targets()).isEqualTo(expected);
            assertThat(streamed.maxNumSegments()).isEqualTo(2);
            assertThat(streamed.onlyExpungeDeletes()).isTrue();
            assertThat(streamed.flush()).isFalse();
            assertThat(streamed.forceMergeUUID()).isEqualTo(merge.forceMergeUUID());
            assertThat(roundTrip(new BroadcastRequest(List.of()), BroadcastRequest::new, version).targets()).isEmpty();
        }
    }

    private static <T extends BroadcastRequest> T roundTrip(T request, Writeable.Reader<T> reader, Version version)
        throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.setVersion(version);
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(version);
                T streamed = reader.read(in);
                assertThat(in.available()).isZero();
                return streamed;
            }
        }
    }

    private static List<PartitionName> partitions() {
        // Construct a bunch of relations, some with partitions, some with null values
        List<PartitionName> partitions = new ArrayList<>();
        // Un-partitioned index
        partitions.add(new PartitionName(RelationName.fromIndexName(randomAlphaOfLength(5)), List.of()));
        // Partitioned index with one value
        RelationName relation1 = RelationName.fromIndexName(randomAlphaOfLength(6));
        partitions.add(new PartitionName(relation1, List.of("foo")));
        // Partitioned index with multiple values
        RelationName relation2 = RelationName.fromIndexName(randomAlphaOfLength(6));
        partitions.add(new PartitionName(relation2, List.of("foo", "bar")));
        // Partitioned index with multiple values including nulls
        RelationName relation3 = RelationName.fromIndexName(randomAlphaOfLength(6));
        List<String> partitionValues = new ArrayList<>();
        partitionValues.add("baz");
        partitionValues.add(null);
        partitionValues.add("foo");
        partitions.add(new PartitionName(relation3, partitionValues));

        // Random sort
        Collections.shuffle(partitions, random());
        return partitions;
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPreV6ReadStreaming() throws Exception {
        // Check that reading from pre v6 indices-request-style streams provides PartitionNames

        BytesStreamOutput out = new BytesStreamOutput();

        // Convert to index names
        // Write to the output stream
        List<PartitionName> partitions = partitions();
        List<String> indexNames = new ArrayList<>();
        for (var p : partitions) {
            var index = IndexName.encode(p.relationName(), p.ident());
            indexNames.add(index);
        }
        out.writeString("");    // empty task id
        out.writeStringCollection(indexNames);
        IndicesOptions.STRICT_EXPAND_OPEN.writeIndicesOptions(out);

        // Read in via StreamInput
        // Check that the relations on the input BroadcastRequest are equal to the generated relations
        StreamInput si = out.bytes().streamInput();
        si.setVersion(Version.V_5_10_0);
        BroadcastRequest req = new BroadcastRequest(si);

        assertThat(req.targets()).isEqualTo(partitions.stream()
            .map(p -> new Target(p.relationName(), OID_UNASSIGNED, p.values())).toList());

    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPre60WriteStreaming() throws Exception {

        List<PartitionName> partitions = partitions();
        BroadcastRequest broadcastRequest = new BroadcastRequest(partitions.stream()
            .map(p -> new Target(p.relationName(), 1234, p.values())).toList());

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_10_0);

        broadcastRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        assertThat(in.readString()).isEqualTo("");  // empty task id
        assertThat(in.readVInt()).isEqualTo(partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            String index = in.readString();
            IndexParts ip = IndexName.decode(index);
            if (ip.isPartitioned()) {
                PartitionName partition = new PartitionName(ip.toRelationName(), ip.partitionIdent());
                assertThat(partition).isEqualTo(partitions.get(i));
            } else {
                PartitionName partition = new PartitionName(ip.toRelationName(), List.of());
                assertThat(partition).isEqualTo(partitions.get(i));
            }
        }
        assertThat(IndicesOptions.readIndicesOptions(in)).isEqualTo(IndicesOptions.LENIENT_EXPAND_OPEN);

    }

}
