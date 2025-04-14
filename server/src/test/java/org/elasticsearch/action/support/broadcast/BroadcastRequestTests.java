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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class BroadcastRequestTests extends ESTestCase {

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

        assertThat(req.partitions).isEqualTo(partitions);

    }

    @Test
    public void testPre60WriteStreaming() throws Exception {

        List<PartitionName> partitions = partitions();
        BroadcastRequest broadcastRequest = new BroadcastRequest(partitions);

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
