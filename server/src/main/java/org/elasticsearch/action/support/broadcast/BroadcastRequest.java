/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;

public class BroadcastRequest extends TransportRequest {

    protected final List<PartitionName> partitions;

    protected BroadcastRequest(List<PartitionName> partitions) {
        this.partitions = partitions;
    }

    protected BroadcastRequest(PartitionName partition) {
        this(List.of(partition));
    }

    public BroadcastRequest(StreamInput in) throws IOException {
        super(in);
        this.partitions = readPartitions(in);
    }

    public final List<PartitionName> partitions() {
        return partitions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writePartitions(out, partitions);
    }

    private static List<PartitionName> readPartitions(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
            return in.readList(PartitionName::new);
        } else {
            return readFromPre60(in);
        }
    }

    private static List<PartitionName> readFromPre60(StreamInput in) throws IOException {
        String[] indexes = in.readStringArray();
        List<PartitionName> partitions = new ArrayList<>(indexes.length);
        IndicesOptions.readIndicesOptions(in);
        for (String index : indexes) {
            IndexParts indexParts = IndexName.decode(index);
            if (indexParts.isPartitioned()) {
                partitions.add(new PartitionName(indexParts.toRelationName(), indexParts.partitionIdent()));
            } else {
                partitions.add(new PartitionName(indexParts.toRelationName(), List.of()));
            }
        }
        return partitions;
    }

    private static void writePartitions(StreamOutput out, List<PartitionName> partitions) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
            out.writeCollection(partitions);
        } else {
            out.writeStringCollection(bwcIndicesNames(partitions));
            IndicesOptions.LENIENT_EXPAND_OPEN.writeIndicesOptions(out);
        }
    }

    private static List<String> bwcIndicesNames(List<PartitionName> partitions) {
        List<String> output = new ArrayList<>();
        for (var partition : partitions) {
            if (partition.values().isEmpty()) {
                output.add(partition.relationName().name());
            } else {
                output.add(IndexName.encode(partition.relationName(), partition.ident()));
            }
        }
        return output;
    }
}
