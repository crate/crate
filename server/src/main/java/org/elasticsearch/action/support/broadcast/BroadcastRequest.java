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

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class BroadcastRequest extends TransportRequest {

    /**
     * Carries the table identity alongside its name. Each request type determines
     * whether to resolve by name or OID; pre-6.5 peers only receive the name.
     * Empty partition values select all partitions of the table.
     */
    public record Target(RelationName relationName, int tableOid, List<String> partitionValues) implements Writeable {

        public Target(StreamInput in) throws IOException {
            this(new RelationName(in), in.readVInt(), in.readList(StreamInput::readOptionalString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            relationName.writeTo(out);
            out.writeVInt(tableOid);
            out.writeCollection(partitionValues, StreamOutput::writeOptionalString);
        }
    }

    private final List<Target> targets;

    protected BroadcastRequest(List<Target> targets) {
        this.targets = List.copyOf(targets);
    }

    public BroadcastRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            targets = in.readList(Target::new);
        } else {
            List<PartitionName> partitions = in.getVersion().onOrAfter(Version.V_6_0_0)
                ? in.readList(PartitionName::new)
                : readPartitionNamesFromPre60(in);
            targets = partitions.stream()
                .map(p -> new Target(p.relationName(), OID_UNASSIGNED, p.values()))
                .toList();
        }
    }

    public final List<Target> targets() {
        return targets;
    }

    /** Adapt targets for existing name-based index lookups and older peers. */
    public final List<PartitionName> partitions() {
        return targets.stream()
            .map(t -> new PartitionName(t.relationName(), t.partitionValues()))
            .toList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeCollection(targets);
        } else {
            List<PartitionName> partitions = partitions();
            if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
                out.writeCollection(partitions);
            } else {
                writePartitionNamesToPre60(out, partitions);
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static List<PartitionName> readPartitionNamesFromPre60(StreamInput in) throws IOException {
        String[] indexes = in.readStringArray();
        List<PartitionName> partitions = new ArrayList<>(indexes.length);
        IndicesOptions.readIndicesOptions(in);
        for (String index : indexes) {
            IndexParts indexParts = IndexName.decode(index);
            partitions.add(indexParts.toPartitionName());
        }
        return partitions;
    }

    @SuppressWarnings("deprecation")
    public static void writePartitionNamesToPre60(StreamOutput out, List<PartitionName> partitions) throws IOException {
        List<String> indexes = new ArrayList<>();
        for (var partition : partitions) {
            if (partition.values().isEmpty()) {
                indexes.add(partition.relationName().name());
            } else {
                indexes.add(IndexName.encode(partition.relationName(), partition.ident()));
            }
        }
        out.writeStringCollection(indexes);
        IndicesOptions.LENIENT_EXPAND_OPEN.writeIndicesOptions(out);
    }
}
