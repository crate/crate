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

package org.elasticsearch.action.admin.indices.shrink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


/**
 * Request resize of a table or partition
 */
public class ResizeRequest extends AcknowledgedRequest<ResizeRequest> {

    private final RelationName table;
    private final List<String> partitionValues;
    private final int newNumShards;

    public ResizeRequest(RelationName table, List<String> partitionValues, int newNumShards) {
        this.table = table;
        this.partitionValues = partitionValues;
        this.newNumShards = newNumShards;
    }

    public ResizeRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_5_10_0)) {
            table = new RelationName(in);
            int numValues = in.readVInt();
            partitionValues = new ArrayList<>(numValues);
            for (int i = 0; i < numValues; i++) {
                partitionValues.add(in.readOptionalString());
            }
            newNumShards = in.readVInt();
        } else {
            CreateIndexRequest targetIndexRequest = new CreateIndexRequest(in);
            String sourceIndex = in.readString();
            in.readEnum(ResizeType.class);
            in.readOptionalBoolean();

            IndexParts indexParts = IndexName.decode(sourceIndex);
            table = indexParts.toRelationName();
            if (indexParts.isPartitioned()) {
                partitionValues = PartitionName.decodeIdent(indexParts.partitionIdent());
            } else {
                partitionValues = List.of();
            }
            newNumShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexRequest.settings());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_10_0)) {
            table.writeTo(out);
            out.writeVInt(partitionValues.size());
            for (String value : partitionValues) {
                out.writeOptionalString(value);
            }
            out.writeVInt(newNumShards);
        } else {
            // Without knowing the current number of shards it's not possible to infer if
            // the request should use SPLIT or SHRINK
            // 5.10 nodes can handle resize sent from older nodes but not the other way around
            throw new UnsupportedOperationException("Cannot resize tables if older nodes are in the cluster");
        }
    }

    public RelationName table() {
        return table;
    }

    public List<String> partitionValues() {
        return partitionValues;
    }

    public int newNumShards() {
        return newNumShards;
    }
}
