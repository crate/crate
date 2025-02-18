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
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;


/**
 * Request resize of a table or partition
 * Uses double of the default master node timeout and ack timeout.
 * default 30 seconds might be not enough and resizing operation fails.
 */
public class ResizeRequest extends AcknowledgedRequest<ResizeRequest> {

    private final RelationName table;
    private final List<String> partitionValues;
    private final int newNumShards;

    public ResizeRequest(RelationName table, List<String> partitionValues, int newNumShards) {
        super();
        this.table = table;
        this.partitionValues = partitionValues;
        this.newNumShards = newNumShards;
        this.timeout = TimeValue.timeValueSeconds(this.timeout.seconds() * 2);
        this.masterNodeTimeout = TimeValue.timeValueSeconds(this.masterNodeTimeout.seconds() * 2);
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
            throw new UnsupportedOperationException(
                "Cannot stream ResizeRequest in mixed 6.0.0/<5.10 clusters. All nodes need to be >= 5.10");
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
            throw new UnsupportedOperationException(
                "Cannot stream ResizeRequest in mixed 6.0.0/<5.10 clusters. All nodes need to be >= 5.10");
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
