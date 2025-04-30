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

package org.elasticsearch.action.admin.indices.delete;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.PartitionName;

/**
 * A request to delete an index.
 * This is only here to support backwards-compatibility, and is not used by 6.0 nodes
 */
public class DeleteIndexRequest extends AcknowledgedRequest<DeleteIndexRequest> {

    private final List<PartitionName> partitions;

    public List<PartitionName> partitions() {
        return partitions;
    }

    public DeleteIndexRequest(StreamInput in) throws IOException {
        super(in);
        partitions = BroadcastRequest.readPartitionNamesFromPre60(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        BroadcastRequest.writePartitionNamesToPre60(out, partitions);
    }
}
