/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.import_;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ImportResponse extends NodesOperationResponse<NodeImportResponse> implements ToXContent {

    private List<NodeImportResponse> responses;
    private List<FailedNodeException> nodeFailures;

    public ImportResponse() {
    }

    public ImportResponse(List<NodeImportResponse> responses, int total,
            int successfulNodes, int failedNodes, List<FailedNodeException> nodeFailures) {
        this.responses = responses;
        this.nodeFailures = nodeFailures;
    }

    public List<NodeImportResponse> getResponses() {
        return responses;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        builder.startArray("imports");
        for (NodeImportResponse r : this.responses) {
            r.toXContent(builder, params);
        }
        builder.endArray();
        if (nodeFailures != null && nodeFailures.size() > 0) {
            builder.startArray("failures");
            for (FailedNodeException failure : nodeFailures) {
                builder.startObject();
                builder.field("node_id", failure.nodeId());
                builder.field("reason", failure.getDetailedMessage());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int responsesCount = in.readInt();
        this.responses = new ArrayList<NodeImportResponse>(responsesCount);
        for (int i = 0; i < responsesCount; i++) {
            responses.add(NodeImportResponse.readNew(in));
        }
        int failuresCount = in.readInt();
        this.nodeFailures = new ArrayList<FailedNodeException>(failuresCount);
        for (int i = 0; i < failuresCount; i++) {
            String nodeId = in.readString();
            String msg = in.readOptionalString();
            FailedNodeException e = new FailedNodeException(nodeId, msg, null);
            nodeFailures.add(e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(responses.size());
        for (NodeImportResponse response : responses) {
            response.writeTo(out);
        }
        out.writeInt(nodeFailures.size());
        for (FailedNodeException e : nodeFailures) {
            out.writeString(e.nodeId());
            out.writeOptionalString(e.getMessage());
        }
    }
}