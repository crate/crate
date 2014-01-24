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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.export;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 * The response of the count action.
 */
public class ExportResponse extends BroadcastOperationResponse implements ToXContent {


    private List<ShardExportResponse> responses;
    private long totalExported;

    public ExportResponse(List<ShardExportResponse> responses, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        //To change body of created methods use File | Settings | File Templates.
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.responses = responses;
        for (ShardExportResponse r : this.responses) {
            totalExported += r.getNumExported();
        }
    }

    public ExportResponse() {

    }


    public long getTotalExported() {
        return totalExported;
    }


    public List<ShardExportResponse> getResponses() {
        return responses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        totalExported = in.readVLong();
        int numResponses = in.readVInt();
        responses = new ArrayList<ShardExportResponse>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            responses.add(ShardExportResponse.readNew(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(totalExported);
        out.writeVInt(responses.size());
        for (ShardExportResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("exports");
        for (ShardExportResponse r : this.responses) {
            r.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("totalExported", totalExported);
        buildBroadcastShardsHeader(builder, this);
        builder.endObject();
        return builder;
    }
}
