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

package org.cratedb.action.searchinto;


import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.action.support.RestActions
        .buildBroadcastShardsHeader;

public class SearchIntoResponse extends BroadcastOperationResponse
        implements ToXContent {


    private List<ShardSearchIntoResponse> responses;

    private long totalWrites;
    private long failedWrites;
    private long succeededWrites;

    public SearchIntoResponse(List<ShardSearchIntoResponse> responses,
            int totalShards, int successfulShards, int failedShards,
            List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.responses = responses;
        for (ShardSearchIntoResponse r : this.responses) {
            totalWrites += r.getTotalWrites();
            succeededWrites += r.getSucceededWrites();
            failedWrites += r.getFailedWrites();
        }
    }

    public SearchIntoResponse() {

    }

    public long getTotalWrites() {
        return totalWrites;
    }


    public List<ShardSearchIntoResponse> getResponses() {
        return responses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        totalWrites = in.readVLong();
        succeededWrites = in.readVLong();
        failedWrites = in.readVLong();
        int numResponses = in.readVInt();
        responses = new ArrayList<ShardSearchIntoResponse>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            responses.add(ShardSearchIntoResponse.readNew(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(totalWrites);
        out.writeVLong(succeededWrites);
        out.writeVLong(failedWrites);
        out.writeVInt(responses.size());
        for (ShardSearchIntoResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder,
            Params params) throws IOException {
        builder.startObject();
        builder.startArray("writes");
        for (ShardSearchIntoResponse r : this.responses) {
            r.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("total", totalWrites);
        builder.field("succeeded", succeededWrites);
        builder.field("failed", failedWrites);
        buildBroadcastShardsHeader(builder, this);
        builder.endObject();
        return builder;
    }
}
