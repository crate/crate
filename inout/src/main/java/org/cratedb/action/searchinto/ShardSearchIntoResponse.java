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

package org.cratedb.action.searchinto;

import org.cratedb.searchinto.WriterResult;
import org.elasticsearch.action.support.broadcast
        .BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Internal searchinto response of a shard searchinto request executed
 * directly  against a specific shard.
 */
class ShardSearchIntoResponse extends BroadcastShardOperationResponse
        implements ToXContent {

    private WriterResult result;
    private List<String> cmdArray;
    private boolean dryRun = false;
    private Text node;

    private long totalWrites;
    private long failedWrites;
    private long succeededWrites;


    ShardSearchIntoResponse() {
    }


    public ShardSearchIntoResponse(Text node, String index, int shardId,
            WriterResult result) {
        super(index, shardId);
        this.node = node;
        this.result = result;
    }

    /**
     * Constructor for dry runs. Does not contain any execution infos
     */
    public ShardSearchIntoResponse(Text node, String index, int shardId) {
        this.node = node;
        this.dryRun = true;
    }

    public long getTotalWrites() {
        return totalWrites;
    }

    long getFailedWrites() {
        return failedWrites;
    }

    long getSucceededWrites() {
        return succeededWrites;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public Text getNode() {
        return node;
    }

    public static ShardSearchIntoResponse readNew(StreamInput in) throws
            IOException {
        ShardSearchIntoResponse response = new ShardSearchIntoResponse();
        response.readFrom(in);
        return response;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        node = in.readOptionalText();
        dryRun = in.readBoolean();
        if (in.readBoolean()) {
            result = WriterResult.readNew(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalText(node);
        out.writeBoolean(dryRun);
        if (result != null) {
            out.writeBoolean(true);
            result.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder,
            Params params) throws IOException {
        builder.startObject();
        builder.field("index", getIndex());
        builder.field("shard", getShardId());
        if (node != null) {
            builder.field("node", node);
        }
        result.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
