/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.action.support.broadcast;

import io.crate.es.action.ActionResponse;
import io.crate.es.action.support.DefaultShardOperationFailedException;
import io.crate.es.common.ParseField;
import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.common.xcontent.ConstructingObjectParser;
import io.crate.es.rest.RestStatus;

import java.io.IOException;
import java.util.List;

import static io.crate.es.action.support.DefaultShardOperationFailedException.readShardOperationFailed;
import static io.crate.es.common.xcontent.ConstructingObjectParser.constructorArg;
import static io.crate.es.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Base class for all broadcast operation based responses.
 */
public class BroadcastResponse extends ActionResponse {

    public static final DefaultShardOperationFailedException[] EMPTY = new DefaultShardOperationFailedException[0];

    private static final ParseField _SHARDS_FIELD = new ParseField("_shards");
    private static final ParseField TOTAL_FIELD = new ParseField("total");
    private static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    private static final ParseField FAILED_FIELD = new ParseField("failed");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");

    private int totalShards;
    private int successfulShards;
    private int failedShards;
    private DefaultShardOperationFailedException[] shardFailures = EMPTY;

    protected static <T extends BroadcastResponse> void declareBroadcastFields(ConstructingObjectParser<T, Void> PARSER) {
        ConstructingObjectParser<BroadcastResponse, Void> shardsParser = new ConstructingObjectParser<>("_shards", true,
            arg -> new BroadcastResponse((int) arg[0], (int) arg[1], (int) arg[2], (List<DefaultShardOperationFailedException>) arg[3]));
        shardsParser.declareInt(constructorArg(), TOTAL_FIELD);
        shardsParser.declareInt(constructorArg(), SUCCESSFUL_FIELD);
        shardsParser.declareInt(constructorArg(), FAILED_FIELD);
        shardsParser.declareObjectArray(optionalConstructorArg(),
            (p, c) -> DefaultShardOperationFailedException.fromXContent(p), FAILURES_FIELD);
        PARSER.declareObject(constructorArg(), shardsParser, _SHARDS_FIELD);
    }

    public BroadcastResponse() {
    }

    public BroadcastResponse(int totalShards, int successfulShards, int failedShards,
                             List<DefaultShardOperationFailedException> shardFailures) {
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        if (shardFailures == null) {
            this.shardFailures = EMPTY;
        } else {
            this.shardFailures = shardFailures.toArray(new DefaultShardOperationFailedException[shardFailures.size()]);
        }
    }

    /**
     * The total shards this request ran against.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful shards this request was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed shards this request was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * The REST status that should be used for the response
     */
    public RestStatus getStatus() {
        if (failedShards > 0) {
            return shardFailures[0].status();
        } else {
            return RestStatus.OK;
        }
    }

    /**
     * The list of shard failures exception.
     */
    public DefaultShardOperationFailedException[] getShardFailures() {
        return shardFailures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        int size = in.readVInt();
        if (size > 0) {
            shardFailures = new DefaultShardOperationFailedException[size];
            for (int i = 0; i < size; i++) {
                shardFailures[i] = readShardOperationFailed(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(shardFailures.length);
        for (DefaultShardOperationFailedException exp : shardFailures) {
            exp.writeTo(out);
        }
    }
}
