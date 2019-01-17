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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Arrays;
import java.util.List;

/**
 * A response to flush action.
 */
public class FlushResponse extends BroadcastResponse {

    private static final ConstructingObjectParser<FlushResponse, Void> PARSER = new ConstructingObjectParser<>("flush", true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return new FlushResponse(response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(),
                    Arrays.asList(response.getShardFailures()));
        });

    static {
        declareBroadcastFields(PARSER);
    }

    FlushResponse() {

    }

    FlushResponse(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    public static FlushResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
