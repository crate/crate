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

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpgradeStatusResponse extends BroadcastResponse {
    private ShardUpgradeStatus[] shards;

    private Map<String, IndexUpgradeStatus> indicesUpgradeStatus;

    UpgradeStatusResponse() {
    }

    UpgradeStatusResponse(ShardUpgradeStatus[] shards, int totalShards, int successfulShards, int failedShards,
                          List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexUpgradeStatus> getIndices() {
        if (indicesUpgradeStatus != null) {
            return indicesUpgradeStatus;
        }
        Map<String, IndexUpgradeStatus> indicesUpgradeStats = new HashMap<>();

        Set<String> indices = new HashSet<>();
        for (ShardUpgradeStatus shard : shards) {
            indices.add(shard.getIndex());
        }

        for (String indexName : indices) {
            List<ShardUpgradeStatus> shards = new ArrayList<>();
            for (ShardUpgradeStatus shard : this.shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    shards.add(shard);
                }
            }
            indicesUpgradeStats.put(indexName, new IndexUpgradeStatus(indexName, shards.toArray(new ShardUpgradeStatus[shards.size()])));
        }
        this.indicesUpgradeStatus = indicesUpgradeStats;
        return indicesUpgradeStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = new ShardUpgradeStatus[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = ShardUpgradeStatus.readShardUpgradeStatus(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shards.length);
        for (ShardUpgradeStatus shard : shards) {
            shard.writeTo(out);
        }
    }

    public long getTotalBytes() {
        long totalBytes = 0;
        for (IndexUpgradeStatus indexShardUpgradeStatus : getIndices().values()) {
            totalBytes += indexShardUpgradeStatus.getTotalBytes();
        }
        return totalBytes;
    }
}
