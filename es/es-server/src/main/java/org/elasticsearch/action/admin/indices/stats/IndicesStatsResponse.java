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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndicesStatsResponse extends BroadcastResponse {

    private final ShardStats[] shards;

    IndicesStatsResponse(ShardStats[] shards, int totalShards, int successfulShards, int failedShards,
                         List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    public IndexStats getIndex(String index) {
        return getIndices().get(index);
    }

    private Map<String, IndexStats> indicesStats;

    public Map<String, IndexStats> getIndices() {
        if (indicesStats != null) {
            return indicesStats;
        }
        Map<String, IndexStats> indicesStats = new HashMap<>();

        Set<Index> indices = new HashSet<>();
        for (ShardStats shard : shards) {
            indices.add(shard.getShardRouting().index());
        }

        for (Index index : indices) {
            List<ShardStats> shards = new ArrayList<>();
            String indexName = index.getName();
            for (ShardStats shard : this.shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    shards.add(shard);
                }
            }
            indicesStats.put(
                indexName, new IndexStats(indexName, index.getUUID(), shards.toArray(new ShardStats[0]))
            );
        }
        this.indicesStats = indicesStats;
        return indicesStats;
    }

    public IndicesStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardStats::new, ShardStats[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }
}
