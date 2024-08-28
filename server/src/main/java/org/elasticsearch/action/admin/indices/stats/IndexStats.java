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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexStats implements Iterable<IndexShardStats> {

    private final List<ShardStats> shards;

    public IndexStats(List<ShardStats> shards) {
        this.shards = shards;
    }

    public List<ShardStats> getShards() {
        return this.shards;
    }

    private Map<Integer, IndexShardStats> indexShards;

    public Map<Integer, IndexShardStats> getIndexShards() {
        if (indexShards != null) {
            return indexShards;
        }
        Map<Integer, List<ShardStats>> tmpIndexShards = new HashMap<>();
        for (ShardStats shard : shards) {
            List<ShardStats> lst = tmpIndexShards.get(shard.getShardRouting().id());
            if (lst == null) {
                lst = new ArrayList<>();
                tmpIndexShards.put(shard.getShardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = HashMap.newHashMap(tmpIndexShards.size());
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            List<ShardStats> shardStats = entry.getValue();
            indexShards.put(entry.getKey(), new IndexShardStats(shardStats));
        }
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return getIndexShards().values().iterator();
    }
}
