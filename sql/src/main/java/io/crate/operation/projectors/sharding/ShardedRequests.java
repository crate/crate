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

package io.crate.operation.projectors.sharding;

import io.crate.executor.transport.ShardRequest;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class ShardedRequests<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item> {

    final Map<String, List<ItemAndRouting<TItem>>> itemsByMissingIndex = new HashMap<>();
    final Map<ShardLocation, TReq> itemsByShard = new HashMap<>();

    private final BiFunction<ShardId, String, TReq> requestFactory;

    private int location = -1;

    /**
     * @param requestFactory function to create a request, will receive the indexName and routing
     */
    public ShardedRequests(BiFunction<ShardId, String, TReq> requestFactory) {
        this.requestFactory = requestFactory;
    }

    public void add(TItem item, String indexName, String routing) {
        List<ItemAndRouting<TItem>> items = itemsByMissingIndex.computeIfAbsent(indexName, k -> new ArrayList<>());
        items.add(new ItemAndRouting<>(item, routing));
    }

    public void add(TItem item, ShardLocation shardLocation, String routing) {
        TReq req = itemsByShard.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId, routing);
            itemsByShard.put(shardLocation, req);
        }
        location++;
        req.add(location, item);
    }

    static class ItemAndRouting<TItem> {
        final TItem item;
        final String routing;

        ItemAndRouting(TItem item, String routing) {
            this.item = item;
            this.routing = routing;
        }
    }
}
