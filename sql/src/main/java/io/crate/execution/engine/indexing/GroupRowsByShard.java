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

package io.crate.execution.engine.indexing;

import io.crate.data.Row;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class GroupRowsByShard<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements BiConsumer<ShardedRequests<TReq, TItem>, Row> {

    private static final Logger LOGGER = Loggers.getLogger(GroupRowsByShard.class);

    private final RowShardResolver rowShardResolver;
    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final Function<String, TItem> itemFactory;
    private final Supplier<String> indexNameResolver;
    private final ClusterService clusterService;
    private final boolean autoCreateIndices;

    GroupRowsByShard(ClusterService clusterService,
                     RowShardResolver rowShardResolver,
                     Supplier<String> indexNameResolver,
                     List<? extends CollectExpression<Row, ?>> expressions,
                     Function<String, TItem> itemFactory,
                     boolean autoCreateIndices) {
        assert expressions instanceof RandomAccess
            : "expressions should be a RandomAccess list for zero allocation iterations";

        this.clusterService = clusterService;
        this.rowShardResolver = rowShardResolver;
        this.indexNameResolver = indexNameResolver;
        this.expressions = expressions;
        this.itemFactory = itemFactory;
        this.autoCreateIndices = autoCreateIndices;
    }

    @Override
    public void accept(ShardedRequests<TReq, TItem> shardedRequests, Row row) {
        rowShardResolver.setNextRow(row);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        String id = rowShardResolver.id();
        TItem item = itemFactory.apply(id);
        String indexName = indexNameResolver.get();
        String routing = rowShardResolver.routing();
        ShardLocation shardLocation = getShardLocation(indexName, id, routing);
        if (shardLocation == null) {
            shardedRequests.add(item, indexName, routing);
        } else {
            shardedRequests.add(item, shardLocation);
        }
    }

    @Nullable
    private ShardLocation getShardLocation(String indexName, String id, @Nullable String routing) {
        try {
            ShardIterator shardIterator = clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                id,
                routing
            );

            final String nodeId;
            ShardRouting shardRouting = shardIterator.nextOrNull();
            if (shardRouting == null) {
                nodeId = null;
            } else if (shardRouting.active() == false) {
                nodeId = shardRouting.relocatingNodeId();
            } else {
                nodeId = shardRouting.currentNodeId();
            }
            if (nodeId == null && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unable to get the node id for index {} and shard {}", indexName, id);
            }
            return new ShardLocation(shardIterator.shardId(), nodeId);
        } catch (IndexNotFoundException e) {
            if (!autoCreateIndices) {
                throw e;
            }
            return null;
        }
    }

    /**
     * @throws IllegalStateException if a shardLocation still can't be resolved
     */
    void reResolveShardLocations(ShardedRequests<TReq, TItem> requests) {
        Iterator<Map.Entry<String, List<ShardedRequests.ItemAndRouting<TItem>>>> entryIt =
            requests.itemsByMissingIndex.entrySet().iterator();
        while (entryIt.hasNext()) {
            Map.Entry<String, List<ShardedRequests.ItemAndRouting<TItem>>> e = entryIt.next();
            List<ShardedRequests.ItemAndRouting<TItem>> items = e.getValue();
            Iterator<ShardedRequests.ItemAndRouting<TItem>> it = items.iterator();
            while (it.hasNext()) {
                ShardedRequests.ItemAndRouting<TItem> itemAndRouting = it.next();
                ShardLocation shardLocation =
                    getShardLocation(e.getKey(), itemAndRouting.item.id(), itemAndRouting.routing);
                if (shardLocation == null) {
                    throw new IllegalStateException("shardLocation not resolvable after createIndices");
                }
                requests.add(itemAndRouting.item, shardLocation);
                it.remove();
            }
            if (items.isEmpty()) {
                entryIt.remove();
            }
        }
    }
}
