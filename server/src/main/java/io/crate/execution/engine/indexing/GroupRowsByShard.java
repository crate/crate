/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.indexing;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.IndexNotFoundException;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;

public final class GroupRowsByShard<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements BiConsumer<ShardedRequests<TReq, TItem>, Row> {

    private static final Logger LOGGER = LogManager.getLogger(GroupRowsByShard.class);

    private final RowShardResolver rowShardResolver;
    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final List<? extends CollectExpression<Row, ?>> sourceInfoExpressions;
    private final Function<String, TItem> itemFactory;
    private final Supplier<String> indexNameResolver;
    private final ClusterService clusterService;
    private final boolean autoCreateIndices;
    private final BiConsumer<ShardedRequests, String> itemFailureRecorder;
    private final Predicate<ShardedRequests> hasSourceUriFailure;
    private final Input<String> sourceUriInput;
    private final Input<Long> lineNumberInput;
    private final ToLongFunction<Row> estimateRowSize;

    public GroupRowsByShard(ClusterService clusterService,
                            RowShardResolver rowShardResolver,
                            ToLongFunction<Row> estimateRowSize,
                            Supplier<String> indexNameResolver,
                            List<? extends CollectExpression<Row, ?>> expressions,
                            Function<String, TItem> itemFactory,
                            boolean autoCreateIndices,
                            UpsertResultContext upsertContext) {
        this.estimateRowSize = estimateRowSize;
        assert expressions instanceof RandomAccess
            : "expressions should be a RandomAccess list for zero allocation iterations";

        this.clusterService = clusterService;
        this.rowShardResolver = rowShardResolver;
        this.indexNameResolver = indexNameResolver;
        this.expressions = expressions;
        this.sourceInfoExpressions = upsertContext.getSourceInfoExpressions();
        this.itemFactory = itemFactory;
        this.itemFailureRecorder = upsertContext.getItemFailureRecorder();
        this.hasSourceUriFailure = upsertContext.getHasSourceUriFailureChecker();
        this.sourceUriInput = upsertContext.getSourceUriInput();
        this.lineNumberInput = upsertContext.getLineNumberInput();
        this.autoCreateIndices = autoCreateIndices;
    }

    public GroupRowsByShard(ClusterService clusterService,
                            RowShardResolver rowShardResolver,
                            ToLongFunction<Row> estimateRowSize,
                            Supplier<String> indexNameResolver,
                            List<? extends CollectExpression<Row, ?>> expressions,
                            Function<String, TItem> itemFactory,
                            boolean autoCreateIndices) {
        this(clusterService,
             rowShardResolver,
             estimateRowSize,
             indexNameResolver,
             expressions,
             itemFactory,
             autoCreateIndices,
             UpsertResultContext.forRowCount());
    }

    @Override
    public void accept(ShardedRequests<TReq, TItem> shardedRequests, Row row) {
        for (int i = 0; i < sourceInfoExpressions.size(); i++) {
            sourceInfoExpressions.get(i).setNextRow(row);
        }
        if (hasSourceUriFailure.test(shardedRequests)) {
            // source uri failed processing (reading)
            return;
        }

        try {
            rowShardResolver.setNextRow(row);
            for (int i = 0; i < expressions.size(); i++) {
                expressions.get(i).setNextRow(row);
            }

            String id = rowShardResolver.id();
            TItem item = itemFactory.apply(id);
            String indexName = indexNameResolver.get();
            String routing = rowShardResolver.routing();
            String sourceUri = sourceUriInput.value();
            Long lineNumber = lineNumberInput.value();

            RowSourceInfo rowSourceInfo = RowSourceInfo.emptyMarkerOrNewInstance(sourceUri, lineNumber);
            ShardLocation shardLocation = getShardLocation(indexName, id, routing);
            long sizeEstimate = estimateRowSize.applyAsLong(row);
            if (shardLocation == null) {
                shardedRequests.add(item, sizeEstimate, indexName, routing, rowSourceInfo);
            } else {
                shardedRequests.add(item, sizeEstimate, shardLocation, rowSourceInfo);
            }
        } catch (CircuitBreakingException e) {
            throw e;
        } catch (Throwable t) {
            itemFailureRecorder.accept(shardedRequests, t.getMessage());
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
        Iterator<Map.Entry<String, List<ShardedRequests.ItemAndRoutingAndSourceInfo<TItem>>>> entryIt =
            requests.itemsByMissingIndex.entrySet().iterator();
        while (entryIt.hasNext()) {
            Map.Entry<String, List<ShardedRequests.ItemAndRoutingAndSourceInfo<TItem>>> e = entryIt.next();
            List<ShardedRequests.ItemAndRoutingAndSourceInfo<TItem>> items = e.getValue();
            Iterator<ShardedRequests.ItemAndRoutingAndSourceInfo<TItem>> it = items.iterator();
            while (it.hasNext()) {
                ShardedRequests.ItemAndRoutingAndSourceInfo<TItem> itemAndRoutingAndSourceInfo = it.next();
                ShardLocation shardLocation =
                    getShardLocation(e.getKey(), itemAndRoutingAndSourceInfo.item.id(), itemAndRoutingAndSourceInfo.routing);
                if (shardLocation == null) {
                    throw new IllegalStateException("shardLocation not resolvable after createIndices");
                }
                requests.add(itemAndRoutingAndSourceInfo.item, 0, shardLocation, itemAndRoutingAndSourceInfo.rowSourceInfo);
                it.remove();
            }
            if (items.isEmpty()) {
                entryIt.remove();
            }
        }
    }
}
