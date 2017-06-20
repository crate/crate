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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntArrayList;
import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardResponse;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.operation.NodeJobsCounter.MAX_NODE_CONCURRENT_OPERATIONS;

public class ShardDMLExecutor<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements Function<BatchIterator, CompletableFuture<? extends Iterable<Row>>> {

    private static final Logger LOGGER = Loggers.getLogger(ShardDMLExecutor.class);

    private static final BackoffPolicy BACKOFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);
    public static final int DEFAULT_BULK_SIZE = 10_000;

    private final int bulkSize;
    private final ScheduledExecutorService scheduler;
    private final BitSet responses;
    private final AtomicInteger pendingItemsCount = new AtomicInteger(0);
    private final Consumer<Row> rowConsumer;
    private final BooleanSupplier shouldPause;
    private final Function<Boolean, CompletableFuture<BitSet>> execute;
    private final String localNodeId;
    private final CompletableFuture<Void> executionFuture = new CompletableFuture<>();

    private TReq currentRequest;
    private int numItems = -1;

    public ShardDMLExecutor(int bulkSize,
                            ScheduledExecutorService scheduler,
                            CollectExpression<Row, ?> uidExpression,
                            ClusterService clusterService,
                            NodeJobsCounter nodeJobsCounter,
                            Supplier<TReq> requestFactory,
                            Function<String, TItem> itemFactory,
                            BiConsumer<TReq, ActionListener<ShardResponse>> transportAction) {
        this.bulkSize = bulkSize;
        this.scheduler = scheduler;
        this.responses = new BitSet();
        this.currentRequest = requestFactory.get();
        this.localNodeId = getLocalNodeId(clusterService);

        this.rowConsumer = createRowConsumer(uidExpression, itemFactory);
        this.shouldPause = () ->
            nodeJobsCounter.getInProgressJobsForNode(localNodeId) >= MAX_NODE_CONCURRENT_OPERATIONS;
        this.execute = createExecuteFunction(scheduler, nodeJobsCounter, requestFactory, transportAction);
    }

    private Consumer<Row> createRowConsumer(CollectExpression<Row, ?> uidExpression,
                                            Function<String, TItem> itemFactory) {
        return (row) -> {
            numItems++;
            uidExpression.setNextRow(row);
            currentRequest.add(numItems, itemFactory.apply(((BytesRef) uidExpression.value()).utf8ToString()));
        };
    }

    private Function<Boolean, CompletableFuture<BitSet>> createExecuteFunction(ScheduledExecutorService scheduler,
                                                                               NodeJobsCounter nodeJobsCounter,
                                                                               Supplier<TReq> requestFactory,
                                                                               BiConsumer<TReq, ActionListener<ShardResponse>> transportAction) {
        return (isLastBatch) -> {
            nodeJobsCounter.increment(localNodeId);

            Function<ShardResponse, BitSet> transformResponseFunction = response -> {
                nodeJobsCounter.decrement(localNodeId);
                processShardResponse(response);
                return responses;
            };

            FutureActionListener<ShardResponse, BitSet> listener =
                new FutureActionListener<ShardResponse, BitSet>(transformResponseFunction) {

                    // Some operations fail instantly (eg writing on a table/partition that has `blocks.write=true`)
                    @Override
                    public void onFailure(Exception e) {
                        super.onFailure(e);
                        nodeJobsCounter.decrement(localNodeId);
                        executionFuture.completeExceptionally(e);
                    }
                };

            transportAction.accept(
                currentRequest,
                new RetryListener<>(scheduler,
                    (actionListener) -> transportAction.accept(currentRequest, actionListener),
                    listener,
                    BACKOFF_POLICY
                )
            );
            // The current bulk was submitted to the transport action for execution, so we'll continue to collect and
            // accumulate data for the next bulk in a new request object
            currentRequest = requestFactory.get();
            return listener;
        };
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator batchIterator) {
        new BatchIteratorBackpressureExecutor<>(batchIterator, scheduler,
            rowConsumer, execute, shouldPause, pendingItemsCount, bulkSize, BACKOFF_POLICY, executionFuture).
            consumeIteratorAndExecute();

        return executionFuture.
            thenApply(ignored -> Collections.singletonList(new Row1((long) responses.cardinality())));
    }

    @Nullable
    private String getLocalNodeId(ClusterService clusterService) {
        String nodeId = null;
        try {
            nodeId = clusterService.localNode().getId();
        } catch (IllegalStateException e) {
            LOGGER.debug("Unable to get local node id", e);
        }
        return nodeId;
    }

    private void processShardResponse(ShardResponse shardResponse) {
        IntArrayList itemIndices = shardResponse.itemIndices();
        pendingItemsCount.addAndGet(-itemIndices.size());
        if (shardResponse.failure() != null) {
            executionFuture.completeExceptionally(shardResponse.failure());
            return;
        }
        synchronized (responses) {
            ShardResponse.markResponseItemsAndFailures(shardResponse, responses);
        }
    }
}
