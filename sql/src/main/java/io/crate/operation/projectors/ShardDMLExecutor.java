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
import java.util.List;
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
    private final ClusterService clusterService;
    private final NodeJobsCounter nodeJobsCounter;
    private final AtomicInteger pendingItemsCount = new AtomicInteger(0);
    private final Consumer<Row> rowConsumer;
    private final BooleanSupplier backpressureTrigger;
    private final Function<Boolean, CompletableFuture<BitSet>> execute;

    private TReq currentRequest;
    private int numItems = -1;
    private CompletableFuture executionFuture = new CompletableFuture();

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
        this.clusterService = clusterService;
        this.nodeJobsCounter = nodeJobsCounter;
        this.responses = new BitSet();
        this.currentRequest = requestFactory.get();

        this.rowConsumer = createRowConsumer(uidExpression, itemFactory);
        this.backpressureTrigger = createBackpressureTrigger();
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

    private BooleanSupplier createBackpressureTrigger() {
        return () -> {
            final String nodeId = getLocalNodeId();
            if (isExecutionPossible(nodeId)) {
                return false;
            }
            return true;
        };
    }

    private boolean isExecutionPossible(String nodeId) {
        return nodeJobsCounter.getInProgressJobsForNode(nodeId) < MAX_NODE_CONCURRENT_OPERATIONS;
    }

    private Function<Boolean, CompletableFuture<BitSet>> createExecuteFunction(ScheduledExecutorService scheduler,
                                                                               NodeJobsCounter nodeJobsCounter,
                                                                               Supplier<TReq> requestFactory,
                                                                               BiConsumer<TReq, ActionListener<ShardResponse>> transportAction) {
        return (isLastBatch) -> {
            final String nodeId = getLocalNodeId();
            nodeJobsCounter.increment(nodeId);

            Function<ShardResponse, BitSet> processResponseFunction = response -> {
                currentRequest = requestFactory.get();
                nodeJobsCounter.decrement(nodeId);
                processShardResponse(response);
                return responses;
            };

            FutureActionListener<ShardResponse, BitSet> listener = new FutureActionListener(processResponseFunction) {
                @Override
                public void onFailure(Exception e) {
                    super.onFailure(e);
                    pendingItemsCount.decrementAndGet();
                    nodeJobsCounter.decrement(nodeId);
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
            return listener;
        };
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator batchIterator) {
        new BatchIteratorBackpressureExecutor<>(batchIterator, scheduler,
            rowConsumer, execute, backpressureTrigger, pendingItemsCount, bulkSize, BACKOFF_POLICY, executionFuture).
            consumeIteratorAndExecute();

        return executionFuture.
            thenApply(ignored -> Collections.singletonList(new Row1((long) responses.cardinality())));
    }

    @Nullable
    private String getLocalNodeId() {
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
        List<ShardResponse.Failure> failures = shardResponse.failures();
        if (shardResponse.failure() != null) {
            executionFuture.completeExceptionally(shardResponse.failure());
            return;
        }
        synchronized (responses) {
            for (int i = 0; i < itemIndices.size(); i++) {
                int location = itemIndices.get(i);
                ShardResponse.Failure failure = failures.get(i);
                if (failure == null) {
                    responses.set(location, true);
                } else {
                    responses.set(location, false);
                }
            }
        }
    }
}
