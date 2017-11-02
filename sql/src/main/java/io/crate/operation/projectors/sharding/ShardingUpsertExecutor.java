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

import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardResponse;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import io.crate.operation.projectors.BatchIteratorBackpressureExecutor;
import io.crate.operation.projectors.RetryListener;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesRequest;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.operation.NodeJobsCounter.MAX_NODE_CONCURRENT_OPERATIONS;

public class ShardingUpsertExecutor<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements Function<BatchIterator<Row>, CompletableFuture<? extends Iterable<Row>>> {

    public static final CrateSetting<TimeValue> BULK_REQUEST_TIMEOUT_SETTING = CrateSetting.of(Setting.positiveTimeSetting(
        "bulk.request_timeout", new TimeValue(1, TimeUnit.MINUTES),
        Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING);

    private static final BackoffPolicy BACKOFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);
    private static final Logger LOGGER = Loggers.getLogger(ShardingUpsertExecutor.class);

    private final GroupRowsByShard<TReq, TItem> grouper;
    private final NodeJobsCounter nodeJobsCounter;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final int bulkSize;
    private final UUID jobId;
    private final BiFunction<ShardId, String, TReq> requestFactory;
    private final BulkRequestExecutor<TReq> requestExecutor;
    private final TransportBulkCreateIndicesAction createIndicesAction;

    public ShardingUpsertExecutor(ClusterService clusterService,
                                  NodeJobsCounter nodeJobsCounter,
                                  ScheduledExecutorService scheduler,
                                  Executor executor,
                                  int bulkSize,
                                  UUID jobId,
                                  RowShardResolver rowShardResolver,
                                  Function<String, TItem> itemFactory,
                                  BiFunction<ShardId, String, TReq> requestFactory,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  Supplier<String> indexNameResolver,
                                  boolean autoCreateIndices,
                                  BulkRequestExecutor<TReq> requestExecutor,
                                  TransportBulkCreateIndicesAction createIndicesAction) {
        this.nodeJobsCounter = nodeJobsCounter;
        this.scheduler = scheduler;
        this.executor = executor;
        this.bulkSize = bulkSize;
        this.jobId = jobId;
        this.requestFactory = requestFactory;
        this.requestExecutor = requestExecutor;
        this.createIndicesAction = createIndicesAction;
        this.grouper = new GroupRowsByShard<>(
            clusterService,
            rowShardResolver,
            indexNameResolver,
            expressions,
            itemFactory,
            autoCreateIndices
        );
    }

    public CompletableFuture<Long> execute(ShardedRequests<TReq, TItem> requests) {
        if (requests.itemsByMissingIndex.isEmpty()) {
            return execRequests(requests.itemsByShard);
        }
        return createIndices(requests.itemsByMissingIndex)
            .thenCompose(resp -> {
                if (grouper.reResolveShardLocations(requests)) {
                    return execRequests(requests.itemsByShard);
                }
                return execute(requests);
            });
    }

    private CompletableFuture<Long> execRequests(Map<ShardLocation, TReq> itemsByShard) {
        final AtomicInteger numRequests = new AtomicInteger(itemsByShard.size());
        final AtomicLong rowCount = new AtomicLong(0L);
        final CompletableFuture<Long> rowCountFuture = new CompletableFuture<>();
        Iterator<Map.Entry<ShardLocation, TReq>> it = itemsByShard.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ShardLocation, TReq> entry = it.next();
            TReq request = entry.getValue();
            it.remove();

            String nodeId = entry.getKey().nodeId;
            nodeJobsCounter.increment(nodeId);
            ActionListener<ShardResponse> listener =
                new ShardResponseActionListener(nodeId, rowCount, numRequests, rowCountFuture);

            listener = new RetryListener<>(
                scheduler,
                l -> {
                    LOGGER.debug("Executing retry Listener for nodeId: {} request: {}", nodeId, request);
                    requestExecutor.execute(request, l);
                },
                listener,
                BACKOFF_POLICY
            );
            requestExecutor.execute(request, listener);
        }
        return rowCountFuture;
    }


    private CompletableFuture<BulkCreateIndicesResponse> createIndices(Map<String, List<ShardedRequests.ItemAndRouting<TItem>>> itemsByMissingIndex) {
        FutureActionListener<BulkCreateIndicesResponse, BulkCreateIndicesResponse> listener = FutureActionListener.newInstance();
        createIndicesAction.execute(
            new BulkCreateIndicesRequest(itemsByMissingIndex.keySet(), jobId), listener);
        return listener;
    }

    private boolean shouldPause(ShardedRequests<TReq, TItem> requests) {
        for (ShardLocation shardLocation : requests.itemsByShard.keySet()) {
            String requestNodeId = shardLocation.nodeId;
            if (nodeJobsCounter.getInProgressJobsForNode(requestNodeId) >= MAX_NODE_CONCURRENT_OPERATIONS) {
                LOGGER.debug("reached maximum concurrent operations for node {}", requestNodeId);
                return true;
            }
        }
        return false;
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator<Row> batchIterator) {
        BatchIterator<ShardedRequests<TReq, TItem>> reqBatchIterator =
            BatchIterators.partition(batchIterator, bulkSize, () -> new ShardedRequests<>(requestFactory), grouper);

        BatchIteratorBackpressureExecutor<ShardedRequests<TReq, TItem>, Long> executor = new BatchIteratorBackpressureExecutor<>(
            scheduler,
            this.executor,
            reqBatchIterator,
            this::execute,
            (a, b) -> a + b,
            0L,
            this::shouldPause,
            BACKOFF_POLICY
        );
        return executor.consumeIteratorAndExecute()
            .thenApply(rowCount -> Collections.singletonList(new Row1(rowCount)));
    }

    private class ShardResponseActionListener implements ActionListener<ShardResponse> {
        private final String operationNodeId;
        private final AtomicLong rowCount;
        private final AtomicInteger numRequests;
        private final CompletableFuture<Long> rowCountFuture;
        InterruptedException interruptedException;

        ShardResponseActionListener(String operationNodeId,
                                    AtomicLong rowCount,
                                    AtomicInteger numRequests,
                                    CompletableFuture<Long> rowCountFuture) {
            this.operationNodeId = operationNodeId;
            this.rowCount = rowCount;
            this.numRequests = numRequests;
            this.rowCountFuture = rowCountFuture;
            interruptedException = null;
        }

        @Override
        public void onResponse(ShardResponse shardResponse) {
            nodeJobsCounter.decrement(operationNodeId);
            rowCount.addAndGet(shardResponse.successRowCount());
            maybeSetInterrupt(shardResponse.failure());
            countdown();
        }

        @Override
        public void onFailure(Exception e) {
            nodeJobsCounter.decrement(operationNodeId);
            countdown();
        }

        private void countdown() {
            if (numRequests.decrementAndGet() == 0) {
                if (interruptedException == null) {
                    rowCountFuture.complete(rowCount.get());
                } else {
                    rowCountFuture.completeExceptionally(interruptedException);
                }
            }
        }

        private void maybeSetInterrupt(@Nullable Exception failure) {
            if (failure instanceof InterruptedException) {
                interruptedException = (InterruptedException) failure;
            }
        }
    }
}
