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

import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.breaker.TypeGuessEstimateRowSize;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.execution.support.RetryListener;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static io.crate.execution.jobs.NodeJobsCounter.MAX_NODE_CONCURRENT_OPERATIONS;

public class ShardingUpsertExecutor
    implements Function<BatchIterator<Row>, CompletableFuture<? extends Iterable<? extends Row>>> {

    public static final CrateSetting<TimeValue> BULK_REQUEST_TIMEOUT_SETTING = CrateSetting.of(Setting.positiveTimeSetting(
        "bulk.request_timeout", new TimeValue(1, TimeUnit.MINUTES),
        Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING);

    private static final BackoffPolicy BACKOFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);
    private static final Logger LOGGER = LogManager.getLogger(ShardingUpsertExecutor.class);

    private final GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper;
    private final NodeJobsCounter nodeJobsCounter;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final int bulkSize;
    private final UUID jobId;
    private final Function<ShardId, ShardUpsertRequest> requestFactory;
    private final BulkRequestExecutor<ShardUpsertRequest> requestExecutor;
    private final TransportCreatePartitionsAction createPartitionsAction;
    private final BulkShardCreationLimiter bulkShardCreationLimiter;
    private final IsUsedBytesOverThreshold isUsedBytesOverThreshold;
    private final UpsertResultCollector resultCollector;
    private final boolean isDebugEnabled;
    private volatile boolean createPartitionsRequestOngoing = false;

    ShardingUpsertExecutor(ClusterService clusterService,
                           NodeJobsCounter nodeJobsCounter,
                           ScheduledExecutorService scheduler,
                           Executor executor,
                           int bulkSize,
                           UUID jobId,
                           RowShardResolver rowShardResolver,
                           Function<String, ShardUpsertRequest.Item> itemFactory,
                           Function<ShardId, ShardUpsertRequest> requestFactory,
                           List<? extends CollectExpression<Row, ?>> expressions,
                           Supplier<String> indexNameResolver,
                           boolean autoCreateIndices,
                           BulkRequestExecutor<ShardUpsertRequest> requestExecutor,
                           TransportCreatePartitionsAction createPartitionsAction,
                           int targetTableNumShards,
                           int targetTableNumReplicas,
                           UpsertResultContext upsertResultContext) {
        this.nodeJobsCounter = nodeJobsCounter;
        this.scheduler = scheduler;
        this.executor = executor;
        this.bulkSize = bulkSize;
        this.jobId = jobId;
        this.requestFactory = requestFactory;
        this.requestExecutor = requestExecutor;
        this.createPartitionsAction = createPartitionsAction;
        ToLongFunction<Row> estimateRowSize = new TypeGuessEstimateRowSize();
        this.grouper = new GroupRowsByShard<>(
            clusterService,
            rowShardResolver,
            estimateRowSize,
            indexNameResolver,
            expressions,
            upsertResultContext.getSourceInfoExpressions(),
            itemFactory,
            upsertResultContext.getItemFailureRecorder(),
            upsertResultContext.getHasSourceUriFailureChecker(),
            upsertResultContext.getSourceUriInput(),
            upsertResultContext.getLineNumberInput(),
            autoCreateIndices
        );
        bulkShardCreationLimiter = new BulkShardCreationLimiter(
            targetTableNumShards,
            targetTableNumReplicas,
            clusterService.state().nodes().getDataNodes().size());
        isUsedBytesOverThreshold = new IsUsedBytesOverThreshold();
        this.resultCollector = upsertResultContext.getResultCollector();
        isDebugEnabled = LOGGER.isDebugEnabled();
    }

    public CompletableFuture<UpsertResults> execute(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests) {
        final UpsertResults upsertResults = resultCollector.supplier().get();
        collectFailingSourceUris(requests, upsertResults);
        collectFailingItems(requests, upsertResults);

        if (requests.itemsByMissingIndex.isEmpty()) {
            return execRequests(requests.itemsByShard, requests.rowSourceInfos, upsertResults);
        }
        createPartitionsRequestOngoing = true;
        return createPartitions(requests.itemsByMissingIndex)
            .thenCompose(resp -> {
                grouper.reResolveShardLocations(requests);
                createPartitionsRequestOngoing = false;
                return execRequests(requests.itemsByShard, requests.rowSourceInfos, upsertResults);
            });
    }

    private static void collectFailingSourceUris(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests,
                                                 final UpsertResults upsertResults) {
        for (Map.Entry<String, String> entry : requests.sourceUrisWithFailure.entrySet()) {
            upsertResults.addUriFailure(entry.getKey(), entry.getValue());
        }
    }

    private static void collectFailingItems(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests,
                                            final UpsertResults upsertResults) {
        for (Map.Entry<String, List<ShardedRequests.ReadFailureAndLineNumber>> entry : requests.itemsWithFailureBySourceUri.entrySet()) {
            String sourceUri = entry.getKey();
            for (ShardedRequests.ReadFailureAndLineNumber readFailureAndLineNumber : entry.getValue()) {
                upsertResults.addResult(sourceUri, readFailureAndLineNumber.readFailure, readFailureAndLineNumber.lineNumber);
            }
        }
    }

    private CompletableFuture<UpsertResults> execRequests(Map<ShardLocation, ShardUpsertRequest> itemsByShard,
                                                          List<RowSourceInfo> rowSourceInfos,
                                                          final UpsertResults upsertResults) {
        if (itemsByShard.isEmpty()) {
            // could be that processing the source uri only results in errors, so no items per shard exists
            return CompletableFuture.completedFuture(upsertResults);
        }
        final AtomicInteger numRequests = new AtomicInteger(itemsByShard.size());
        final AtomicReference<Exception> interrupt = new AtomicReference<>(null);
        final CompletableFuture<UpsertResults> resultFuture = new CompletableFuture<>();
        Iterator<Map.Entry<ShardLocation, ShardUpsertRequest>> it = itemsByShard.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ShardLocation, ShardUpsertRequest> entry = it.next();
            ShardUpsertRequest request = entry.getValue();
            it.remove();

            String nodeId = entry.getKey().nodeId;
            nodeJobsCounter.increment(nodeId);
            ActionListener<ShardResponse> listener =
                new ShardResponseActionListener(
                    nodeId,
                    numRequests,
                    interrupt,
                    upsertResults,
                    resultCollector.accumulator(),
                    rowSourceInfos,
                    resultFuture);

            listener = new RetryListener<>(
                scheduler,
                l -> {
                    if (isDebugEnabled) {
                        LOGGER.debug("Executing retry Listener for nodeId: {} request: {}", nodeId, request);
                    }
                    requestExecutor.execute(request, l);
                },
                listener,
                BACKOFF_POLICY
            );
            requestExecutor.execute(request, listener);
        }
        return resultFuture;
    }


    private CompletableFuture<AcknowledgedResponse> createPartitions(
        Map<String, List<ShardedRequests.ItemAndRoutingAndSourceInfo<ShardUpsertRequest.Item>>> itemsByMissingIndex) {
        FutureActionListener<AcknowledgedResponse, AcknowledgedResponse> listener = FutureActionListener.newInstance();
        createPartitionsAction.execute(
            new CreatePartitionsRequest(itemsByMissingIndex.keySet(), jobId), listener);
        return listener;
    }

    private boolean shouldPauseOnTargetNodeJobsCounter(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests) {
        for (ShardLocation shardLocation : requests.itemsByShard.keySet()) {
            String requestNodeId = shardLocation.nodeId;
            if (nodeJobsCounter.getInProgressJobsForNode(requestNodeId) >= MAX_NODE_CONCURRENT_OPERATIONS) {
                if (isDebugEnabled) {
                    LOGGER.debug("reached maximum concurrent operations for node {}", requestNodeId);
                }
                return true;
            }
        }
        return false;
    }

    /** @noinspection unused*/
    private boolean shouldPauseOnPartitionCreation(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> ignore) {
        if (createPartitionsRequestOngoing) {
            if (isDebugEnabled) {
                LOGGER.debug("partition creation in progress, will pause");
            }
            return true;
        }
        return false;
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator<Row> batchIterator) {
        var reqBatchIterator = BatchIterators.partition(
            batchIterator,
            bulkSize,
            () -> new ShardedRequests<>(requestFactory),
            grouper,
            bulkShardCreationLimiter.or(isUsedBytesOverThreshold)
        );

        // If IO is involved the source iterator should pause when the target node reaches a concurrent job counter limit.
        // Without IO, we assume that the source iterates over in-memory structures which should be processed as
        // fast as possible to free resources.
        Predicate<ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item>> shouldPause =
            this::shouldPauseOnPartitionCreation;
        if (batchIterator.involvesIO()) {
            shouldPause = shouldPause.or(this::shouldPauseOnTargetNodeJobsCounter);
        }

        BatchIteratorBackpressureExecutor<ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item>, UpsertResults> executor =
            new BatchIteratorBackpressureExecutor<>(
                scheduler,
                this.executor,
                reqBatchIterator,
                this::execute,
                resultCollector.combiner(),
                resultCollector.supplier().get(),
                shouldPause,
                BACKOFF_POLICY
            );
        return executor.consumeIteratorAndExecute()
            .thenApply(upsertResults -> resultCollector.finisher().apply(upsertResults));
    }

    private class ShardResponseActionListener implements ActionListener<ShardResponse> {
        private final String operationNodeId;
        private final UpsertResultCollector.Accumulator resultAccumulator;
        private final List<RowSourceInfo> rowSourceInfos;
        private final UpsertResults upsertResults;
        private final AtomicInteger numRequests;
        private final AtomicReference<Exception> interrupt;
        private final CompletableFuture<UpsertResults> upsertResultFuture;

        ShardResponseActionListener(String operationNodeId,
                                    AtomicInteger numRequests,
                                    AtomicReference<Exception> interrupt,
                                    UpsertResults upsertResults,
                                    UpsertResultCollector.Accumulator resultAccumulator,
                                    List<RowSourceInfo> rowSourceInfos,
                                    CompletableFuture<UpsertResults> upsertResultFuture) {
            this.operationNodeId = operationNodeId;
            this.numRequests = numRequests;
            this.interrupt = interrupt;
            this.upsertResults = upsertResults;
            this.resultAccumulator = resultAccumulator;
            this.rowSourceInfos = rowSourceInfos;
            this.upsertResultFuture = upsertResultFuture;
        }

        @Override
        public void onResponse(ShardResponse shardResponse) {
            nodeJobsCounter.decrement(operationNodeId);
            resultAccumulator.accept(upsertResults, shardResponse, rowSourceInfos);
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
                Exception interruptedException = interrupt.get();
                if (interruptedException == null) {
                    upsertResultFuture.complete(upsertResults);
                } else {
                    upsertResultFuture.completeExceptionally(interruptedException);
                }
            }
        }

        private void maybeSetInterrupt(@Nullable Exception failure) {
            if (failure instanceof InterruptedException) {
                interrupt.set(failure);
            }
        }
    }

    private static class IsUsedBytesOverThreshold implements Predicate<ShardedRequests<?, ?>> {

        private static final double HEAP_PERCENTAGE = 0.30d;
        private static final double MAX_FREE_MEM_USAGE_PERCENT = 0.70d;

        private final Runtime rt;
        private final long maxBytesUsableByShardedRequests;

        IsUsedBytesOverThreshold() {
            rt = Runtime.getRuntime();
            maxBytesUsableByShardedRequests = (long) (rt.maxMemory() * HEAP_PERCENTAGE);
        }

        @Override
        public final boolean test(ShardedRequests<?, ?> shardedRequests) {
            return shardedRequests.usedMemoryEstimate() > maxBytesUsableByShardedRequests
                   || shardedRequests.usedMemoryEstimate() > (rt.freeMemory() * MAX_FREE_MEM_USAGE_PERCENT);
        }
    }
}
