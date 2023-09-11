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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsAction;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.common.concurrent.ConcurrencyLimit;
import io.crate.common.unit.TimeValue;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.Item;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.support.RetryListener;

public class ShardingUpsertExecutor
    implements Function<BatchIterator<Row>, CompletableFuture<? extends Iterable<? extends Row>>> {

    public static final Setting<TimeValue> BULK_REQUEST_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "bulk.request_timeout",
        new TimeValue(1, TimeUnit.MINUTES),
        Property.NodeScope,
        Property.Dynamic,
        Property.Exposed
    );

    static final Logger LOGGER = LogManager.getLogger(ShardingUpsertExecutor.class);
    static final double BREAKER_LIMIT_PERCENTAGE = 0.50d;

    private final GroupRowsByShard<ShardUpsertRequest, ShardUpsertRequest.Item> grouper;
    private final NodeLimits nodeLimits;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final int bulkSize;
    private final UUID jobId;
    private final Function<ShardId, ShardUpsertRequest> requestFactory;
    private final BulkRequestExecutor<ShardUpsertRequest> requestExecutor;
    private final ElasticsearchClient elasticsearchClient;
    private final BulkShardCreationLimiter bulkShardCreationLimiter;
    private final UpsertResultCollector resultCollector;
    private final boolean isDebugEnabled;
    private final CircuitBreaker queryCircuitBreaker;
    private final String localNode;
    private final BlockBasedRamAccounting ramAccounting;
    private volatile boolean createPartitionsRequestOngoing = false;
    private final Predicate<UpsertResults> earlyTerminationCondition;
    private final Function<UpsertResults, Throwable> earlyTerminationExceptionGenerator;

    ShardingUpsertExecutor(ClusterService clusterService,
                           BiConsumer<String, IndexItem> constraintsChecker,
                           NodeLimits nodeJobsCounter,
                           CircuitBreaker queryCircuitBreaker,
                           RamAccounting ramAccounting,
                           ScheduledExecutorService scheduler,
                           Executor executor,
                           int bulkSize,
                           UUID jobId,
                           RowShardResolver rowShardResolver,
                           ItemFactory<ShardUpsertRequest.Item> itemFactory,
                           Function<ShardId, ShardUpsertRequest> requestFactory,
                           List<? extends CollectExpression<Row, ?>> expressions,
                           Supplier<String> indexNameResolver,
                           boolean autoCreateIndices,
                           ElasticsearchClient elasticsearchClient,
                           int targetTableNumShards,
                           int targetTableNumReplicas,
                           UpsertResultContext upsertResultContext,
                           Predicate<UpsertResults> earlyTerminationCondition,
                           Function<UpsertResults, Throwable> earlyTerminationExceptionGenerator) {
        this.localNode = clusterService.state().nodes().getLocalNodeId();
        this.nodeLimits = nodeJobsCounter;
        this.queryCircuitBreaker = queryCircuitBreaker;
        this.scheduler = scheduler;
        this.executor = executor;
        this.bulkSize = bulkSize;
        this.jobId = jobId;
        this.requestFactory = requestFactory;
        this.requestExecutor = (req, resp) -> elasticsearchClient.execute(ShardUpsertAction.INSTANCE, req)
            .whenComplete(resp);
        this.elasticsearchClient = elasticsearchClient;
        this.ramAccounting = new BlockBasedRamAccounting(ramAccounting::addBytes, (int) ByteSizeUnit.MB.toBytes(2));
        this.grouper = new GroupRowsByShard<>(
            clusterService,
            constraintsChecker,
            rowShardResolver,
            indexNameResolver,
            expressions,
            itemFactory,
            autoCreateIndices,
            upsertResultContext);
        bulkShardCreationLimiter = new BulkShardCreationLimiter(
            targetTableNumShards,
            targetTableNumReplicas,
            clusterService.state().nodes().getDataNodes().size());
        this.resultCollector = upsertResultContext.getResultCollector();
        isDebugEnabled = LOGGER.isDebugEnabled();
        this.earlyTerminationCondition = earlyTerminationCondition;
        this.earlyTerminationExceptionGenerator = earlyTerminationExceptionGenerator;
    }

    public CompletableFuture<UpsertResults> execute(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests) {
        final UpsertResults upsertResults = resultCollector.supplier().get();
        collectFailingSourceUris(requests, upsertResults);
        collectFailingItems(requests, upsertResults);

        if (requests.itemsByMissingIndex.isEmpty()) {
            return execRequests(requests, upsertResults);
        }
        createPartitionsRequestOngoing = true;
        return elasticsearchClient.execute(
            CreatePartitionsAction.INSTANCE,
            new CreatePartitionsRequest(requests.itemsByMissingIndex.keySet()))
            .thenCompose(resp -> {
                grouper.reResolveShardLocations(requests);
                createPartitionsRequestOngoing = false;
                return execRequests(requests, upsertResults);
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

    private CompletableFuture<UpsertResults> execRequests(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests,
                                                          final UpsertResults upsertResults) {
        if (requests.itemsByShard.isEmpty()) {
            requests.close();
            // could be that processing the source uri only results in errors, so no items per shard exists
            return CompletableFuture.completedFuture(upsertResults);
        }
        final AtomicInteger numRequests = new AtomicInteger(requests.itemsByShard.size());
        final AtomicReference<Exception> interrupt = new AtomicReference<>(null);
        final CompletableFuture<UpsertResults> resultFuture = new CompletableFuture<>();
        Iterator<Map.Entry<ShardLocation, ShardUpsertRequest>> it = requests.itemsByShard.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ShardLocation, ShardUpsertRequest> entry = it.next();
            ShardUpsertRequest request = entry.getValue();
            it.remove();

            String nodeId = entry.getKey().nodeId;
            ConcurrencyLimit nodeLimit = nodeLimits.get(nodeId);
            ActionListener<ShardResponse> listener =
                new ShardResponseActionListener(
                    numRequests,
                    interrupt,
                    upsertResults,
                    resultCollector.accumulator(),
                    requests.rowSourceInfos,
                    nodeLimit,
                    resultFuture);

            listener = new RetryListener<>(
                scheduler,
                l -> requestExecutor.execute(request, l),
                listener,
                BackoffPolicy.unlimitedDynamic(nodeLimit)
            );
            requestExecutor.execute(request, listener);
        }
        return resultFuture.whenComplete((r, err) -> requests.close());
    }


    private boolean shouldPauseOnTargetNodeJobsCounter(ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item> requests) {
        for (ShardLocation shardLocation : requests.itemsByShard.keySet()) {
            String requestNodeId = shardLocation.nodeId;
            ConcurrencyLimit nodeLimit = nodeLimits.get(requestNodeId);
            if (nodeLimit.exceedsLimit()) {
                if (isDebugEnabled) {
                    LOGGER.debug(
                        "reached maximum concurrent operations for node {} (limit={}, rrt={}ms, inflight={})",
                        requestNodeId,
                        nodeLimit.getLimit(),
                        nodeLimit.getLastRtt(TimeUnit.MILLISECONDS),
                        nodeLimit.numInflight()
                    );
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
        final ConcurrencyLimit nodeLimit = nodeLimits.get(localNode);
        long startTime = nodeLimit.startSample();
        var isUsedBytesOverThreshold = new IsUsedBytesOverThreshold(queryCircuitBreaker, nodeLimit);
        var reqBatchIterator = BatchIterators.chunks(
            batchIterator,
            bulkSize,
            () -> new ShardedRequests<>(requestFactory, ramAccounting),
            grouper,
            bulkShardCreationLimiter.or(isUsedBytesOverThreshold)
        );
        // If IO is involved the source iterator should pause when the target node reaches a concurrent job counter limit.
        // Without IO, we assume that the source iterates over in-memory structures which should be processed as
        // fast as possible to free resources.
        Predicate<ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item>> shouldPause =
            this::shouldPauseOnPartitionCreation;
        if (batchIterator.hasLazyResultSet()) {
            shouldPause = shouldPause
                .or(this::shouldPauseOnTargetNodeJobsCounter)
                .or(isUsedBytesOverThreshold);
        }
        BatchIteratorBackpressureExecutor<ShardedRequests<ShardUpsertRequest, ShardUpsertRequest.Item>, UpsertResults> executor =
            new BatchIteratorBackpressureExecutor<>(
                jobId,
                scheduler,
                this.executor,
                reqBatchIterator,
                this::execute,
                resultCollector.combiner(),
                resultCollector.supplier().get(),
                shouldPause,
                earlyTerminationCondition,
                earlyTerminationExceptionGenerator,
                this::getMaxLastRttInMs
            );
        return executor.consumeIteratorAndExecute()
            .thenApply(upsertResults -> resultCollector.finisher().apply(upsertResults))
            .whenComplete((res, err) -> {
                nodeLimit.onSample(startTime, err != null);
            });
    }

    private long getMaxLastRttInMs(ShardedRequests<ShardUpsertRequest, Item> req) {
        long rtt = 0;
        for (var shardLocation : req.itemsByShard.keySet()) {
            String nodeId = shardLocation.nodeId;
            rtt = Math.max(rtt, nodeLimits.get(nodeId).getLastRtt(TimeUnit.MILLISECONDS));
        }
        return rtt;
    }

    private class ShardResponseActionListener implements ActionListener<ShardResponse> {
        private final UpsertResultCollector.Accumulator resultAccumulator;
        private final List<RowSourceInfo> rowSourceInfos;
        private final UpsertResults upsertResults;
        private final AtomicInteger numRequests;
        private final AtomicReference<Exception> interrupt;
        private final CompletableFuture<UpsertResults> upsertResultFuture;
        private final ConcurrencyLimit nodeLimit;
        private final long startTime;

        ShardResponseActionListener(AtomicInteger numRequests,
                                    AtomicReference<Exception> interrupt,
                                    UpsertResults upsertResults,
                                    UpsertResultCollector.Accumulator resultAccumulator,
                                    List<RowSourceInfo> rowSourceInfos,
                                    ConcurrencyLimit nodeLimit,
                                    CompletableFuture<UpsertResults> upsertResultFuture) {
            this.numRequests = numRequests;
            this.interrupt = interrupt;
            this.upsertResults = upsertResults;
            this.resultAccumulator = resultAccumulator;
            this.rowSourceInfos = rowSourceInfos;
            this.nodeLimit = nodeLimit;
            this.startTime = nodeLimit.startSample();
            this.upsertResultFuture = upsertResultFuture;
        }

        @Override
        public void onResponse(ShardResponse shardResponse) {
            nodeLimit.onSample(startTime, false);
            resultAccumulator.accept(upsertResults, shardResponse, rowSourceInfos);
            maybeSetInterrupt(shardResponse.failure());
            countdown();
        }

        @Override
        public void onFailure(Exception e) {
            nodeLimit.onSample(startTime, true);
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
}
