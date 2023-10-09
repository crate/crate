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


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.jetbrains.annotations.Nullable;

import io.crate.common.concurrent.ConcurrencyLimit;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.support.RetryListener;

public class ShardDMLExecutor<TReq extends ShardRequest<TReq, TItem>,
                              TItem extends ShardRequest.Item,
                              TAcc,
                              TResult extends Iterable<? extends Row>>
    implements Function<BatchIterator<Row>, CompletableFuture<? extends Iterable<? extends Row>>> {

    private static final Logger LOGGER = LogManager.getLogger(ShardDMLExecutor.class);

    public static final int DEFAULT_BULK_SIZE = 10_000;

    private final UUID jobId;
    private final int bulkSize;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final CollectExpression<Row, ?> uidExpression;
    private final NodeLimits nodeLimits;
    private final Supplier<TReq> requestFactory;
    private final Function<String, TItem> itemFactory;
    private final BiConsumer<TReq, ActionListener<ShardResponse>> operation;
    private final Collector<ShardResponse, TAcc, TResult> collector;
    private final String localNode;
    private final CircuitBreaker queryCircuitBreaker;
    private final ClusterService clusterService;
    private int numItems = -1;
    private final RamAccounting ramAccounting;


    public ShardDMLExecutor(UUID jobId,
                            int bulkSize,
                            ScheduledExecutorService scheduler,
                            Executor executor,
                            CollectExpression<Row, ?> uidExpression,
                            ClusterService clusterService,
                            RamAccounting ramAccounting,
                            CircuitBreaker queryCircuitBreaker,
                            NodeLimits nodeLimits,
                            Supplier<TReq> requestFactory,
                            Function<String, TItem> itemFactory,
                            BiConsumer<TReq, ActionListener<ShardResponse>> transportAction,
                            Collector<ShardResponse, TAcc, TResult> collector
                            ) {
        this.queryCircuitBreaker = queryCircuitBreaker;
        this.ramAccounting = ramAccounting;
        this.localNode = clusterService.state().nodes().getLocalNodeId();
        this.jobId = jobId;
        this.bulkSize = bulkSize;
        this.scheduler = scheduler;
        this.executor = executor;
        this.uidExpression = uidExpression;
        this.clusterService = clusterService;
        this.nodeLimits = nodeLimits;
        this.requestFactory = requestFactory;
        this.itemFactory = itemFactory;
        this.operation = transportAction;
        this.collector = collector;
    }

    private void addRowToRequest(TReq req, Row row) {
        numItems++;
        uidExpression.setNextRow(row);
        TItem item = itemFactory.apply((String) uidExpression.value());
        synchronized (ramAccounting) {
            ramAccounting.addBytes(item.ramBytesUsed());
        }
        req.add(numItems, item);
    }

    @Nullable
    private String resolveNodeId(TReq request) {
        // The primary shard might be moving to another node,
        // so this is not 100% accurate but good enough for the congestion control purposes.
        try {
            ShardRouting primaryShard = clusterService
                .state()
                .routingTable()
                .shardRoutingTable(request.shardId())
                .primaryShard();
            return primaryShard == null ? null : primaryShard.currentNodeId();
        } catch (IndexNotFoundException | ShardNotFoundException ignored) {
            return null;
        }
    }

    private CompletableFuture<TAcc> executeBatch(TReq request) {
        ConcurrencyLimit nodeLimit = nodeLimits.get(resolveNodeId(request));
        long startTime = nodeLimit.startSample();
        CompletableFuture<TAcc> future = new CompletableFuture<>();
        var listener = new ActionListener<ShardResponse>() {

            @Override
            public void onResponse(ShardResponse response) {
                nodeLimit.onSample(startTime, false);
                synchronized (ramAccounting) {
                    for (var item : request.items()) {
                        ramAccounting.addBytes(- item.ramBytesUsed());
                    }
                }
                TAcc acc = collector.supplier().get();
                try {
                    collector.accumulator().accept(acc, response);
                    future.complete(acc);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }

            @Override
            public void onFailure(Exception e) {
                nodeLimit.onSample(startTime, true);
                synchronized (ramAccounting) {
                    for (var item : request.items()) {
                        ramAccounting.addBytes(- item.ramBytesUsed());
                    }
                }
                future.completeExceptionally(e);
            }
        };
        operation.accept(request, withRetry(request, nodeLimit, listener));
        return future;
    }

    private RetryListener<ShardResponse> withRetry(TReq request,
                                                   ConcurrencyLimit nodeLimit,
                                                   ActionListener<ShardResponse> listener) {
        return new RetryListener<>(
            scheduler,
            l -> operation.accept(request, l),
            listener,
            BackoffPolicy.unlimitedDynamic(nodeLimit)
        );
    }

    @Override
    public CompletableFuture<TResult> apply(BatchIterator<Row> batchIterator) {
        ConcurrencyLimit nodeLimit = nodeLimits.get(localNode);
        var isUsedBytesOverThreshold = new IsUsedBytesOverThreshold(queryCircuitBreaker, nodeLimit);
        BatchIterator<TReq> reqBatchIterator = BatchIterators.chunks(
            batchIterator,
            bulkSize,
            requestFactory,
            this::addRowToRequest,
            isUsedBytesOverThreshold
        );
        // If the source batch iterator does not involve IO, mostly in-memory structures are used which we want to free
        // as soon as possible. We do not want to throttle based on the targets node counter in such cases.
        Predicate<TReq> shouldPause = ignored -> true;
        if (batchIterator.hasLazyResultSet()) {
            shouldPause = req -> {
                var requestNodeId = resolveNodeId(req);
                var requestNodeLimit = nodeLimits.get(requestNodeId);
                if (requestNodeLimit.exceedsLimit()) {
                    LOGGER.info(
                            "Overload protection: reached maximum concurrent operations for node {}" +
                            " (limit={}, rrt={}ms, inflight={})",
                            requestNodeId,
                            requestNodeLimit.getLimit(),
                            requestNodeLimit.getLastRtt(TimeUnit.MILLISECONDS),
                            requestNodeLimit.numInflight()
                    );
                    return true;
                }
                return false;
            };
        }
        return new BatchIteratorBackpressureExecutor<>(
            jobId,
            scheduler,
            executor,
            reqBatchIterator,
            this::executeBatch,
            collector.combiner(),
            collector.supplier().get(),
            shouldPause,
            null,
            null,
            req -> nodeLimits.get(resolveNodeId(req)).getLastRtt(TimeUnit.MILLISECONDS)
        ).consumeIteratorAndExecute()
            .thenApply(collector.finisher());
    }

    @Nullable
    private static String getLocalNodeId(ClusterService clusterService) {
        String nodeId = null;
        try {
            nodeId = clusterService.localNode().getId();
        } catch (IllegalStateException e) {
            LOGGER.debug("Unable to get local node id", e);
        }
        return nodeId;
    }

    public static void maybeRaiseFailure(@Nullable Exception exception) {
        if (exception != null) {
            Throwable t = SQLExceptions.unwrap(exception);
            if (!(t instanceof DocumentMissingException) && !(t instanceof VersionConflictEngineException)) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    public static final Collector<ShardResponse, long[], Iterable<Row>> ROW_COUNT_COLLECTOR = Collector.of(
        () -> new long[]{0L},
        (acc, response) -> {
            maybeRaiseFailure(response.failure());
            acc[0] += response.successRowCount();
        },
        (acc, response) -> {
            acc[0] += response[0];
            return acc;
        },
        acc -> List.of(new Row1(acc[0]))
    );

    public static final Collector<ShardResponse, List<Object[]>, Iterable<Row>> RESULT_ROW_COLLECTOR = Collector.of(
        ArrayList::new,
        (acc, response) -> {
            maybeRaiseFailure(response.failure());
            List<Object[]> rows = response.getResultRows();
            if (rows != null) {
                acc.addAll(rows);
            }
        },
        (acc, response) -> {
            acc.addAll(response);
            return acc;
        },
        CollectionBucket::new
    );
}
