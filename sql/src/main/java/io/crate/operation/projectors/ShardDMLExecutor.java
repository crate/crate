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

import com.google.common.base.Throwables;
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
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.crate.operation.NodeJobsCounter.MAX_NODE_CONCURRENT_OPERATIONS;

public class ShardDMLExecutor<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements Function<BatchIterator<Row>, CompletableFuture<? extends Iterable<? extends Row>>> {

    private static final Logger LOGGER = Loggers.getLogger(ShardDMLExecutor.class);

    private static final BackoffPolicy BACKOFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);
    static final int DEFAULT_BULK_SIZE = 10_000;

    private final int bulkSize;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;
    private final CollectExpression<Row, ?> uidExpression;
    private final NodeJobsCounter nodeJobsCounter;
    private final Supplier<TReq> requestFactory;
    private final Function<String, TItem> itemFactory;
    private final BiConsumer<TReq, ActionListener<ShardResponse>> operation;
    private final Predicate<TReq> shouldPause;
    private final String localNodeId;

    private int numItems = -1;

    ShardDMLExecutor(int bulkSize,
                     ScheduledExecutorService scheduler,
                     Executor executor,
                     CollectExpression<Row, ?> uidExpression,
                     ClusterService clusterService,
                     NodeJobsCounter nodeJobsCounter,
                     Supplier<TReq> requestFactory,
                     Function<String, TItem> itemFactory,
                     BiConsumer<TReq, ActionListener<ShardResponse>> transportAction) {
        this.bulkSize = bulkSize;
        this.scheduler = scheduler;
        this.executor = executor;
        this.uidExpression = uidExpression;
        this.nodeJobsCounter = nodeJobsCounter;
        this.requestFactory = requestFactory;
        this.itemFactory = itemFactory;
        this.operation = transportAction;
        this.localNodeId = getLocalNodeId(clusterService);

        this.shouldPause = ignored ->
            nodeJobsCounter.getInProgressJobsForNode(localNodeId) >= MAX_NODE_CONCURRENT_OPERATIONS;
    }

    private void addRowToRequest(TReq req, Row row) {
        numItems++;
        uidExpression.setNextRow(row);
        req.add(numItems, itemFactory.apply(((BytesRef) uidExpression.value()).utf8ToString()));
    }

    private CompletableFuture<Long> executeBatch(TReq request) {
        FutureActionListener<ShardResponse, Long> listener = new FutureActionListener<>(ShardDMLExecutor::processShardResponse);
        nodeJobsCounter.increment(localNodeId);
        CompletableFuture<Long> result = listener.whenComplete((r, f) -> nodeJobsCounter.decrement(localNodeId));
        operation.accept(request, withRetry(request, listener));
        return result;
    }

    private RetryListener<ShardResponse> withRetry(TReq request, FutureActionListener<ShardResponse, Long> listener) {
        return new RetryListener<>(
            scheduler,
            l -> operation.accept(request, l),
            listener,
            BACKOFF_POLICY
        );
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator<Row> batchIterator) {
        BatchIterator<TReq> reqBatchIterator =
            BatchIterators.partition(batchIterator, bulkSize, requestFactory, this::addRowToRequest);
        BinaryOperator<Long> combinePartialResult = (a, b) -> a + b;
        long initialResult = 0L;
        return new BatchIteratorBackpressureExecutor<>(
            scheduler,
            executor,
            reqBatchIterator,
            this::executeBatch,
            combinePartialResult,
            initialResult,
            shouldPause,
            BACKOFF_POLICY
        ).consumeIteratorAndExecute()
            .thenApply(rowCount -> Collections.singletonList(new Row1(rowCount == null ? 0L : rowCount)));
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

    private static long processShardResponse(ShardResponse shardResponse) {
        Exception failure = shardResponse.failure();
        if (failure != null) {
            Throwables.throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
        return shardResponse.successRowCount();
    }
}
