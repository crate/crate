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

import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardResponse;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesRequest;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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

    private final ClusterService clusterService;
    private final ScheduledExecutorService scheduler;
    private final int bulkSize;
    private final UUID jobId;
    private final RowShardResolver rowShardResolver;
    private final BiFunction<ShardId, String, TReq> requestFactory;
    private final boolean autoCreateIndices;
    private final BulkRequestExecutor<TReq> requestExecutor;
    private final TransportBulkCreateIndicesAction createIndicesAction;
    private final Map<ShardLocation, TReq> requestsByShard = new HashMap<>();
    private final Map<String, List<PendingRequest<TItem>>> pendingRequestsByIndex = new HashMap<>();
    private final BitSet responses = new BitSet();
    private final NodeJobsCounter nodeJobsCounter;

    private int location = -1;

    private final Consumer<Row> rowConsumer;
    private final BooleanSupplier backpressureTrigger;
    private final Supplier<CompletableFuture<BitSet>> execute;

    ShardingUpsertExecutor(ClusterService clusterService,
                           NodeJobsCounter nodeJobsCounter,
                           ScheduledExecutorService scheduler,
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
        this.clusterService = clusterService;
        this.nodeJobsCounter = nodeJobsCounter;
        this.scheduler = scheduler;
        this.bulkSize = bulkSize;
        this.jobId = jobId;
        this.rowShardResolver = rowShardResolver;
        this.requestFactory = requestFactory;
        this.autoCreateIndices = autoCreateIndices;
        this.requestExecutor = requestExecutor;
        this.createIndicesAction = createIndicesAction;
        this.rowConsumer = createRowConsumer(rowShardResolver, itemFactory, expressions, indexNameResolver);
        this.backpressureTrigger = createBackpressureTrigger(nodeJobsCounter);
        this.execute = this::createExecuteFunction;
    }

    private Consumer<Row> createRowConsumer(RowShardResolver rowShardResolver, Function<String, TItem> itemFactory, List<? extends CollectExpression<Row, ?>> expressions, Supplier<String> indexNameResolver) {
        return (Row row) -> {
            rowShardResolver.setNextRow(row);
            for (int i = 0; i < expressions.size(); i++) {
                CollectExpression<Row, ?> collectExpression = expressions.get(i);
                collectExpression.setNextRow(row);
            }
            TItem item = itemFactory.apply(rowShardResolver.id());
            String indexName = indexNameResolver.get();
            ShardLocation shardLocation =
                getShardLocation(indexName, rowShardResolver.id(), rowShardResolver.routing());
            if (shardLocation == null) {
                addToPendingRequests(item, indexName);
            } else {
                addToRequest(item, shardLocation, requestsByShard);
            }
        };
    }

    private BooleanSupplier createBackpressureTrigger(NodeJobsCounter nodeJobsCounter) {
        return () -> {
            for (ShardLocation shardLocation : requestsByShard.keySet()) {
                String requestNodeId = shardLocation.nodeId;
                if (nodeJobsCounter.getInProgressJobsForNode(requestNodeId) >= MAX_NODE_CONCURRENT_OPERATIONS) {
                    LOGGER.debug("reached maximum concurrent operations for node {}", requestNodeId);
                    return true;
                }
            }
            return false;
        };
    }

    private CompletableFuture<BitSet> createExecuteFunction() {
        CompletableFuture<BitSet> executeBulkFuture = new CompletableFuture<>();

        if (pendingRequestsByIndex.isEmpty() == false) {
            // We create new indices in an async fashion and execute the requests after they are created.
            // This means the iterator consumer thread can carry on accumulating rows for the next bulk.
            // We copy and clear the current bulk global requests and pendingRequests structures in order to allow the
            // iterator consumer thread to create the next bulk whilst we create indices and execute this bulk.
            Map<String, List<PendingRequest<TItem>>> pendingRequestsForCurrentBulk =
                new HashMap<>(pendingRequestsByIndex);
            pendingRequestsByIndex.clear();

            Map<ShardLocation, TReq> bulkRequests = new HashMap<>(requestsByShard);
            requestsByShard.clear();

            createPendingIndices(pendingRequestsForCurrentBulk)
                .thenAccept(resp -> {
                    bulkRequests.putAll(getFromPendingToRequestMap(pendingRequestsForCurrentBulk));
                    sendRequestsForBulk(executeBulkFuture, bulkRequests);
                });
        } else {
            sendRequestsForBulk(executeBulkFuture, requestsByShard);
        }

        return executeBulkFuture;
    }

    @Override
    public CompletableFuture<? extends Iterable<Row>> apply(BatchIterator batchIterator) {
        return new BatchIteratorBackpressureExecutor<>(
            batchIterator,
            scheduler,
            rowConsumer,
            execute,
            backpressureTrigger,
            bulkSize,
            BACKOFF_POLICY
        ).consumeIteratorAndExecute()
            .thenApply(ignored -> Collections.singletonList(new Row1((long) responses.cardinality())));
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

            String nodeId;
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

    private void addToRequest(TItem item, ShardLocation shardLocation, Map<ShardLocation, TReq> requests) {
        TReq req = requests.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId, rowShardResolver.routing());
            requests.put(shardLocation, req);
        }
        location++;
        req.add(location, item);
    }

    private void addToPendingRequests(TItem item, String indexName) {
        List<PendingRequest<TItem>> pendingRequests = pendingRequestsByIndex.get(indexName);
        if (pendingRequests == null) {
            pendingRequests = new ArrayList<>();
            pendingRequestsByIndex.put(indexName, pendingRequests);
        }
        pendingRequests.add(new PendingRequest<>(item, rowShardResolver.routing()));
    }

    private void sendRequestsForBulk(CompletableFuture<BitSet> bulkResultFuture, Map<ShardLocation, TReq> bulkRequests) {
        if (bulkRequests.isEmpty()) {
            bulkResultFuture.complete(responses);
            return;
        }
        AtomicInteger numRequests = new AtomicInteger(bulkRequests.size());
        Iterator<Map.Entry<ShardLocation, TReq>> it = bulkRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ShardLocation, TReq> entry = it.next();
            TReq request = entry.getValue();
            // We need to drain the bulkRequests map because when indices don't need to be created we use the global
            // requestsByShard map. We need requestsByShard empty after a bulk is triggered for execution so it can
            // store the requests for the next bulk.
            it.remove();

            final ShardLocation shardLocation = entry.getKey();
            nodeJobsCounter.increment(shardLocation.nodeId);
            ActionListener<ShardResponse> listener = new ActionListener<ShardResponse>() {

                InterruptedException interruptedException = null;

                @Override
                public void onResponse(ShardResponse shardResponse) {
                    nodeJobsCounter.decrement(shardLocation.nodeId);
                    processShardResponse(shardResponse);
                    maybeSetInterrupt(shardResponse.failure());
                    countdown();
                }

                private void maybeSetInterrupt(@Nullable Exception failure) {
                    if (failure instanceof InterruptedException) {
                        interruptedException = (InterruptedException) failure;
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    nodeJobsCounter.decrement(shardLocation.nodeId);
                    countdown();
                }

                private void countdown() {
                    if (numRequests.decrementAndGet() == 0) {
                        if (interruptedException == null) {
                            bulkResultFuture.complete(responses);
                        } else {
                            bulkResultFuture.completeExceptionally(interruptedException);
                        }
                    }
                }
            };

            listener = new RetryListener<>(
                scheduler,
                l -> {
                    LOGGER.debug("Executing retry Listener for nodeId: {} request: {}",
                        shardLocation.nodeId,
                        request);
                    requestExecutor.execute(request, l);
                },
                listener,
                BACKOFF_POLICY
            );
            requestExecutor.execute(request, listener);
        }
    }

    private void processShardResponse(ShardResponse shardResponse) {
        synchronized (responses) {
            ShardResponse.markResponseItemsAndFailures(shardResponse, responses);
        }
    }

    private CompletableFuture<BulkCreateIndicesResponse> createPendingIndices(
        Map<String, List<PendingRequest<TItem>>> requestsByIndexForCurrentBulk) {

        FutureActionListener<BulkCreateIndicesResponse, BulkCreateIndicesResponse> listener =
            new FutureActionListener<>(r -> r);
        createIndicesAction.execute(new BulkCreateIndicesRequest(requestsByIndexForCurrentBulk.keySet(), jobId),
            listener);
        return listener;
    }

    private Map<ShardLocation, TReq> getFromPendingToRequestMap(
        Map<String, List<PendingRequest<TItem>>> requestsByIndexForCurrentBulk) {
        Map<ShardLocation, TReq> requests = new HashMap<>();

        for (Map.Entry<String,List<PendingRequest<TItem>>> indexToRequestEntry : requestsByIndexForCurrentBulk.entrySet()) {
            String index = indexToRequestEntry.getKey();
            List<PendingRequest<TItem>> pendingRequests = indexToRequestEntry.getValue();

            for (int i = 0; i < pendingRequests.size(); i++) {
                PendingRequest<TItem> pendingRequest = pendingRequests.get(i);
                ShardLocation shardLocation = getShardLocation(index, pendingRequest.item.id(), pendingRequest.routing);
                assert shardLocation != null : "Unable to get location of shard " + pendingRequest.item.id() +
                                               " for index " + index;
                addToRequest(pendingRequest.item, shardLocation, requests);
            }
        }
        return requests;
    }

    private static class PendingRequest<TItem> {
        private final TItem item;
        private final String routing;

        PendingRequest(TItem item, String routing) {
            this.item = item;
            this.routing = routing;
        }
    }

    private static class ShardLocation {
        private final ShardId shardId;
        private final String nodeId;

        ShardLocation(ShardId shardId, String nodeId) {
            this.shardId = shardId;
            this.nodeId = nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ShardLocation that = (ShardLocation) o;

            if (!shardId.equals(that.shardId)) return false;
            return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
        }

        @Override
        public int hashCode() {
            int result = shardId.hashCode();
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            return result;
        }

    }
}
