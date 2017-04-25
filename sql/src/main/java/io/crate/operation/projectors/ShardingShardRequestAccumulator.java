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
import io.crate.data.BatchAccumulator;
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
import java.util.function.Function;
import java.util.function.Supplier;

public class ShardingShardRequestAccumulator<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements BatchAccumulator<Row, Iterator<? extends Row>> {

    public static final CrateSetting<TimeValue> BULK_REQUEST_TIMEOUT_SETTING = CrateSetting.of(Setting.positiveTimeSetting(
        "bulk.request_timeout", new TimeValue(1, TimeUnit.MINUTES),
        Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING);

    private static final Logger logger = Loggers.getLogger(ShardingShardRequestAccumulator.class);
    private static final BackoffPolicy BACK_OFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);

    private final ClusterService clusterService;
    private final ScheduledExecutorService scheduler;
    private final int bulkSize;
    private final int createIndicesBulkSize;
    private final UUID jobId;
    private final RowShardResolver rowShardResolver;
    private final Function<String, TItem> itemFactory;
    private final BiFunction<ShardId, String, TReq> requestFactory;
    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final Supplier<String> indexNameResolver;
    private final boolean autoCreateIndices;
    private final BulkRequestExecutor<TReq> requestExecutor;
    private final TransportBulkCreateIndicesAction createIndicesAction;
    private final Map<ShardLocation, TReq> requestsByShard = new HashMap<ShardLocation, TReq>();
    private final Map<String, List<PendingRequest<TItem>>> pendingRequestsByIndex = new HashMap<>();
    private final BitSet responses = new BitSet();
    private final NodeJobsCounter nodeJobsCounter;

    private int location = -1;

    public ShardingShardRequestAccumulator(ClusterService clusterService,
                                           NodeJobsCounter nodeJobsCounter,
                                           ScheduledExecutorService scheduler,
                                           int bulkSize,
                                           int createIndicesBulkSize,
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
        this.createIndicesBulkSize = createIndicesBulkSize;
        this.jobId = jobId;
        this.rowShardResolver = rowShardResolver;
        this.itemFactory = itemFactory;
        this.requestFactory = requestFactory;
        this.expressions = expressions;
        this.indexNameResolver = indexNameResolver;
        this.autoCreateIndices = autoCreateIndices;
        this.requestExecutor = requestExecutor;
        this.createIndicesAction = createIndicesAction;
    }

    @Override
    public void onItem(Row row) {
        rowShardResolver.setNextRow(row);
        for (int i = 0; i < expressions.size(); i++) {
            CollectExpression<Row, ?> collectExpression = expressions.get(i);
            collectExpression.setNextRow(row);
        }
        TItem item = itemFactory.apply(rowShardResolver.id());
        String indexName = indexNameResolver.get();
        ShardLocation shardLocation = getShardLocation(indexName, rowShardResolver.id(), rowShardResolver.routing());
        if (shardLocation == null) {
            addToPendingRequests(item, indexName);
        } else {
            addToRequest(item, shardLocation);
        }
    }

    private void addToRequest(TItem item, ShardLocation shardLocation) {
        TReq req = requestsByShard.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId, rowShardResolver.routing());
            requestsByShard.put(shardLocation, req);
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
            if (shardRouting == null || shardRouting.active() == false) {
                nodeId = shardRouting.relocatingNodeId();
            } else {
                nodeId = shardRouting.currentNodeId();
            }

            if(nodeId == null) {
                logger.debug("Unable to get the node id for index {} and shard {}", indexName, id);
            }
            return new ShardLocation(shardIterator.shardId(), nodeId);
        } catch (IndexNotFoundException e) {
            if (!autoCreateIndices) {
                throw e;
            }
            return null;
        }
    }

    private static class ShardLocation {

        private final ShardId shardId;
        private final String nodeId;

        public ShardLocation(ShardId shardId, String nodeId) {
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

    @Override
    public int batchSize() {
        return bulkSize;
    }

    private CompletableFuture<BitSet> execute(boolean isLastBatch) {
        if ((isLastBatch && pendingRequestsByIndex.isEmpty() == false)
            || pendingRequestsByIndex.size() > createIndicesBulkSize) {

            return createPendingIndices()
                .thenCompose(resp -> sendRequests(isLastBatch));
        }
        return sendRequests(isLastBatch);
    }

    @Override
    public CompletableFuture<Iterator<? extends Row>> processBatch(boolean isLastBatch) {
        return execute(isLastBatch).thenApply(r -> createResultIt(isLastBatch));
    }

    private CompletableFuture<BitSet> sendRequests(boolean isLastBatch) {
        if (requestsByShard.isEmpty()) {
            return CompletableFuture.completedFuture(responses);
        }
        CompletableFuture<BitSet> result = new CompletableFuture<>();
        AtomicInteger numRequests = new AtomicInteger(requestsByShard.size());
        for (Iterator<Map.Entry<ShardLocation, TReq>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<ShardLocation, TReq> entry = it.next();
            TReq request = entry.getValue();
            it.remove();

            final ShardLocation shardLocation = entry.getKey();
            nodeJobsCounter.increment(shardLocation.nodeId);
            ActionListener<ShardResponse> listener = new ActionListener<ShardResponse>() {

                @Override
                public void onResponse(ShardResponse shardResponse) {
                    nodeJobsCounter.decrement(shardLocation.nodeId);
                    processShardResponse(shardResponse);
                    countdown();
                }

                @Override
                public void onFailure(Exception e) {
                    nodeJobsCounter.decrement(shardLocation.nodeId);
                    countdown();
                }

                private void countdown() {
                    if (numRequests.decrementAndGet() == 0) {
                        result.complete(responses);
                    }
                }
            };

            listener = new RetryListener<>(
                scheduler,
                l -> requestExecutor.execute(request, l),
                listener,
                BACK_OFF_POLICY
            );
            requestExecutor.execute(request, listener);
        }
        return result;
    }

    private void processShardResponse(ShardResponse shardResponse) {
        IntArrayList itemIndices = shardResponse.itemIndices();
        List<ShardResponse.Failure> failures = shardResponse.failures();
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

    private Iterator<? extends Row> createResultIt(boolean isLastBatch) {
        Iterator<? extends Row> iterator;
        if (isLastBatch) {
            synchronized (responses) {
                iterator = Collections.<Row>singletonList(new Row1((long) responses.cardinality())).iterator();
            }
        } else {
            iterator = Collections.emptyIterator();
        }
        return iterator;
    }

    private CompletableFuture<BulkCreateIndicesResponse> createPendingIndices() {
        FutureActionListener<BulkCreateIndicesResponse, BulkCreateIndicesResponse> listener =
            new FutureActionListener<>(r -> {
                drainFromPendingToRequestMap();
                return r;
            });
        createIndicesAction.execute(new BulkCreateIndicesRequest(pendingRequestsByIndex.keySet(), jobId), listener);
        return listener;
    }

    private void drainFromPendingToRequestMap() {
        Iterator<Map.Entry<String, List<PendingRequest<TItem>>>> it = pendingRequestsByIndex.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, List<PendingRequest<TItem>>> entry = it.next();
            String index = entry.getKey();
            List<PendingRequest<TItem>> pendingRequests = entry.getValue();
            it.remove();

            for (int i = 0; i < pendingRequests.size(); i++) {
                PendingRequest<TItem> pendingRequest = pendingRequests.get(i);
                ShardLocation shardLocation = getShardLocation(index, pendingRequest.item.id(), pendingRequest.routing);
                assert shardLocation != null : "TODO";

                addToRequest(pendingRequest.item, shardLocation);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void reset() {
        pendingRequestsByIndex.clear();
        requestsByShard.clear();
    }

    private static class PendingRequest<TItem> {

        private final TItem item;
        private final String routing;

        PendingRequest(TItem item, String routing) {
            this.item = item;
            this.routing = routing;
        }
    }
}
