/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.action.bulk;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import jsr166e.LongAdder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processor to do Bulk Inserts, similar to {@link org.elasticsearch.action.bulk.BulkProcessor}
 * but less flexible (only supports IndexRequests)
 *
 * If the Bulk threadPool Queue is full retries are made and
 * the {@link #add(String, org.elasticsearch.common.bytes.BytesReference, String, String)} method will start to block.
 */
public class BulkShardProcessor {

    private final ClusterService clusterService;
    private final TransportShardBulkAction transportShardBulkAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final boolean autoCreateIndices;
    private final int bulkSize;
    private final Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
    private final AutoCreateIndex autoCreateIndex;
    private int counter;
    private AtomicInteger activeRetries = new AtomicInteger(0);
    private final Semaphore semaphore = new Semaphore(1);
    private final SettableFuture<Long> result;
    private final LongAdder rowsInserted = new LongAdder();
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed = false;
    private final AtomicInteger pendingCreateIndexRequests = new AtomicInteger(0);
    private final Set<String> indicesCreated = new HashSet<>();
    private final Object createIndexLock = new Object();

    private final ESLogger logger = Loggers.getLogger(getClass());

    public BulkShardProcessor(ClusterService clusterService,
                              Settings settings,
                              TransportShardBulkAction transportShardBulkAction,
                              TransportCreateIndexAction transportCreateIndexAction,
                              boolean autoCreateIndices,
                              int bulkSize) {
        this.clusterService = clusterService;
        this.transportShardBulkAction = transportShardBulkAction;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.autoCreateIndices = autoCreateIndices;
        this.bulkSize = bulkSize;
        counter = 0;
        result = SettableFuture.create();
        autoCreateIndex = new AutoCreateIndex(settings);
    }

    public boolean add(String indexName, BytesReference source, String id, @Nullable String routing) {
        pending.incrementAndGet();
        blockIfRetriesActive("add");
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        if (autoCreateIndices) {
            createIndexIfRequired(indexName, source, id, routing);
        } else {
            partitionRequestByShard(indexName, source, id, routing);
            executeIfNeeded();
        }
        return true;
    }

    private synchronized void partitionRequestByShard(String indexName, BytesReference source, String id, String routing) {
        ShardId shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                Constants.DEFAULT_MAPPING_TYPE,
                id,
                routing
        ).shardId();

        List<BulkItemRequest> items = requestsByShard.get(shardId);
        if (items == null) {
            items = new ArrayList<>();
            requestsByShard.put(shardId, items);
        }
        IndexRequest indexRequest = new IndexRequest(indexName, Constants.DEFAULT_MAPPING_TYPE, id);
        if (routing != null) {
            indexRequest.routing(routing);
        }
        indexRequest.source(source, false);
        indexRequest.timestamp(Long.toString(System.currentTimeMillis()));
        items.add(new BulkItemRequest(counter, indexRequest));
        counter++;
    }

    private void blockIfRetriesActive(String context) {
        if (activeRetries.get() > 0) {
            try {
                trace(String.format("%s, acquiring semaphore", context));
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    public ListenableFuture<Long> result() {
        return result;
    }

    public void close() {
        closed = true;
        executeIfNeeded();
        if (pending.get() == 0) {
            setResult();
        }
    }

    private void setFailure(Throwable e) {
        failure.compareAndSet(null, e);
        result.setException(e);
    }

    private void setResult() {
        Throwable throwable = failure.get();
        if (throwable == null) {
            result.set(rowsInserted.longValue());
        } else {
            result.setException(throwable);
        }
    }

    private void setResultIfDone(int successes) {
        for (int i = 0; i < successes; i++) {
            if (pending.decrementAndGet() == 0 && closed) {
                setResult();
            }
        }
    }

    private void executeIfNeeded() {
        if ( (closed || counter >= bulkSize) && pendingCreateIndexRequests.get() == 0) {
            executeRequests();
        }
    }

    private synchronized void executeRequests() {
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            ShardId shardId= entry.getKey();
            List<BulkItemRequest> items = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    shardId.index().name(),
                    shardId.id(),
                    false,
                    items.toArray(new BulkItemRequest[items.size()]));

            execute(bulkShardRequest);
        }
        counter = 0;
        requestsByShard.clear();
    }

    private void execute(BulkShardRequest bulkShardRequest) {
        blockIfRetriesActive("execute single request");
        trace(String.format("execute shard request %d", bulkShardRequest.shardId()));
        transportShardBulkAction.execute(bulkShardRequest, new ResponseListener(bulkShardRequest));
    }

    private void doRetry(BulkShardRequest originalRequest, @Nullable IntArrayList toRetry) {
        trace("doRetry");
        executeWithRetry(createRetryRequest(originalRequest, toRetry));
    }

    private void executeWithRetry(BulkShardRequest request) {
        if (activeRetries.getAndIncrement() > 0) {
            try {
                trace("doRetry but other retries are pending.. semaphore acquire");
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
        transportShardBulkAction.execute(request, new RetryResponseListener(request));
    }

    private BulkShardRequest createRetryRequest(BulkShardRequest originalRequest, @Nullable IntArrayList toRetry) {
        if (toRetry == null) {
            return originalRequest;
        }

        BulkItemRequest[] items = new BulkItemRequest[toRetry.size()];
        int newIdx = 0;
        for (IntCursor intCursor : toRetry) {
            items[intCursor.index] = new BulkItemRequest(
                    newIdx,
                    originalRequest.items()[intCursor.value].request());
            newIdx++;
        }
        return new BulkShardRequest(
                originalRequest.index(),
                originalRequest.shardId(),
                false,
                items
        );
    }

    private void createIndexIfRequired(final String indexName, final BytesReference source, final String id, final String routing) {

        if (!indexCreated(indexName) || autoCreateIndex.shouldAutoCreate(indexName, clusterService.state())) {
            pendingCreateIndexRequests.incrementAndGet();
            transportCreateIndexAction.execute(new CreateIndexRequest(indexName).cause("bulkShardProcessor"),
                    new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            onCreatedOrExistsAlready();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            e = ExceptionsHelper.unwrapCause(e);
                            if (e instanceof IndexAlreadyExistsException) {
                                // copy from with multiple readers might attempt to create the index
                                // multiple times
                                // can be ignored.
                                logger.info("copy from index {}", e.getMessage());
                                onCreatedOrExistsAlready();
                            } else {
                                setFailure(e);
                            }
                        }

                        private void onCreatedOrExistsAlready() {
                            synchronized (createIndexLock) {
                                indicesCreated.add(indexName);
                            }
                            partitionRequestByShard(indexName, source, id, routing);
                            if (pendingCreateIndexRequests.decrementAndGet() == 0) {
                                executeIfNeeded();
                            }
                        }
                    });
        } else {
            partitionRequestByShard(indexName, source, id, routing);
            executeIfNeeded();
        }
    }

    private boolean indexCreated(String indexName) {
        synchronized (createIndexLock) {
            return indicesCreated.contains(indexName);
        }
    }

    class ResponseListener implements ActionListener<BulkShardResponse> {

        protected final BulkShardRequest bulkShardRequest;

        public ResponseListener(BulkShardRequest bulkShardRequest) {
            this.bulkShardRequest = bulkShardRequest;
        }

        @Override
        public void onResponse(BulkShardResponse bulkShardResponse) {
            trace("execute response");
            IntArrayList toRetry = new IntArrayList();
            int successes = 0;
            for (BulkItemResponse itemResponse : bulkShardResponse.getResponses()) {
                if (itemResponse.isFailed()) {
                    if (itemResponse.getFailureMessage().startsWith("EsRejectedExecution")) {
                        logger.warn("{}, retrying", itemResponse.getFailureMessage());
                        toRetry.add(itemResponse.getItemId());
                    } else {
                        setFailure(new RuntimeException(itemResponse.getFailureMessage()));
                    }
                } else {
                    successes++;
                }
            }
            rowsInserted.add(successes);

            if (!toRetry.isEmpty()) {
                doRetry(bulkShardRequest, toRetry);
            } else {
                setResultIfDone(successes);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            trace("execute failure");
            if (e instanceof EsRejectedExecutionException) {
                logger.warn("{}, retrying", e.getMessage());
                doRetry(bulkShardRequest, null);
            } else {
                setFailure(e);
            }
        }
    }

    private void trace(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace("BulkShardProcessor: pending: {}; active retries: {} - {}",
                    pending.get(), activeRetries.get(), message);
        }
    }

    class RetryResponseListener extends ResponseListener {

        public RetryResponseListener(BulkShardRequest bulkShardRequest) {
            super(bulkShardRequest);
        }

        @Override
        public void onResponse(BulkShardResponse bulkShardResponse) {
            activeRetries.decrementAndGet();
            semaphore.release();
            trace("BulkShardProcessor retry success");
            super.onResponse(bulkShardResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            activeRetries.decrementAndGet();
            semaphore.release();
            trace("BulkShardProcessor retry failure");
            super.onFailure(e);
        }
    }
}
