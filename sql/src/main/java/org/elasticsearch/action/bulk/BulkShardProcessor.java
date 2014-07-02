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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import jsr166e.LongAdder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;

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

    private final static AtomicInteger currentDelay = new AtomicInteger(0);

    private final ClusterService clusterService;
    private final TransportShardBulkAction transportShardBulkAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final boolean autoCreateIndices;
    private final int bulkSize;
    private final Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
    private final AutoCreateIndex autoCreateIndex;
    private int counter;
    private final Semaphore semaphore = new Semaphore(1);
    private final SettableFuture<Long> result;
    private final LongAdder rowsInserted = new LongAdder();
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicInteger activeRetries = new AtomicInteger(0);
    private final AtomicInteger blockedAdds = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed = false;
    private final Set<String> indicesCreated = new HashSet<>();
    private final ThreadPool threadPool;

    private final ESLogger logger = Loggers.getLogger(getClass());

    public BulkShardProcessor(ThreadPool threadPool,
                              ClusterService clusterService,
                              Settings settings,
                              TransportShardBulkAction transportShardBulkAction,
                              TransportCreateIndexAction transportCreateIndexAction,
                              boolean autoCreateIndices,
                              int bulkSize) {
        this.threadPool = threadPool;
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
        blockIfRetriesActive();
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        if (autoCreateIndices) {
            createIndexIfRequired(indexName);
        }
        partitionRequestByShard(indexName, source, id, routing);
        executeIfNeeded();
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

    private void blockIfRetriesActive() {
        if (activeRetries.get() > 0) {
            try {
                trace(String.format("add with active retries, acquiring semaphore: %s", semaphore));
                blockedAdds.getAndIncrement();
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
        if (closed || counter >= bulkSize) {
            executeRequests();
        }
    }

    private synchronized void executeRequests() {
        for (Iterator<Map.Entry<ShardId, List<BulkItemRequest>>> it = requestsByShard.entrySet().iterator(); it.hasNext();) {
            Map.Entry<ShardId, List<BulkItemRequest>> entry = it.next();
            ShardId shardId= entry.getKey();
            List<BulkItemRequest> items = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    shardId.index().name(),
                    shardId.id(),
                    false,
                    items.toArray(new BulkItemRequest[items.size()]));

            execute(bulkShardRequest);
            it.remove();
        }
        counter = 0;
    }

    private void execute(BulkShardRequest bulkShardRequest) {
        trace(String.format("execute shard request %d", bulkShardRequest.shardId()));
        transportShardBulkAction.execute(bulkShardRequest, new ResponseListener(bulkShardRequest));
    }

    private void doRetry(BulkShardRequest originalRequest) {
        trace("doRetry");
        executeWithRetry(originalRequest);
    }

    private void executeWithRetry(final BulkShardRequest request) {
        activeRetries.getAndIncrement();
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        threadPool.schedule(TimeValue.timeValueMillis(currentDelay.getAndIncrement() * 10),
                ThreadPool.Names.SAME, new Runnable() {
                    @Override
                    public void run() {
                        transportShardBulkAction.execute(request, new RetryResponseListener(request));
                    }
                });
    }

    private void createIndexIfRequired(final String indexName) {
        if (!indicesCreated.contains(indexName) || autoCreateIndex.shouldAutoCreate(indexName, clusterService.state())) {
            try {
                transportCreateIndexAction.execute(new CreateIndexRequest(indexName).cause("bulkShardProcessor")).actionGet();
                indicesCreated.add(indexName);
            } catch (ElasticsearchException e) {
                if (e instanceof IndexAlreadyExistsException) {
                    // copy from with multiple readers might attempt to create the index
                    // multiple times
                    // can be ignored.
                    if (logger.isDebugEnabled()) {
                        logger.debug("copy from index {}", e.getMessage());
                    }
                    indicesCreated.add(indexName);
                } else {
                    setFailure(e);
                }
            }
        }
    }

    private void processResponse(BulkShardResponse bulkShardResponse) {
        trace("execute response");
        int successes = 0;
        for (BulkItemResponse itemResponse : bulkShardResponse.getResponses()) {
            if (itemResponse.isFailed()) {
                setFailure(new RuntimeException(itemResponse.getFailureMessage()));
            } else {
                successes++;
            }
        }
        rowsInserted.add(successes);
        setResultIfDone(successes);
    }

    private void processFailure(Throwable e, BulkShardRequest bulkShardRequest) {
        trace("execute failure");
        if (e instanceof EsRejectedExecutionException) {
            logger.warn("{}, retrying", e.getMessage());
            doRetry(bulkShardRequest);
        } else {
            setFailure(e);
        }
    }

    class ResponseListener implements ActionListener<BulkShardResponse> {

        protected final BulkShardRequest bulkShardRequest;

        public ResponseListener(BulkShardRequest bulkShardRequest) {
            this.bulkShardRequest = bulkShardRequest;
        }

        @Override
        public void onResponse(BulkShardResponse bulkShardResponse) {
            if (blockedAdds.get() > 0) {
                blockedAdds.decrementAndGet();
                semaphore.release();
            }
            processResponse(bulkShardResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            if (blockedAdds.get() > 0) {
                blockedAdds.decrementAndGet();
                semaphore.release();
            }
            processFailure(e, bulkShardRequest);
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
            trace("BulkShardProcessor retry success");
            if (activeRetries.decrementAndGet() == 0) {
                currentDelay.set(0);
            }
            semaphore.release();
            processResponse(bulkShardResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            trace("BulkShardProcessor retry failure");
            if (activeRetries.decrementAndGet() == 0) {
                currentDelay.set(0);
            }
            semaphore.release();
            processFailure(e, bulkShardRequest);
        }
    }
}
