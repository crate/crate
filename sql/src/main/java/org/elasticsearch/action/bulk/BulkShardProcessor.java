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
import io.crate.exceptions.Exceptions;
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

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Processor to do Bulk Inserts, similar to {@link org.elasticsearch.action.bulk.BulkProcessor}
 * but less flexible (only supports IndexRequests)
 *
 * If the Bulk threadPool Queue is full retries are made and
 * the {@link #add(String, org.elasticsearch.common.bytes.BytesReference, String, String)} method will start to block.
 */
public class BulkShardProcessor {

    private final ClusterService clusterService;
    private final TransportShardBulkActionDelegate transportShardBulkActionDelegate;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final boolean autoCreateIndices;
    private final boolean allowCreateOnly;
    private final int bulkSize;
    private final Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
    private final AutoCreateIndex autoCreateIndex;
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final SettableFuture<BitSet> result;
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicInteger currentDelay = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final BitSet responses;
    private final Object responsesLock = new Object();
    private volatile boolean closed = false;
    private final Set<String> indicesCreated = new HashSet<>();
    private final ReadWriteLock retryLock = new ReadWriteLock();
    private final Semaphore executeLock = new Semaphore(1);
    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newScheduledThreadPool(1, daemonThreadFactory("bulkShardProcessor"));
    private final TimeValue requestTimeout;

    private final ESLogger logger = Loggers.getLogger(getClass());

    public BulkShardProcessor(ClusterService clusterService,
                              Settings settings,
                              TransportShardBulkActionDelegate transportShardBulkActionDelegate,
                              TransportCreateIndexAction transportCreateIndexAction,
                              boolean autoCreateIndices,
                              boolean allowCreateOnly,
                              int bulkSize) {
        this.clusterService = clusterService;
        this.transportShardBulkActionDelegate = transportShardBulkActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.autoCreateIndices = autoCreateIndices;
        this.allowCreateOnly = allowCreateOnly;
        this.bulkSize = bulkSize;
        responses = new BitSet();
        result = SettableFuture.create();
        autoCreateIndex = new AutoCreateIndex(settings);
        requestTimeout = settings.getAsTime("insert_by_query.request_timeout", BulkShardRequest.DEFAULT_TIMEOUT);
    }

    public boolean add(String indexName, BytesReference source, String id, @Nullable String routing) {
        assert id != null : "id must not be null";
        pending.incrementAndGet();
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        if (autoCreateIndices) {
            createIndexIfRequired(indexName);
        }

        // will only block if retries/writer are active
        try {
            retryLock.readLock();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }

        partitionRequestByShard(indexName, source, id, routing);
        executeIfNeeded();
        return true;
    }

    private void partitionRequestByShard(String indexName, BytesReference source, String id, @Nullable String routing) {
        ShardId shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                Constants.DEFAULT_MAPPING_TYPE,
                id,
                routing
        ).shardId();

        IndexRequest indexRequest = new IndexRequest(indexName, Constants.DEFAULT_MAPPING_TYPE, id);
        if (routing != null) {
            indexRequest.routing(routing);
        }
        indexRequest.source(source, false);
        indexRequest.timestamp(Long.toString(System.currentTimeMillis()));
        indexRequest.create(allowCreateOnly);

        try {
            executeLock.acquire();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        List<BulkItemRequest> items = requestsByShard.get(shardId);
        if (items == null) {
            items = new ArrayList<>();
            requestsByShard.put(shardId, items);
        }
        counter.getAndIncrement();
        items.add(new BulkItemRequest(globalCounter.getAndIncrement(), indexRequest));
        executeLock.release();
    }

    public ListenableFuture<BitSet> result() {
        return result;
    }

    public void close() {
        trace("close");
        closed = true;
        executeRequests();
        if (pending.get() == 0) {
            setResult();
        }
        scheduledExecutorService.shutdown();
    }

    private void setFailure(Throwable e) {
        failure.compareAndSet(null, e);
        result.setException(e);
    }

    private void setResult() {
        Throwable throwable = failure.get();
        if (throwable == null) {
            result.set(responses);
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
        if (closed || counter.get() >= bulkSize) {
            executeRequests();
        }
    }

    private void executeRequests() {
        try {
            executeLock.acquire();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        for (Iterator<Map.Entry<ShardId, List<BulkItemRequest>>> it = requestsByShard.entrySet().iterator(); it.hasNext();) {
            Map.Entry<ShardId, List<BulkItemRequest>> entry = it.next();
            ShardId shardId= entry.getKey();
            List<BulkItemRequest> items = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    new BulkRequest(),
                    shardId.index().name(),
                    shardId.id(),
                    false,
                    items.toArray(new BulkItemRequest[items.size()]));
            bulkShardRequest.timeout(requestTimeout);
            execute(bulkShardRequest);
            it.remove();
        }
        counter.set(0);
        executeLock.release();
    }

    private void execute(BulkShardRequest bulkShardRequest) {
        trace(String.format("execute shard request %d", bulkShardRequest.shardId()));
        transportShardBulkActionDelegate.execute(bulkShardRequest, new ResponseListener(bulkShardRequest));
    }

    private void doRetry(final BulkShardRequest request, final boolean repeatingRetry) {
        trace("doRetry");
        if (repeatingRetry) {
            try {
                Thread.sleep(currentDelay.incrementAndGet());
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            transportShardBulkActionDelegate.execute(request, new RetryResponseListener(request));
        } else {
            // new retries will be spawned in new thread because they can block
            scheduledExecutorService.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            // will block if other retries/writer are active
                            try {
                                retryLock.writeLock();
                            } catch (InterruptedException e) {
                                Thread.interrupted();
                            }
                            transportShardBulkActionDelegate.execute(request, new RetryResponseListener(request));
                        }
                    }, currentDelay.getAndIncrement(), TimeUnit.MILLISECONDS);
        }
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
        for (BulkItemResponse itemResponse : bulkShardResponse.getResponses()) {
            synchronized (responsesLock) {
                responses.set(itemResponse.getItemId(), !itemResponse.isFailed());
            }
        }
        setResultIfDone(bulkShardResponse.getResponses().length);
    }

    private void processFailure(Throwable e, BulkShardRequest bulkShardRequest, boolean repeatingRetry) {
        trace("execute failure");
        e = Exceptions.unwrap(e);
        if (e instanceof EsRejectedExecutionException) {
            logger.warn("{}, retrying", e.getMessage());
            doRetry(bulkShardRequest, repeatingRetry);
        } else {
            if (repeatingRetry) {
                // release failed retry
                try {
                    retryLock.writeUnlock();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
            for (BulkItemRequest bulkItemRequest : bulkShardRequest.items()) {
                synchronized (responsesLock) {
                    responses.set(bulkItemRequest.id(), false);
                }
            }
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
            processResponse(bulkShardResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            processFailure(e, bulkShardRequest, false);
        }
    }

    private void trace(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace("BulkShardProcessor: pending: {}; active retries: {} - {}",
                    pending.get(), retryLock.activeWriters(), message);
        }
    }

    class RetryResponseListener extends ResponseListener {

        public RetryResponseListener(BulkShardRequest bulkShardRequest) {
            super(bulkShardRequest);
        }

        @Override
        public void onResponse(BulkShardResponse bulkShardResponse) {
            trace("BulkShardProcessor retry success");
            currentDelay.set(0);
            try {
                retryLock.writeUnlock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            processResponse(bulkShardResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            trace("BulkShardProcessor retry failure");
            processFailure(e, bulkShardRequest, true);
        }
    }

    /**
     * A {@link Semaphore} based read/write lock allowing multiple readers,
     * no reader will block others, and only 1 active writer. Writers take
     * precedence over readers, a writer will block all readers.
     * Compared to a {@link ReadWriteLock}, no lock is owned by a thread.
     */
    class ReadWriteLock {
        private final Semaphore readLock = new Semaphore(1, true);
        private final Semaphore writeLock = new Semaphore(1, true);
        private final AtomicInteger activeWriters = new AtomicInteger(0);
        private final AtomicInteger waitingReaders = new AtomicInteger(0);

        public ReadWriteLock() {
        }

        public void writeLock() throws InterruptedException {
            // check readLock permits to prevent deadlocks
            if (activeWriters.getAndIncrement() == 0 && readLock.availablePermits() == 1) {
                // draining read permits, so all reads will block
                readLock.drainPermits();
            }
            writeLock.acquire();
        }

        public void writeUnlock() throws InterruptedException {
            if (activeWriters.decrementAndGet() == 0) {
                // unlock all readers
                readLock.release(waitingReaders.getAndSet(0)+1);
            }
            writeLock.release();
        }

        public void readLock() throws InterruptedException {
            // only acquire permit if writers are active
            if(activeWriters.get() > 0) {
                waitingReaders.getAndIncrement();
                readLock.acquire();
            }
        }

        public int activeWriters() {
            return activeWriters.get();
        }

    }

}
