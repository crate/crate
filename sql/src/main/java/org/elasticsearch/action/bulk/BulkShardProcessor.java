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

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
 * the {@link #add} method will start to block.
 */
public class BulkShardProcessor {

    private static final ESLogger LOGGER = Loggers.getLogger(BulkShardProcessor.class);

    private final boolean autoCreateIndices;
    private final int bulkSize;

    private final Map<ShardId, ShardUpsertRequest> requestsByShard = new HashMap<>();
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicInteger pending = new AtomicInteger(0);
    private final Semaphore executeLock = new Semaphore(1);

    private final SettableFuture<BitSet> result;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final BitSet responses;
    private final Object responsesLock = new Object();
    private final boolean overwriteDuplicates;
    private volatile boolean closed = false;

    private final ClusterService clusterService;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final AutoCreateIndex autoCreateIndex;
    private final Set<String> indicesCreated = new HashSet<>();

    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final TimeValue requestTimeout;

    private final boolean continueOnErrors;

    @Nullable
    private Reference[] missingAssignmentsColumns;
    @Nullable
    private String[] assignmentsColumns;

    private final ResponseListener responseListener = new ResponseListener(false);
    private final ResponseListener retryResponseListener = new ResponseListener(true);


    public BulkShardProcessor(ClusterService clusterService,
                              Settings settings,
                              TransportCreateIndexAction transportCreateIndexAction,
                              boolean autoCreateIndices,
                              boolean overwriteDuplicates,
                              int bulkSize,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              boolean continueOnErrors,
                              @Nullable String[] assignmentsColumns,
                              @Nullable Reference[] missingAssignmentsColumns) {
        this.overwriteDuplicates = overwriteDuplicates;
        assert assignmentsColumns != null | missingAssignmentsColumns != null;
        this.clusterService = clusterService;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.autoCreateIndices = autoCreateIndices;
        this.bulkSize = bulkSize;
        this.continueOnErrors = continueOnErrors;
        this.assignmentsColumns = assignmentsColumns;
        this.missingAssignmentsColumns = missingAssignmentsColumns;
        responses = new BitSet();
        result = SettableFuture.create();
        autoCreateIndex = new AutoCreateIndex(settings);
        requestTimeout = settings.getAsTime("insert_by_query.request_timeout", BulkShardRequest.DEFAULT_TIMEOUT);
    }

    public boolean add(String indexName,
                       String id,
                       @Nullable Symbol[] assignments,
                       @Nullable Object[] missingAssignments,
                       @Nullable String routing,
                       @Nullable Long version) {
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

        ShardId shardId = shardId(indexName, id, routing);
        try {
            bulkRetryCoordinatorPool.coordinator(shardId).retryLock().acquireReadLock();
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Throwable e) {
            setFailure(e);
            return false;
        }

        partitionRequestByShard(shardId, id, assignments, missingAssignments, routing, version);
        executeIfNeeded();
        return true;
    }

    public boolean addForExistingShard(ShardId shardId,
                                       String id,
                                       @Nullable Symbol[] assignments,
                                       @Nullable Object[] missingAssignments,
                                       @Nullable String routing,
                                       @Nullable Long version) {

        assert id != null : "id must not be null";
        pending.incrementAndGet();
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        try {
            bulkRetryCoordinatorPool.coordinator(shardId).retryLock().acquireReadLock();
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Throwable e) {
            setFailure(e);
            return false;
        }

        partitionRequestByShard(shardId, id, assignments, missingAssignments, routing, version);
        executeIfNeeded();
        return true;
    }


    private ShardId shardId(String indexName, String id, @Nullable String routing) {
        return clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                Constants.DEFAULT_MAPPING_TYPE,
                id,
                routing
        ).shardId();
    }

    private void partitionRequestByShard(ShardId shardId,
                                         String id,
                                         @Nullable Symbol[] assignments,
                                         @Nullable Object[] missingAssignments,
                                         @Nullable String routing,
                                         @Nullable Long version) {
        try {
            executeLock.acquire();
            ShardUpsertRequest updateRequest = requestsByShard.get(shardId);
            if (updateRequest == null) {
                updateRequest = new ShardUpsertRequest(shardId, assignmentsColumns, missingAssignmentsColumns);
                updateRequest.timeout(requestTimeout);
                updateRequest.continueOnError(continueOnErrors);
                updateRequest.overwriteDuplicates(overwriteDuplicates);
                requestsByShard.put(shardId, updateRequest);
            }
            counter.getAndIncrement();
            updateRequest.add(globalCounter.getAndIncrement(), id, assignments, missingAssignments, version, routing);
        } catch (InterruptedException e) {
            Thread.interrupted();
        } finally {
            executeLock.release();
        }
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
            for (Iterator<Map.Entry<ShardId, ShardUpsertRequest>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<ShardId, ShardUpsertRequest> entry = it.next();
                ShardUpsertRequest shardUpsertRequest = entry.getValue();
                BulkRetryCoordinator coordinator = bulkRetryCoordinatorPool.coordinator(entry.getKey());
                coordinator.execute(shardUpsertRequest, responseListener);
                it.remove();
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Throwable t) {
            setFailure(t);
        } finally {
            counter.set(0);
            executeLock.release();
        }
    }

    private void createIndexIfRequired(final String indexName) {
        if (!indicesCreated.contains(indexName) || autoCreateIndex.shouldAutoCreate(indexName, clusterService.state())) {
            try {
                transportCreateIndexAction.execute(new CreateIndexRequest(indexName).cause("bulkShardProcessor")).actionGet();
                indicesCreated.add(indexName);
            } catch (Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (e instanceof IndexAlreadyExistsException) {
                    // copy from with multiple readers might attempt to create the index
                    // multiple times
                    // can be ignored.
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("copy from index {}", e.getMessage());
                    }
                    indicesCreated.add(indexName);
                } else {
                    setFailure(e);
                }
            }
        }
    }

    private void processResponse(ShardUpsertResponse shardUpsertResponse) {
        trace("execute response");
        for (int i = 0; i < shardUpsertResponse.locations().size(); i++) {
            int location = shardUpsertResponse.locations().get(i);
            synchronized (responsesLock) {
                responses.set(location, shardUpsertResponse.responses().get(i) != null);
            }
        }
        setResultIfDone(shardUpsertResponse.locations().size());
    }

    private void processFailure(Throwable e, ShardUpsertRequest shardUpsertRequest, boolean repeatingRetry) {
        trace("execute failure");
        e = Exceptions.unwrap(e);
        ShardId shardId = new ShardId(shardUpsertRequest.index(), shardUpsertRequest.shardId());

        BulkRetryCoordinator coordinator;
        try {
            coordinator = bulkRetryCoordinatorPool.coordinator(shardId);
        } catch (Throwable t) {
            setFailure(t);
            return;
        }
        if (e instanceof EsRejectedExecutionException) {
            LOGGER.trace("{}, retrying", e.getMessage());
            coordinator.retry(shardUpsertRequest, repeatingRetry, retryResponseListener);
        } else {
            if (repeatingRetry) {
                // release failed retry
                try {
                    coordinator.retryLock().releaseWriteLock();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
            Iterator<IntCursor> it = shardUpsertRequest.locations().iterator();
            while (it.hasNext()) {
                synchronized (responsesLock) {
                    responses.set(it.next().value, false);
                }
            }
            setFailure(e);
        }
    }

    private void trace(String message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("BulkShardProcessor: pending: {}; {}",
                    pending.get(), message);
        }
    }

    class ResponseListener implements BulkRetryCoordinator.RequestActionListener<ShardUpsertRequest, ShardUpsertResponse> {

        private final boolean retryListener;

        private ResponseListener(boolean retryListener) {
            this.retryListener = retryListener;
        }

        @Override
        public void onResponse(ShardUpsertRequest shardUpsertRequest,
                               ShardUpsertResponse shardUpsertResponse) {
            processResponse(shardUpsertResponse);
        }

        @Override
        public void onFailure(ShardUpsertRequest shardUpsertRequest, Throwable e) {
            processFailure(e, shardUpsertRequest, retryListener);
        }
    }

}
