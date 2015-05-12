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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.exceptions.Exceptions;
import io.crate.metadata.settings.CrateSettings;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesRequest;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processor to do Bulk Inserts, similar to {@link org.elasticsearch.action.bulk.BulkProcessor}
 * but less flexible (only supports IndexRequests)
 *
 * If the Bulk threadPool Queue is full retries are made and
 * the {@link #add} method will start to block.
 */
public class SymbolBasedBulkShardProcessor<Request extends BulkProcessorRequest, Response extends BulkProcessorResponse<?>> {

    public static final int MAX_CREATE_INDICES_BULK_SIZE = 100;

    private final boolean autoCreateIndices;
    private final Predicate<String> shouldAutocreateIndexPredicate;

    private final int bulkSize;
    private final int createIndicesBulkSize;

    private final Map<ShardId, Request> requestsByShard = new HashMap<>();
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    private final AtomicInteger requestItemCounter = new AtomicInteger(0);
    private final AtomicInteger pending = new AtomicInteger(0);
    private final Semaphore executeLock = new Semaphore(1);

    private final SettableFuture<BitSet> result;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final BitSet responses;
    private final Object responsesLock = new Object();
    private volatile boolean closed = false;

    private final ClusterService clusterService;
    private final TransportBulkCreateIndicesAction transportBulkCreateIndicesAction;
    private final AutoCreateIndex autoCreateIndex;

    private final AtomicInteger pendingNewIndexRequests = new AtomicInteger(0);
    private final Map<String, List<PendingRequest>> requestsForNewIndices = new HashMap<>();
    private final Set<String> indicesCreated = new HashSet<>();

    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;

    private final BulkRequestBuilder<Request> requestBuilder;
    private final BulkRequestExecutor<Request, Response> requestExecutor;

    private static final ESLogger LOGGER = Loggers.getLogger(SymbolBasedBulkShardProcessor.class);

    public SymbolBasedBulkShardProcessor(ClusterService clusterService,
                                         TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                                         Settings settings,
                                         BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                         boolean autoCreateIndices,
                                         int bulkSize,
                                         BulkRequestBuilder<Request> requestBuilder,
                                         BulkRequestExecutor<Request, Response> requestExecutor) {
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.clusterService = clusterService;
        this.autoCreateIndices = autoCreateIndices;
        this.bulkSize = bulkSize;
        this.createIndicesBulkSize = Math.min(bulkSize, MAX_CREATE_INDICES_BULK_SIZE);
        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.shouldAutocreateIndexPredicate = new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                assert input != null;
                return autoCreateIndex.shouldAutoCreate(input, SymbolBasedBulkShardProcessor.this.clusterService.state());
            }
        };


        responses = new BitSet();
        result = SettableFuture.create();

        this.requestExecutor = requestExecutor;
        this.requestBuilder = requestBuilder;
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

        ShardId shardId = shardId(indexName, id, routing);
        if (shardId == null) {
            addRequestForNewIndex(indexName, id, assignments, missingAssignments, routing, version);
        } else {
            try {
                // will only block if retries/writer are active
                bulkRetryCoordinatorPool.coordinator(shardId).retryLock().acquireReadLock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (Throwable e) {
                setFailure(e);
                return false;
            }
            partitionRequestByShard(shardId, id, assignments, missingAssignments, routing, version);
        }
        executeIfNeeded();
        return true;
    }

    public boolean add(String indexName,
                       String id,
                       Object[] missingAssignments,
                       @Nullable String routing,
                       @Nullable Long version) {
        return add(indexName, id, null, missingAssignments, routing, version);
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

        // will only block if retries/writer are active
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

    @Nullable
    private ShardId shardId(String indexName,
                            String id,
                            @Nullable String routing) {
        ShardId shardId = null;
        try {
            shardId = clusterService.operationRouting().indexShards(
                    clusterService.state(),
                    indexName,
                    Constants.DEFAULT_MAPPING_TYPE,
                    id,
                    routing
            ).shardId();
        } catch (IndexMissingException e) {
            if (!autoCreateIndices) {
                throw e;
            }
        }
        return shardId;
    }

    private void addRequestForNewIndex(String indexName,
                                       String id,
                                       @Nullable Symbol[] assignments,
                                       @Nullable Object[] missingAssignments,
                                       @Nullable String routing,
                                       @Nullable Long version) {
        synchronized (requestsForNewIndices) {
            List<PendingRequest> pendingRequestList = requestsForNewIndices.get(indexName);
            if (pendingRequestList == null) {
                pendingRequestList = new ArrayList<>();
                requestsForNewIndices.put(indexName, pendingRequestList);
            }
            pendingRequestList.add(new PendingRequest(indexName, id, assignments,
                    missingAssignments, routing, version));
            pendingNewIndexRequests.incrementAndGet();
        }
    }

    private void partitionRequestByShard(ShardId shardId,
                                         String id,
                                         @Nullable Symbol[] assignments,
                                         @Nullable Object[] missingAssignments,
                                         @Nullable String routing,
                                         @Nullable Long version) {
        try {
            executeLock.acquire();
            Request request = requestsByShard.get(shardId);
            if (request == null) {
                request = requestBuilder.newRequest(shardId);
                requestsByShard.put(shardId, request);
            }
            requestItemCounter.getAndIncrement();
            requestBuilder.addItem(
                    request,
                    shardId,
                    globalCounter.getAndIncrement(),
                    id,
                    assignments,
                    missingAssignments,
                    routing,
                    version
            );
        } catch (InterruptedException e) {
            Thread.interrupted();
        } finally {
            executeLock.release();
        }
    }

    private void executeRequests() {
        try {
            executeLock.acquire();
            for (Iterator<Map.Entry<ShardId, Request>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
                if (failure.get() != null) {
                    return;
                }
                Map.Entry<ShardId, Request> entry = it.next();
                final Request request = entry.getValue();
                final ShardId shardId = entry.getKey();
                requestExecutor.execute(request, new ActionListener<Response>() {
                    @Override
                    public void onResponse(Response response) {
                        processResponse(response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        processFailure(e, shardId, request, false);
                    }
                });
                it.remove();
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Throwable e) {
            setFailure(e);
        } finally {
            requestItemCounter.set(0);
            executeLock.release();
        }
    }

    private void createPendingIndices() {
        final List<PendingRequest> pendings = new ArrayList<>();
        final Set<String> indices;

        synchronized (requestsForNewIndices) {
            indices = ImmutableSet.copyOf(
                    Iterables.filter(
                            Sets.difference(requestsForNewIndices.keySet(), indicesCreated),
                            shouldAutocreateIndexPredicate)
            );
            for (Map.Entry<String, List<PendingRequest>> entry : requestsForNewIndices.entrySet()) {
                pendings.addAll(entry.getValue());
            }
            requestsForNewIndices.clear();
            pendingNewIndexRequests.set(0);
        }


        if (pendings.size() > 0 || indices.size() > 0) {
            LOGGER.debug("create {} pending indices in bulk...", indices.size());
            TimeValue timeout = CrateSettings.BULK_PARTITION_CREATION_TIMEOUT.extractTimeValue(clusterService.state().metaData().settings());
            if (timeout.millis() == 0L) {
                // apply default
                // wait up to 10 seconds for every single create index request
                timeout = new TimeValue(indices.size() * 10L, TimeUnit.SECONDS);
            }
            BulkCreateIndicesRequest bulkCreateIndicesRequest = new BulkCreateIndicesRequest(indices)
                    .ignoreExisting(true)
                    .timeout(timeout);

            FutureCallback<Void> indicesCreatedCallback = new FutureCallback<Void>() {
                @Override
                public void onSuccess(@Nullable Void result) {
                    if (failure.get() != null) {
                        return;
                    }
                    trace("applying pending requests for created indices...");
                    for (final PendingRequest pendingRequest : pendings) {
                        // add pending requests for created indices
                        ShardId shardId = shardId(pendingRequest.indexName, pendingRequest.id, pendingRequest.routing);
                        partitionRequestByShard(shardId, pendingRequest.id,
                                pendingRequest.assignments,
                                pendingRequest.missingAssignments,
                                pendingRequest.routing, pendingRequest.version);
                    }
                    trace("added %d pending requests, lets see if we can execute them", pendings.size());
                    executeRequestsIfNeeded();
                }

                @Override
                public void onFailure(Throwable t) {
                    setFailure(t);
                }
            };

            if (indices.isEmpty()) {
                indicesCreatedCallback.onSuccess(null);
            } else {
                // initialize callback for when all indices are created
                IndicesCreatedObserver.waitForIndicesCreated(clusterService, LOGGER, indices, indicesCreatedCallback, timeout);
                // initiate the request
                transportBulkCreateIndicesAction.execute(bulkCreateIndicesRequest, new ActionListener<BulkCreateIndicesResponse>() {
                    @Override
                    public void onResponse(BulkCreateIndicesResponse response) {
                        trace("%d of %d indices already created", response.alreadyExisted().size(), indices.size());
                        indicesCreated.addAll(indices);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOGGER.error("error when creating pending indices in bulk", t);
                        setFailure(ExceptionsHelper.unwrapCause(t));
                    }
                });
            }
        }

    }


    public ListenableFuture<BitSet> result() {
        return result;
    }

    public void close() {
        trace("close");
        closed = true;
        executeIfNeeded();
        if (pending.get() == 0) {
            setResult();
        }
    }

    public void kill() {
        failure.compareAndSet(null, new CancellationException());
        result.cancel(true);
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
        if (pending.addAndGet(-successes) == 0 && closed) {
            setResult();
        }
    }

    private void executeIfNeeded() {
        if ((closed
                || requestsForNewIndices.size() >= createIndicesBulkSize
                || pendingNewIndexRequests.get() >= bulkSize) && failure.get() == null) {
            createPendingIndices();
        }
        executeRequestsIfNeeded();
    }

    private void executeRequestsIfNeeded() {
        if ((closed || requestItemCounter.get() >= bulkSize) && failure.get() == null) {
            executeRequests();
        }
    }

    private void processResponse(Response response) {
        trace("executing response...");
        for (int i = 0; i < response.itemIndices().size(); i++) {
            int location = response.itemIndices().get(i);
            synchronized (responsesLock) {
                responses.set(location, response.responses().get(i) != null);
            }
        }
        setResultIfDone(response.itemIndices().size());
        trace("response executed.");
    }

    private void processFailure(Throwable e, final ShardId shardId, final Request request, boolean repeatingRetry) {
        trace("execute failure");
        e = Exceptions.unwrap(e);
        BulkRetryCoordinator coordinator;
        try {
            coordinator = bulkRetryCoordinatorPool.coordinator(shardId);
        } catch (Throwable coordinatorException) {
            setFailure(coordinatorException);
            return;
        }
        if (e instanceof EsRejectedExecutionException) {
            LOGGER.trace("{}, retrying", e.getMessage());
            coordinator.retry(request, requestExecutor, repeatingRetry, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    processResponse(response);
                }

                @Override
                public void onFailure(Throwable e) {
                    processFailure(e, shardId, request, true);
                }
            });
        } else {
            if (repeatingRetry) {
                // release failed retry
                try {
                    coordinator.retryLock().releaseWriteLock();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
            for (IntCursor intCursor : request.itemIndices()) {
                synchronized (responsesLock) {
                    responses.set(intCursor.value, false);
                }
            }
            setFailure(e);
        }
    }

    private void trace(String message, Object ... args) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("SymbolBasedBulkShardProcessor: pending: {}; {}",
                    pending.get(),
                    String.format(Locale.ENGLISH, message, args));
        }
    }

    static class PendingRequest {
        private final String indexName;
        private final String id;
        @Nullable
        private final Symbol[] assignments;
        @Nullable
        private final Object[] missingAssignments;
        @Nullable
        private final String routing;
        @Nullable
        private final Long version;


        PendingRequest(String indexName,
                       String id,
                       @Nullable Symbol[] assignments,
                       @Nullable Object[] missingAssignments,
                       @Nullable String routing,
                       @Nullable Long version) {
            this.indexName = indexName;
            this.id = id;
            this.assignments = assignments;
            this.missingAssignments = missingAssignments;
            this.routing = routing;
            this.version = version;
        }
    }

    public interface BulkRequestBuilder<Request extends BulkProcessorRequest> {
        Request newRequest(ShardId shardId);

        void addItem(Request existingRequest,
                     ShardId shardId,
                     int location,
                     String id,
                     @Nullable Symbol[] assignments,
                     @Nullable Object[] missingAssignments,
                     @Nullable String routing,
                     @Nullable Long version);
    }


}
