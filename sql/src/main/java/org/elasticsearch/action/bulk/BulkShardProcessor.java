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
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.exceptions.Exceptions;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.collect.ShardingProjector;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processor to do Bulk Inserts, similar to {@link BulkProcessor}
 * but less flexible (only supports IndexRequests)
 *
 * If the Bulk threadPool Queue is full retries are made and
 * the {@link #add} method will start to block.
 */
public class BulkShardProcessor<Request extends BulkProcessorRequest, Response extends BulkProcessorResponse<?>> {

    private static final ESLogger LOGGER = Loggers.getLogger(BulkShardProcessor.class);
    public static final int MAX_CREATE_INDICES_BULK_SIZE = 100;

    private final Predicate<String> shouldAutocreateIndexPredicate;

    private final boolean autoCreateIndices;
    private final int bulkSize;
    private final int createIndicesBulkSize;

    private final Map<ShardId, Request> requestsByShard = new HashMap<>();
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    private final AtomicInteger counter = new AtomicInteger(0);
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
    private final ShardingProjector shardingProjector;


    private final BulkRequestBuilder<Request> bulkRequestBuilder;
    private final BulkRequestExecutor<Request, Response> bulkRequestExecutor;

    public BulkShardProcessor(ClusterService clusterService,
                              Settings settings,
                              TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                              ShardingProjector shardingProjector,
                              boolean autoCreateIndices,
                              int bulkSize,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              BulkRequestBuilder<Request> bulkRequestBuilder,
                              BulkRequestExecutor<Request, Response> bulkRequestExecutor) {
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.clusterService = clusterService;

        this.bulkRequestBuilder = bulkRequestBuilder;
        this.bulkRequestExecutor = bulkRequestExecutor;
        this.autoCreateIndices = autoCreateIndices;
        this.bulkSize = bulkSize;
        this.createIndicesBulkSize = Math.min(bulkSize, MAX_CREATE_INDICES_BULK_SIZE);

        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.shouldAutocreateIndexPredicate = new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                assert input != null;
                return autoCreateIndex.shouldAutoCreate(input, BulkShardProcessor.this.clusterService.state());
            }
        };
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.shardingProjector = shardingProjector;

        responses = new BitSet();
        result = SettableFuture.create();
    }

    public boolean add(String indexName,
                       Row row,
                       @Nullable Long version) {
        pending.incrementAndGet();
        Throwable throwable = failure.get();
        if (throwable != null) {
            result.setException(throwable);
            return false;
        }

        shardingProjector.setNextRow(row);
        ShardId shardId = shardId(indexName, shardingProjector.id(), shardingProjector.routing());
        if (shardId == null) {
            addRequestForNewIndex(indexName, shardingProjector.id(), row, shardingProjector.routing(), version);
        } else {
            try {
                bulkRetryCoordinatorPool.coordinator(shardId).retryLock().acquireReadLock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (Throwable e) {
                setFailure(e);
                return false;
            }
            partitionRequestByShard(shardId, shardingProjector.id(), row, shardingProjector.routing(), version);
        }
        executeIfNeeded();
        return true;
    }

    private void addRequestForNewIndex(String indexName, String id, Row row, @Nullable String routing, @Nullable Long version) throws IndexMissingException {
        synchronized (requestsForNewIndices) {
            List<PendingRequest> pendingRequestList = requestsForNewIndices.get(indexName);
            if (pendingRequestList == null) {
                pendingRequestList = new ArrayList<>();
                requestsForNewIndices.put(indexName, pendingRequestList);
            }
            pendingRequestList.add(new PendingRequest(indexName, id, row, routing, version));
            pendingNewIndexRequests.incrementAndGet();
        }
    }

    @Nullable
    private ShardId shardId(String indexName, String id, @Nullable String routing) {
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

    private void partitionRequestByShard(ShardId shardId,
                                         String id,
                                         Row row,
                                         @Nullable String routing,
                                         @Nullable Long version) {
        try {
            executeLock.acquire();
            Request request = requestsByShard.get(shardId);
            if (request == null) {
                request = bulkRequestBuilder.newRequest(shardId);
                requestsByShard.put(shardId, request);
            }
            counter.incrementAndGet();
            bulkRequestBuilder.addItem(
                    request,
                    shardId,
                    globalCounter.getAndIncrement(),
                    id,
                    row,
                    routing,
                    version);

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
        if ((closed || counter.get() >= bulkSize) && failure.get() == null) {
            executeRequests();
        }
    }

    private void executeRequests() {
        try {
            executeLock.acquire();

            for (Iterator<Map.Entry<ShardId, Request>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<ShardId, Request> entry = it.next();
                final Request shardRequest = entry.getValue();
                final ShardId shardId = entry.getKey();
                bulkRequestExecutor.execute(shardRequest, new ActionListener<Response>() {
                    @Override
                    public void onResponse(Response response) {
                        processResponse(response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        processFailure(e, shardId, shardRequest, false);
                    }
                });
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
            LOGGER.debug("create {} pending indices...", indices.size());
            TimeValue timeout = CrateSettings.BULK_PARTITION_CREATION_TIMEOUT.extractTimeValue(clusterService.state().metaData().settings());
            if (timeout.millis() == 0L) {
                // apply default
                // wait up to 10 seconds for every single create index request
                timeout = new TimeValue(indices.size() * 10L, TimeUnit.SECONDS);
            }
            final BulkCreateIndicesRequest bulkCreateIndicesRequest = new BulkCreateIndicesRequest(indices)
                    .timeout(timeout); // wait up to 10 seconds for every create index request
            FutureCallback<Void> indicesCreatedCallback = new FutureCallback<Void>() {
                @Override
                public void onSuccess(@Nullable Void result) {
                    RowN row = null;
                    for (PendingRequest pendingRequest : pendings) {
                        // add pending requests for created indices
                        ShardId shardId = shardId(pendingRequest.indexName,
                                pendingRequest.id, pendingRequest.routing);
                        if (row == null) {
                            row = new RowN(pendingRequest.row);
                        } else {
                            row.cells(pendingRequest.row);
                        }
                        partitionRequestByShard(shardId, pendingRequest.id, row,
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
                // shortcut
                indicesCreatedCallback.onSuccess(null);
            } else {
                // initialize callback for when all indices are created
                IndicesCreatedObserver.waitForIndicesCreated(clusterService, LOGGER, indices, indicesCreatedCallback, timeout);
                transportBulkCreateIndicesAction.execute(bulkCreateIndicesRequest, new ActionListener<BulkCreateIndicesResponse>() {
                    @Override
                    public void onResponse(BulkCreateIndicesResponse bulkCreateIndicesResponse) {
                        trace("%d of %d indices already created", bulkCreateIndicesResponse.alreadyExisted(), indices.size());
                        indicesCreated.addAll(indices);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        setFailure(ExceptionsHelper.unwrapCause(e));
                    }
                });
            }
        }

    }

    private void processResponse(Response response) {
        trace("execute response");

        for (int i = 0; i < response.itemIndices().size(); i++) {
            int location = response.itemIndices().get(i);
            synchronized (responsesLock) {
                responses.set(location, response.responses().get(i) != null);
            }
        }
        setResultIfDone(response.itemIndices().size());
    }

    private void processFailure(Throwable e, final ShardId shardId, final Request request, boolean repeatingRetry) {
        trace("execute failure");
        e = Exceptions.unwrap(e);
        BulkRetryCoordinator coordinator;
        try {
            coordinator = bulkRetryCoordinatorPool.coordinator(shardId);
        } catch (Throwable t) {
            setFailure(t);
            return;
        }
        if (e instanceof EsRejectedExecutionException) {
            LOGGER.trace("{}, retrying", e.getMessage());
            coordinator.retry(request, bulkRequestExecutor, repeatingRetry, new ActionListener<Response>() {

                @Override
                public void onFailure(Throwable e) {
                    processFailure(e, shardId, request, true);
                }

                @Override
                public void onResponse(Response response) {
                    processResponse(response);
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
            LOGGER.trace("BulkShardProcessor: pending: {}; {}",
                    pending.get(), String.format(Locale.ENGLISH, message, args));
        }
    }

    private static class PendingRequest {
        private final String indexName;
        private final String id;
        private final Object[] row;
        private final String routing;

        @Nullable
        private final Long version;

        PendingRequest(String indexName, String id, Row row, String routing, @Nullable Long version) {
            this.indexName = indexName;
            this.id = id;
            this.row = row.materialize();
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
                     Row row,
                     @Nullable String routing,
                     @Nullable Long version);
    }
}
