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

package org.elasticsearch.action.admin.indices.create;

import io.crate.core.collections.UniqueBlockingQueue;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * create indices in bulk on the master node with just one request sent
 *
 * this action enqueues requests
 */
@Singleton
public class TransportBulkCreateIndicesAction
        extends TransportMasterNodeOperationAction<BulkCreateIndicesRequest, BulkCreateIndicesResponse> {

    public static final int QUEUE_SIZE = 100;
    public static final String NAME = "indices:admin/bulk_create";

    private final UniqueBlockingQueue<RequestItem> requestQueue;
    private final MetaDataCreateIndexService createIndexService;
    private final Future<?> queuedRequestExecutorFuture;

    @Inject
    protected TransportBulkCreateIndicesAction(Settings settings,
                                               TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               MetaDataCreateIndexService createIndexService,
                                               ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, threadPool, actionFilters);
        this.requestQueue = new UniqueBlockingQueue<>(QUEUE_SIZE);
        this.createIndexService = createIndexService;
        // start queued Request executor
        queuedRequestExecutorFuture = ((ThreadPoolExecutor) threadPool.executor(executor())).submit(new QueuedRequestExecutor());
    }

    public void doStop() {
        queuedRequestExecutorFuture.cancel(true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected BulkCreateIndicesRequest newRequest() {
        return new BulkCreateIndicesRequest();
    }

    @Override
    protected BulkCreateIndicesResponse newResponse() {
        return new BulkCreateIndicesResponse();
    }

    @Override
    protected void masterOperation(final BulkCreateIndicesRequest request, ClusterState state, final ActionListener<BulkCreateIndicesResponse> listener) throws ElasticsearchException {
        final List<CreateIndexResponse> responses = new ArrayList<>(request.requests().size());
        final BulkCreateIndicesResponse finalResponse = new BulkCreateIndicesResponse(responses);

        // shortcut
        if (request.requests().isEmpty()) {
            listener.onResponse(finalResponse);
            return;
        }

        final AtomicInteger pending = new AtomicInteger(0);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> actionListener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                responses.add(new CreateIndexResponse(response.isAcknowledged()));
                countDownAndFinish();
            }

            private void countDownAndFinish() {
                if (pending.decrementAndGet() == 0) {
                    Throwable e = exception.get();
                    logger.trace("sending response");
                    if (e == null) {
                        listener.onResponse(finalResponse);
                    } else {
                        listener.onFailure(e);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof IndexAlreadyExistsException && request.ignoreExisting()) {
                    String index = ((IndexAlreadyExistsException) t).index().getName();
                    finalResponse.addAlreadyExisted(index);
                    logger.trace("[{}] index already exists", t, index);
                } else {
                    logger.debug("failed to create index in bulk, abort", t);
                    exception.compareAndSet(null, t);
                }
                countDownAndFinish();
            }
        };


        for (CreateIndexRequest createIndexRequest : request.requests()) {
            try {
                RequestItem item = new RequestItem(createIndexRequest, threadPool);
                if (requestQueue.put(item)) {
                    item.future.addListener(actionListener);
                    pending.incrementAndGet();
                } else {
                    String index = item.request.index();
                    finalResponse.addAlreadyExisted(index);
                    logger.trace("index [{}] already in the queue", index);
                }
            } catch (InterruptedException e) {
                logger.debug("got interrupted while adding create index request to the queue");
                Thread.interrupted();
                listener.onFailure(e);
                return;
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(BulkCreateIndicesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    private static class RequestItem {
        private final CreateIndexRequest request;
        private final PlainListenableActionFuture<ClusterStateUpdateResponse> future;

        private RequestItem(CreateIndexRequest request, ThreadPool threadPool) {
            this.request = request;
            // REALLY REALLY important to run this threaded, or else we will hog
            // management threads
            this.future = new PlainListenableActionFuture<>(true, threadPool);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestItem that = (RequestItem) o;

            return request.index().equals(that.request.index());
        }

        @Override
        public int hashCode() {
            return request.index().hashCode();
        }
    }

    private class QueuedRequestExecutor implements Runnable, ActionListener<ClusterStateUpdateResponse> {
        @Override
        public void run() {
            processARequest();
        }

        private void processARequest() {
            try {
                RequestItem item = requestQueue.take();
                logger.trace("dequeued create request for index {}", item.request.index());
                final CreateIndexClusterStateUpdateRequest updateRequest
                        = new CreateIndexClusterStateUpdateRequest(item.request, "bulk", item.request.index())
                        .ackTimeout(item.request.timeout()).masterNodeTimeout(item.request.masterNodeTimeout())
                        .settings(item.request.settings()).mappings(item.request.mappings())
                        .aliases(item.request.aliases()).customs(item.request.customs());
                item.future.addListener(this);
                createIndexService.createIndex(updateRequest, item.future);
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (Throwable e) {
                logger.trace("error on starting to create index", e);
            }
        }

        @Override
        public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
            logger.trace("successfully executed create index request");
            processARequest(); // take next request
        }

        @Override
        public void onFailure(Throwable e) {
            processARequest(); // take next request
        }
    }
}
