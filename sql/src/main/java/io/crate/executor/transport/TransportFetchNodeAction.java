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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.jobs.JobContextService;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.fetch.NodeFetchOperation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Singleton
public class TransportFetchNodeAction implements NodeAction<NodeFetchRequest, NodeFetchResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/fetch";
    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;
    private static final String RESPONSE_EXECUTOR = ThreadPool.Names.SAME;

    private final Transports transports;
    private final NodeFetchOperation nodeFetchOperation;

    @Inject
    public TransportFetchNodeAction(TransportService transportService,
                                    Transports transports,
                                    ThreadPool threadPool,
                                    JobsLogs jobsLogs,
                                    JobContextService jobContextService) {
        this.transports = transports;
        this.nodeFetchOperation = new NodeFetchOperation(threadPool, jobsLogs, jobContextService);

        transportService.registerRequestHandler(
            TRANSPORT_ACTION,
            NodeFetchRequest.class,
            EXECUTOR_NAME,
            // force execution because this handler might receive empty close requests which
            // need to be processed to not leak the FetchContext.
            // This shouldn't cause too much of an issue because fetch requests always happen after a query phase.
            // If the threadPool is overloaded the query phase would fail first.
            true,
            false,
            new NodeActionRequestHandler<NodeFetchRequest, NodeFetchResponse>(this) {}
        );
    }

    public void execute(String targetNode,
                        final IntObjectMap<Streamer[]> streamers,
                        final NodeFetchRequest request,
                        ActionListener<NodeFetchResponse> listener) {
        transports.sendRequest(TRANSPORT_ACTION, targetNode, request, listener,
            new DefaultTransportResponseHandler<NodeFetchResponse>(listener, RESPONSE_EXECUTOR) {
                @Override
                public NodeFetchResponse newInstance() {
                    return NodeFetchResponse.forReceiveing(streamers);
                }
            });
    }

    @Override
    public void nodeOperation(final NodeFetchRequest request,
                              final ActionListener<NodeFetchResponse> responseListener) {
        ListenableFuture<IntObjectMap<StreamBucket>> resultFuture = nodeFetchOperation.fetch(
            request.jobId(),
            request.fetchPhaseId(),
            request.toFetch(),
            request.isCloseContext()
        );
        Futures.addCallback(resultFuture, new FutureCallback<IntObjectMap<StreamBucket>>() {
            @Override
            public void onSuccess(@Nullable IntObjectMap<StreamBucket> result) {
                responseListener.onResponse(NodeFetchResponse.forSending(result));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                responseListener.onFailure(t);
            }
        });
    }
}
