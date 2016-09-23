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
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.Exceptions;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.fetch.FetchContext;
import io.crate.operation.fetch.NodeFetchOperation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;

@Singleton
public class TransportFetchNodeAction implements NodeAction<NodeFetchRequest, NodeFetchResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/fetch";
    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;
    private static final String RESPONSE_EXECUTOR = ThreadPool.Names.SAME;

    private final Transports transports;
    private final StatsTables statsTables;
    private final NodeFetchOperation nodeFetchOperation;
    private final CircuitBreaker circuitBreaker;
    private final JobContextService jobContextService;

    @Inject
    public TransportFetchNodeAction(TransportService transportService,
                                    Transports transports,
                                    StatsTables statsTables,
                                    CircuitBreakerService breakerService,
                                    JobContextService jobContextService,
                                    NodeFetchOperation nodeFetchOperation) {
        this.transports = transports;
        this.statsTables = statsTables;
        this.nodeFetchOperation = nodeFetchOperation;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        this.jobContextService = jobContextService;

        transportService.registerRequestHandler(TRANSPORT_ACTION,
            NodeFetchRequest.class,
            EXECUTOR_NAME,
            new NodeActionRequestHandler<NodeFetchRequest, NodeFetchResponse>(this) {});
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
                              final ActionListener<NodeFetchResponse> fetchResponse) {

        String ramAccountingContextId = String.format(Locale.ENGLISH, "%s: %d", request.jobId(), request.fetchPhaseId());
        final RamAccountingContext ramAccountingContext = new RamAccountingContext(ramAccountingContextId, circuitBreaker);
        try {
            statsTables.operationStarted(request.fetchPhaseId(), request.jobId(), "fetch");

            // nothing to fetch, just close
            if (request.toFetch() == null && request.isCloseContext()) {
                JobExecutionContext ctx = jobContextService.getContextOrNull(request.jobId());
                if (ctx != null) {
                    FetchContext fetchContext = ctx.getSubContextOrNull(request.fetchPhaseId());
                    if (fetchContext != null) {
                        fetchContext.close();
                    }
                }
                fetchResponse.onResponse(NodeFetchResponse.EMPTY);
                return;
            }

            JobExecutionContext jobExecutionContext = jobContextService.getContext(request.jobId());
            final FetchContext fetchContext = jobExecutionContext.getSubContext(request.fetchPhaseId());

            ListenableFuture<IntObjectMap<StreamBucket>> fetchedBucketFuture =
                nodeFetchOperation.doFetch(fetchContext, request.toFetch());

            Futures.addCallback(fetchedBucketFuture, new FutureCallback<IntObjectMap<StreamBucket>>() {
                @Override
                public void onSuccess(@Nullable IntObjectMap<StreamBucket> result) {
                    assert result != null : "result of nodeFetchOperation.doFetch must not be null";
                    NodeFetchResponse response = NodeFetchResponse.forSending(result);
                    if (request.isCloseContext()) {
                        fetchContext.close();
                    }
                    fetchResponse.onResponse(response);
                    statsTables.operationFinished(request.fetchPhaseId(), request.jobId(), null,
                        ramAccountingContext.totalBytes());
                    ramAccountingContext.close();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    fetchContext.kill(t);
                    fetchResponse.onFailure(t);
                    statsTables.operationFinished(request.fetchPhaseId(), request.jobId(), Exceptions.messageOf(t),
                        ramAccountingContext.totalBytes());
                    ramAccountingContext.close();
                }
            });
        } catch (Throwable t) {
            fetchResponse.onFailure(t);
            statsTables.operationFinished(request.fetchPhaseId(), request.jobId(), Exceptions.messageOf(t),
                ramAccountingContext.totalBytes());
            ramAccountingContext.close();
        }
    }
}
