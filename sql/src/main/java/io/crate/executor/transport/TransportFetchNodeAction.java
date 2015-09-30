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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.Executor;

@Singleton
public class TransportFetchNodeAction implements NodeAction<NodeFetchRequest, NodeFetchResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/fetch";
    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;
    private static final String RESPONSE_EXECUTOR = ThreadPool.Names.SAME;

    private Transports transports;
    private final StatsTables statsTables;
    private final NodeFetchOperation nodeFetchOperation;
    private final CircuitBreaker circuitBreaker;
    private final JobContextService jobContextService;
    private final ThreadPool threadPool;

    @Inject
    public TransportFetchNodeAction(TransportService transportService,
                                    Transports transports,
                                    ThreadPool threadPool,
                                    StatsTables statsTables,
                                    CircuitBreakerService breakerService,
                                    JobContextService jobContextService,
                                    NodeFetchOperation nodeFetchOperation) {
        this.transports = transports;
        this.statsTables = statsTables;
        this.nodeFetchOperation = nodeFetchOperation;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.jobContextService = jobContextService;
        this.threadPool = threadPool;

        transportService.registerHandler(TRANSPORT_ACTION,
                new NodeActionRequestHandler<NodeFetchRequest, NodeFetchResponse>(this) {
                    @Override
                    public NodeFetchRequest newInstance() {
                        return new NodeFetchRequest();
                    }
                });
    }

    public void execute(String targetNode,
                        final IntObjectMap<Streamer[]> streamers,
                        final NodeFetchRequest request,
                        ActionListener<NodeFetchResponse> listener) {
        transports.executeLocalOrWithTransport(this, targetNode, request, listener,
                new DefaultTransportResponseHandler<NodeFetchResponse>(listener, RESPONSE_EXECUTOR) {
                    @Override
                    public NodeFetchResponse newInstance() {
                        return NodeFetchResponse.forReceiveing(streamers);
                    }
                });
    }

    @Override
    public String actionName() {
        return TRANSPORT_ACTION;
    }

    @Override
    public String executorName() {
        return EXECUTOR_NAME;
    }

    @Override
    public void nodeOperation(final NodeFetchRequest request,
                              final ActionListener<NodeFetchResponse> fetchResponse) {

        String ramAccountingContextId = String.format(Locale.ENGLISH, "%s: %d", request.jobId(), request.fetchPhaseId());
        final RamAccountingContext ramAccountingContext = new RamAccountingContext(ramAccountingContextId, circuitBreaker);
        try {
            statsTables.operationStarted(request.fetchPhaseId(), request.jobId(), "fetch");

            JobExecutionContext jobExecutionContext = jobContextService.getContext(request.jobId());
            final FetchContext fetchContext = jobExecutionContext.getSubContext(request.fetchPhaseId());

            // nothing to fetch, just close
            if (request.toFetch() == null) {
                fetchContext.close();
                fetchResponse.onResponse(NodeFetchResponse.EMPTY);
                fetchContext.close();
                return;
            }


            Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    fetchResponse.onFailure(t);
                    statsTables.operationFinished(request.fetchPhaseId(), Exceptions.messageOf(t),
                            ramAccountingContext.totalBytes());
                    fetchContext.kill(t);
                }

                @Override
                protected void doRun() throws Exception {
                    IntObjectMap<StreamBucket> fetched = nodeFetchOperation.doFetch(
                            fetchContext, request.toFetch());
                    // no streamers needed to serialize, since the buckets are StreamBuckets
                    NodeFetchResponse response = NodeFetchResponse.forSending(fetched);
                    fetchResponse.onResponse(response);
                    statsTables.operationFinished(request.fetchPhaseId(), null,
                            ramAccountingContext.totalBytes());
                    fetchContext.close();
                }

                @Override
                public void onAfter() {
                    ramAccountingContext.close();
                }
            });
        } catch (Throwable t) {
            fetchResponse.onFailure(t);
            statsTables.operationFinished(request.fetchPhaseId(), Exceptions.messageOf(t),
                    ramAccountingContext.totalBytes());
            ramAccountingContext.close();
        }

    }

}
