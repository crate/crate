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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.fetch.NodeFetchOperation;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

@Singleton
public class TransportFetchNodeAction implements NodeAction<NodeFetchRequest, NodeFetchResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/fetch";
    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private Transports transports;
    private final StatsTables statsTables;
    private final CircuitBreaker circuitBreaker;
    private final JobContextService jobContextService;
    private final ThreadPool threadPool;
    private final Functions functions;

    @Inject
    public TransportFetchNodeAction(TransportService transportService,
                                    Transports transports,
                                    ThreadPool threadPool,
                                    StatsTables statsTables,
                                    Functions functions,
                                    CircuitBreakerService breakerService,
                                    JobContextService jobContextService) {
        this.transports = transports;
        this.statsTables = statsTables;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.jobContextService = jobContextService;
        this.threadPool = threadPool;
        this.functions = functions;

        transportService.registerHandler(TRANSPORT_ACTION,
                new NodeActionRequestHandler<NodeFetchRequest, NodeFetchResponse>(this) {
            @Override
            public NodeFetchRequest newInstance() {
                return new NodeFetchRequest();
            }
        });
    }

    public void execute(
            String targetNode,
            final NodeFetchRequest request,
            ActionListener<NodeFetchResponse> listener) {
        transports.executeLocalOrWithTransport(this, targetNode, request, listener,
                new DefaultTransportResponseHandler<NodeFetchResponse>(listener, ThreadPool.Names.SUGGEST) {
            @Override
            public NodeFetchResponse newInstance() {
                return new NodeFetchResponse(outputStreamers(request.toFetchReferences()));
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
        statsTables.operationStarted(request.executionPhaseId(), request.jobId(), "fetch");
        String ramAccountingContextId = String.format(Locale.ENGLISH, "%s: %d", request.jobId(), request.executionPhaseId());
        final RamAccountingContext ramAccountingContext = new RamAccountingContext(ramAccountingContextId, circuitBreaker);

        NodeFetchOperation fetchOperation = new NodeFetchOperation(
                request.jobId(),
                request.executionPhaseId(),
                request.jobSearchContextDocIds(),
                request.toFetchReferences(),
                request.closeContext(),
                jobContextService,
                threadPool,
                functions,
                ramAccountingContext);

        Streamer<?>[] streamers = outputStreamers(request.toFetchReferences());
        SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers);
        final NodeFetchResponse response = new NodeFetchResponse(streamers);
        Futures.addCallback(bucketBuilder.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(@Nullable Bucket result) {
                assert result != null;
                response.rows(result);

                fetchResponse.onResponse(response);
                statsTables.operationFinished(request.executionPhaseId(), null, ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fetchResponse.onFailure(t);
                statsTables.operationFinished(request.executionPhaseId(), Exceptions.messageOf(t),
                        ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }
        });

        try {
            fetchOperation.fetch(bucketBuilder);
        } catch (Throwable t) {
            fetchResponse.onFailure(t);
            statsTables.operationFinished(request.executionPhaseId(), Exceptions.messageOf(t),
                    ramAccountingContext.totalBytes());
            ramAccountingContext.close();
        }
    }

    private static Streamer<?>[] outputStreamers(List<Reference> toFetchReferences) {
        Streamer<?>[] streamers = new Streamer<?>[toFetchReferences.size()];
        for (int i = 0; i < toFetchReferences.size(); i++) {
            streamers[i] = toFetchReferences.get(i).valueType().streamer();
        }
        return streamers;
    }
}
