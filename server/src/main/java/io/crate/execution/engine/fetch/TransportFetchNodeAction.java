/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.fetch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.IntObjectMap;

import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;

@Singleton
public class TransportFetchNodeAction extends TransportAction<NodeFetchRequest, NodeFetchResponse> {

    private final Transports transports;
    private final NodeFetchOperation nodeFetchOperation;

    @Inject
    public TransportFetchNodeAction(Settings settings,
                                    TransportService transportService,
                                    Transports transports,
                                    ThreadPool threadPool,
                                    JobsLogs jobsLogs,
                                    TasksService tasksService,
                                    CircuitBreakerService circuitBreakerService) {
        super(FetchNodeAction.NAME);
        this.transports = transports;
        this.nodeFetchOperation = new NodeFetchOperation(
            (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH),
            EsExecutors.numberOfProcessors(settings),
            jobsLogs,
            tasksService,
            circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY)
        );

        transportService.registerRequestHandler(
            FetchNodeAction.NAME,
            ThreadPool.Names.SEARCH,
            // force execution because this handler might receive empty close requests which
            // need to be processed to not leak the FetchTask.
            // This shouldn't cause too much of an issue because fetch requests always happen after a query phase.
            // If the threadPool is overloaded the query phase would fail first.
            true,
            false,
            NodeFetchRequest.FetchRequest::new,
            new NodeActionRequestHandler<>(this::nodeOperation)
        );
    }

    @Override
    public void doExecute(NodeFetchRequest nodeFetchRequest, ActionListener<NodeFetchResponse> listener) {
        transports.sendRequest(
            FetchNodeAction.NAME,
            nodeFetchRequest.nodeId(),
            nodeFetchRequest.innerRequest(),
            listener,
            new ActionListenerResponseHandler<>(FetchNodeAction.NAME, listener, nodeFetchRequest.createResponseReader())
        );
    }

    private CompletableFuture<NodeFetchResponse> nodeOperation(final NodeFetchRequest.FetchRequest request) {
        CompletableFuture<? extends IntObjectMap<StreamBucket>> resultFuture = nodeFetchOperation.fetch(
            request.jobId(),
            request.fetchPhaseId(),
            request.toFetch(),
            request.isCloseContext()
        );
        return resultFuture.thenApply(NodeFetchResponse::new);
    }
}
