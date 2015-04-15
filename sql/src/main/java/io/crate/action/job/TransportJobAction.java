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

package io.crate.action.job;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.ResponseForwarder;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

@Singleton
public class TransportJobAction {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportJobAction.class);

    public static final String ACTION_NAME = "crate/sql/job";
    private static final String EXECUTOR = ThreadPool.Names.GENERIC;
    private static final String COLLECT_EXECUTOR = ThreadPool.Names.SEARCH;


    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;

    private final CircuitBreaker circuitBreaker;
    private final ExecutionNodesExecutingVisitor executionNodeVisitor;
    private final MapSideDataCollectOperation collectOperationHandler;
    private final StatsTables statsTables;

    @Inject
    public TransportJobAction(TransportService transportService,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              CrateCircuitBreakerService breakerService,
                              StatsTables statsTables,
                              MapSideDataCollectOperation collectOperationHandler) {
        this.threadPool = threadPool;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.clusterService = clusterService;
        this.statsTables = statsTables;
        this.collectOperationHandler = collectOperationHandler;
        this.transportService = transportService;
        transportService.registerHandler(ACTION_NAME, new JobInitHandler());
        this.executionNodeVisitor = new ExecutionNodesExecutingVisitor();

    }

    public void execute(String node, final JobRequest request, final ActionListener<JobResponse> listener) {
        ClusterState clusterState = clusterService.state();
        if (node.equals("_local") || node.equals(clusterState.nodes().localNodeId())) {
            try {
                threadPool.executor(EXECUTOR).execute(new Runnable() {
                    @Override
                    public void run() {
                        nodeOperation(request, listener);
                    }
                });
            } catch (RejectedExecutionException e) {
                LOGGER.error("error executing jobinit locally on node [{}]", e, node);
                listener.onFailure(e);
            }
        } else {
            transportService.sendRequest(
                    clusterState.nodes().get(node),
                    ACTION_NAME,
                    request,
                    new DefaultTransportResponseHandler<JobResponse>(listener, EXECUTOR) {
                        @Override
                        public JobResponse newInstance() {
                            return new JobResponse();
                        }
                    }
            );
        }
    }

    private void nodeOperation(final JobRequest request, final ActionListener<JobResponse> actionListener) {
        List<ListenableFuture<Bucket>> executionFutures = new ArrayList<>(request.executionNodes().size());
        for (ExecutionNode executionNode : request.executionNodes()) {
            try {
                String ramAccountingContextId = String.format("%s: %s", executionNode.name(), request.jobId());
                final RamAccountingContext ramAccountingContext =
                        new RamAccountingContext(ramAccountingContextId, circuitBreaker);
                executionFutures.add(executionNodeVisitor.handle(
                        executionNode,
                        ramAccountingContext,
                        request.jobId()
                ));
            } catch (Throwable t) {
                LOGGER.error("error starting ExecutionNode {}", t, executionNode);
                actionListener.onFailure(t);
            }
        }
        // wait for all operations to complete
        // if an error occurs, we can inform the handler node
        Futures.addCallback(Futures.allAsList(executionFutures), new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(@Nullable List<Bucket> buckets) {
                assert buckets != null;
                if (buckets.isEmpty()) {
                    actionListener.onResponse(new JobResponse());
                } else {
                    assert buckets.size() == 1;
                    Bucket directResultBucket = buckets.get(0);
                    LOGGER.trace("direct result ready: {}", directResultBucket);
                    actionListener.onResponse(
                            new JobResponse(directResultBucket)
                    );
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                LOGGER.error("error waiting for ExecutionNode result", t);
                actionListener.onFailure(t);
            }
        });
    }

    private class JobInitHandler extends BaseTransportRequestHandler<JobRequest> {

        @Override
        public JobRequest newInstance() {
            return new JobRequest();
        }

        @Override
        public void messageReceived(JobRequest request, TransportChannel channel) throws Exception {
            ActionListener<JobResponse> actionListener = ResponseForwarder.forwardTo(channel);
            nodeOperation(request, actionListener);
        }

        @Override
        public String executor() {
            return EXECUTOR;
        }
    }

    private static class VisitorContext {
        private final UUID jobId;
        private final SettableFuture<Bucket> directResultFuture;
        private final RamAccountingContext ramAccountingContext;

        private VisitorContext(UUID jobId, RamAccountingContext ramAccountingContext, SettableFuture<Bucket> directResultFuture) {
            this.directResultFuture = directResultFuture;
            this.ramAccountingContext = ramAccountingContext;
            this.jobId = jobId;
        }
    }

    private class ExecutionNodesExecutingVisitor extends ExecutionNodeVisitor<VisitorContext, Void> {

        public SettableFuture<Bucket> handle(ExecutionNode executionNode, RamAccountingContext ramAccountingContext, UUID jobId) {
            SettableFuture<Bucket> future = SettableFuture.create();
            process(executionNode, new VisitorContext(jobId, ramAccountingContext, future));
            return future;
        }

        @Override
        public Void visitCollectNode(final CollectNode collectNode, final VisitorContext context) {
            // start collect Operation
            threadPool.executor(COLLECT_EXECUTOR).execute(new Runnable() {
                @Override
                public void run() {
                    final UUID operationId = UUID.randomUUID();
                    statsTables.operationStarted(operationId, context.jobId, collectNode.name());
                    ResultProvider downstream = collectOperationHandler.createDownstream(collectNode);
                    Futures.addCallback(downstream.result(), new FutureCallback<Bucket>() {
                        @Override
                        public void onSuccess(@Nullable Bucket result) {
                            statsTables.operationFinished(operationId, null, context.ramAccountingContext.totalBytes());
                            context.ramAccountingContext.close();
                            if (result == null) {
                                context.directResultFuture.set(Bucket.EMPTY);
                            } else {
                                context.directResultFuture.set(result);
                            }
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable t) {
                            statsTables.operationFinished(operationId, Exceptions.messageOf(t),
                                    context.ramAccountingContext.totalBytes());
                            context.ramAccountingContext.close();
                            context.directResultFuture.setException(t);
                        }
                    });
                    try {
                        collectOperationHandler.collect(collectNode, downstream, context.ramAccountingContext);
                    } catch (Throwable t) {
                        downstream.fail(t);
                    }
                }
            });
            return null;
        }
    }
}
