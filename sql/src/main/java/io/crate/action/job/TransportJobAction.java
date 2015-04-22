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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.callbacks.OperationFinishedStatsTablesCallback;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.ExecutionNodes;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

@Singleton
public class TransportJobAction implements NodeAction<JobRequest, JobResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportJobAction.class);

    public static final String ACTION_NAME = "crate/sql/job";
    private static final String EXECUTOR = ThreadPool.Names.GENERIC;
    private static final String COLLECT_EXECUTOR = ThreadPool.Names.SEARCH;


    private final Transports transports;
    private final JobContextService jobContextService;

    private final ContextPreparer contextPreparer;
    private final MapSideDataCollectOperation collectOperationHandler;

    @Inject
    public TransportJobAction(TransportService transportService,
                              Transports transports,
                              JobContextService jobContextService,
                              ResultProviderFactory resultProviderFactory,
                              PageDownstreamFactory pageDownstreamFactory,
                              StreamerVisitor streamerVisitor,
                              ThreadPool threadPool,
                              CrateCircuitBreakerService breakerService,
                              StatsTables statsTables,
                              MapSideDataCollectOperation collectOperationHandler) {
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.collectOperationHandler = collectOperationHandler;
        CircuitBreaker circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.contextPreparer = new ContextPreparer(
                collectOperationHandler,
                circuitBreaker,
                statsTables,
                threadPool,
                pageDownstreamFactory,
                resultProviderFactory,
                streamerVisitor
        );
        transportService.registerHandler(ACTION_NAME, new NodeActionRequestHandler<JobRequest, JobResponse>(this) {
            @Override
            public JobRequest newInstance() {
                return new JobRequest();
            }
        });
    }

    public void execute(String node, final JobRequest request, final ActionListener<JobResponse> listener) {
        transports.executeLocalOrWithTransport(this, node, request, listener,
                new DefaultTransportResponseHandler<JobResponse>(listener, EXECUTOR) {
                    @Override
                    public JobResponse newInstance() {
                        return new JobResponse();
                    }
                });
    }

    @Override
    public void nodeOperation(final JobRequest request, final ActionListener<JobResponse> actionListener) {
        JobExecutionContext.Builder contextBuilder = new JobExecutionContext.Builder(request.jobId());

        ListenableFuture<Bucket> directResponseFuture = null;
        for (ExecutionNode executionNode : request.executionNodes()) {
            ListenableFuture<Bucket> responseFuture = contextPreparer.prepare(request.jobId(), executionNode, contextBuilder);
            if (responseFuture != null) {
                assert directResponseFuture == null : "only one executionNode may have a directResponse";
                directResponseFuture = responseFuture;
            }
        }

        JobExecutionContext context = jobContextService.createContext(contextBuilder);
        context.start();

        if (directResponseFuture == null) {
            actionListener.onResponse(new JobResponse());
        } else {
            Futures.addCallback(directResponseFuture, new FutureCallback<Bucket>() {
                @Override
                public void onSuccess(Bucket bucket) {
                    actionListener.onResponse(new JobResponse(bucket));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    actionListener.onFailure(t);
                }
            });
        }
    }

    @Override
    public String actionName() {
        return ACTION_NAME;
    }

    @Override
    public String executorName() {
        return EXECUTOR;
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final JobExecutionContext.Builder contextBuilder;
        private final RamAccountingContext ramAccountingContext;
        private ListenableFuture<Bucket> directResultFuture;

        private PreparerContext(UUID jobId,
                                RamAccountingContext ramAccountingContext,
                                JobExecutionContext.Builder contextBuilder) {
            this.ramAccountingContext = ramAccountingContext;
            this.contextBuilder = contextBuilder;
            this.jobId = jobId;
        }
    }

    private static class ContextPreparer extends ExecutionNodeVisitor<PreparerContext, Void> {

        private final MapSideDataCollectOperation collectOperationHandler;
        private final CircuitBreaker circuitBreaker;
        private final StatsTables statsTables;
        private final ThreadPool threadPool;
        private final PageDownstreamFactory pageDownstreamFactory;
        private final ResultProviderFactory resultProviderFactory;
        private final StreamerVisitor streamerVisitor;

        public ContextPreparer(MapSideDataCollectOperation collectOperationHandler,
                               CircuitBreaker circuitBreaker,
                               StatsTables statsTables,
                               ThreadPool threadPool,
                               PageDownstreamFactory pageDownstreamFactory,
                               ResultProviderFactory resultProviderFactory,
                               StreamerVisitor streamerVisitor) {
            this.collectOperationHandler = collectOperationHandler;
            this.circuitBreaker = circuitBreaker;
            this.statsTables = statsTables;
            this.threadPool = threadPool;
            this.pageDownstreamFactory = pageDownstreamFactory;
            this.resultProviderFactory = resultProviderFactory;
            this.streamerVisitor = streamerVisitor;
        }

        @Nullable
        public ListenableFuture<Bucket> prepare(UUID jobId,
                                                ExecutionNode executionNode,
                                                JobExecutionContext.Builder contextBuilder) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, executionNode);
            PreparerContext preparerContext = new PreparerContext(jobId, ramAccountingContext, contextBuilder);
            process(executionNode, preparerContext);
            return preparerContext.directResultFuture;
        }

        @Override
        public Void visitMergeNode(final MergeNode node, final PreparerContext context) {
            statsTables.operationStarted(node.executionNodeId(), context.jobId, node.name());

            ResultProvider downstream = resultProviderFactory.createDownstream(node, node.jobId());
            PageDownstream pageDownstream = pageDownstreamFactory.createMergeNodePageDownstream(
                    node,
                    downstream,
                    context.ramAccountingContext,
                    Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
            );
            StreamerVisitor.Context streamerContext = streamerVisitor.processPlanNode(node, context.ramAccountingContext);
            PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                    pageDownstream,  streamerContext.inputStreamers(), node.numUpstreams());

            Futures.addCallback(downstream.result(), new OperationFinishedStatsTablesCallback<Bucket>(
                    node.executionNodeId(), statsTables, context.ramAccountingContext));

            context.contextBuilder.addPageDownstreamContext(node.executionNodeId(), pageDownstreamContext);
            return null;
        }

        @Override
        public Void visitCollectNode(final CollectNode node, final PreparerContext context) {
            ResultProvider downstream = collectOperationHandler.createDownstream(node);

            if (ExecutionNodes.hasDirectResponseDownstream(node.downstreamNodes())) {
                context.directResultFuture = downstream.result();
            }
            Futures.addCallback(downstream.result(), new OperationFinishedStatsTablesCallback<Bucket>(
                    node.executionNodeId(), statsTables, context.ramAccountingContext));

            context.contextBuilder.addCollectContext(
                    node.executionNodeId(),
                    new JobCollectContext(collectOperationHandler, context.jobId, node, downstream, context.ramAccountingContext));
            return null;
        }
    }
}
