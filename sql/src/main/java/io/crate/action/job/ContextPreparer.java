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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.callbacks.OperationFinishedStatsTablesCallback;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.CountContext;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.ExecutionNodes;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.CountNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Singleton
public class ContextPreparer {

    private static final ESLogger LOGGER = Loggers.getLogger(ContextPreparer.class);

    private final MapSideDataCollectOperation collectOperationHandler;
    private ClusterService clusterService;
    private CountOperation countOperation;
    private final CircuitBreaker circuitBreaker;
    private final StatsTables statsTables;
    private final ThreadPool threadPool;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ResultProviderFactory resultProviderFactory;
    private final StreamerVisitor streamerVisitor;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(MapSideDataCollectOperation collectOperationHandler,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           StatsTables statsTables,
                           ThreadPool threadPool,
                           CountOperation countOperation,
                           PageDownstreamFactory pageDownstreamFactory,
                           ResultProviderFactory resultProviderFactory,
                           StreamerVisitor streamerVisitor) {
        this.collectOperationHandler = collectOperationHandler;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.statsTables = statsTables;
        this.threadPool = threadPool;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.resultProviderFactory = resultProviderFactory;
        this.streamerVisitor = streamerVisitor;
        innerPreparer = new InnerPreparer();
    }

    @Nullable
    public ListenableFuture<Bucket> prepare(UUID jobId,
                                            ExecutionNode executionNode,
                                            JobExecutionContext.Builder contextBuilder) {
        RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, executionNode);
        PreparerContext preparerContext = new PreparerContext(jobId, ramAccountingContext, contextBuilder);
        innerPreparer.process(executionNode, preparerContext);
        return preparerContext.directResultFuture;
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

    private class InnerPreparer extends ExecutionNodeVisitor<PreparerContext, Void> {

        @Override
        public Void visitCountNode(CountNode countNode, PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = countNode.routing().locations();
            if (locations == null) {
                throw new IllegalArgumentException("locations are empty. Can't start count operation");
            }
            String localNodeId = clusterService.localNode().id();
            Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countNode doesn't contain the current nodeId");
            }

            final SingleBucketBuilder singleBucketBuilder = new SingleBucketBuilder(new Streamer[]{DataTypes.LONG});
            CountContext countContext = new CountContext(
                    countOperation,
                    singleBucketBuilder,
                    indexShardMap,
                    countNode.whereClause()
            );
            context.directResultFuture = singleBucketBuilder.result();
            context.contextBuilder.addSubContext(countNode.executionNodeId(), countContext);
            return null;
        }

        @Override
        public Void visitMergeNode(final MergeNode node, final PreparerContext context) {

            ResultProvider downstream = resultProviderFactory.createDownstream(node, node.jobId());
            PageDownstream pageDownstream = pageDownstreamFactory.createMergeNodePageDownstream(
                    node,
                    downstream,
                    context.ramAccountingContext,
                    Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
            );
            StreamerVisitor.Context streamerContext = streamerVisitor.processPlanNode(node);
            PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                    pageDownstream, streamerContext.inputStreamers(),
                    context.ramAccountingContext, node.numUpstreams());

            statsTables.operationStarted(node.executionNodeId(), context.jobId, node.name());
            Futures.addCallback(downstream.result(), new OperationFinishedStatsTablesCallback<Bucket>(
                    node.executionNodeId(), statsTables, context.ramAccountingContext));

            context.contextBuilder.addSubContext(node.executionNodeId(), pageDownstreamContext);
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
            final JobCollectContext jobCollectContext = new JobCollectContext(
                    context.jobId,
                    context.ramAccountingContext,
                    downstream
            );
            context.contextBuilder.addSubContext(node.executionNodeId(), jobCollectContext);
            if (!node.keepContextForFetcher()) {
                downstream.result().addListener(new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.trace("Closing JobCollectContext {}/{} because result is ready",
                                node.jobId().get(), node.executionNodeId());

                        jobCollectContext.close();
                    }
                }, MoreExecutors.directExecutor());
            }
            return null;
        }
    }
}
