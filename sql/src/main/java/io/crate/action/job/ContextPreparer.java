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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.CountContext;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.NestedLoopContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.ExecutionNodes;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.CountNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
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

    private final MapSideDataCollectOperation collectOperation;
    private ClusterService clusterService;
    private CountOperation countOperation;
    private final CircuitBreaker circuitBreaker;
    private final ThreadPool threadPool;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ResultProviderFactory resultProviderFactory;
    private final StreamerVisitor streamerVisitor;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           ThreadPool threadPool,
                           CountOperation countOperation,
                           PageDownstreamFactory pageDownstreamFactory,
                           ResultProviderFactory resultProviderFactory,
                           StreamerVisitor streamerVisitor) {
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
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
        PreparerContext preparerContext = new PreparerContext(jobId, contextBuilder);
        innerPreparer.process(executionNode, preparerContext);
        return preparerContext.directResultFuture;
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final JobExecutionContext.Builder contextBuilder;
        private ListenableFuture<Bucket> directResultFuture;

        private PreparerContext(UUID jobId,
                                JobExecutionContext.Builder contextBuilder) {
            this.contextBuilder = contextBuilder;
            this.jobId = jobId;
        }
    }

    private class InnerPreparer extends ExecutionNodeVisitor<PreparerContext, Void> {

        @Override
        public Void visitCountNode(CountNode node, PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = node.routing().locations();
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
                    node.whereClause()
            );
            context.directResultFuture = singleBucketBuilder.result();
            context.contextBuilder.addSubContext(node.executionNodeId(), countContext);
            return null;
        }

        @Override
        public Void visitMergeNode(MergeNode node, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, node);
            ResultProvider downstream = resultProviderFactory.createDownstream(node, node.jobId());
            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            node,
                            downstream,
                            ramAccountingContext,
                            Optional.of(threadPool.executor(ThreadPool.Names.SEARCH)));
            StreamerVisitor.Context streamerContext = streamerVisitor.processPlanNode(node);
            PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                    node.name(),
                    pageDownstreamProjectorChain.v1(),
                    streamerContext.inputStreamers(),
                    ramAccountingContext,
                    node.numUpstreams());

            context.contextBuilder.addSubContext(node.executionNodeId(), pageDownstreamContext);

            FlatProjectorChain flatProjectorChain = pageDownstreamProjectorChain.v2();
            if (flatProjectorChain != null) {
                flatProjectorChain.startProjections(pageDownstreamContext);
            }
            return null;
        }

        @Override
        public Void visitCollectNode(CollectNode node, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, node);
            ResultProvider downstream = resultProviderFactory.createDownstream(node, node.jobId());

            if (ExecutionNodes.hasDirectResponseDownstream(node.downstreamNodes())) {
                context.directResultFuture = downstream.result();
            }
            final JobCollectContext jobCollectContext = new JobCollectContext(
                    context.jobId,
                    node,
                    collectOperation,
                    ramAccountingContext,
                    downstream
            );
            context.contextBuilder.addSubContext(node.executionNodeId(), jobCollectContext);
            return null;
        }

        @Override
        public Void visitNestedLoopNode(NestedLoopNode node, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, node);

            ResultProvider downstream = resultProviderFactory.createDownstream(node, node.jobId());

            NestedLoopContext nestedLoopContext = new NestedLoopContext(
                    node,
                    downstream,
                    ramAccountingContext,
                    pageDownstreamFactory,
                    threadPool,
                    streamerVisitor);

            context.contextBuilder.addSubContext(node.executionNodeId(), nestedLoopContext);
            return null;
        }
    }
}
