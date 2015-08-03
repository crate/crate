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
import io.crate.jobs.PageDownstreamContext;
import io.crate.metadata.Routing;
import io.crate.operation.NodeOperation;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.Paging;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

@Singleton
public class ContextPreparer {

    private final MapSideDataCollectOperation collectOperation;
    private ClusterService clusterService;
    private CountOperation countOperation;
    private final CircuitBreaker circuitBreaker;
    private final ThreadPool threadPool;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ResultProviderFactory resultProviderFactory;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           ThreadPool threadPool,
                           CountOperation countOperation,
                           PageDownstreamFactory pageDownstreamFactory,
                           ResultProviderFactory resultProviderFactory) {
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.threadPool = threadPool;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.resultProviderFactory = resultProviderFactory;
        innerPreparer = new InnerPreparer();
    }

    @Nullable
    public ListenableFuture<Bucket> prepare(UUID jobId,
                                            NodeOperation nodeOperation,
                                            JobExecutionContext.Builder contextBuilder) {
        PreparerContext preparerContext = new PreparerContext(jobId, nodeOperation, contextBuilder);
        innerPreparer.process(nodeOperation.executionPhase(), preparerContext);
        return preparerContext.directResultFuture;
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final NodeOperation nodeOperation;
        private final JobExecutionContext.Builder contextBuilder;
        private ListenableFuture<Bucket> directResultFuture;

        private PreparerContext(UUID jobId,
                                NodeOperation nodeOperation,
                                JobExecutionContext.Builder contextBuilder) {
            this.nodeOperation = nodeOperation;
            this.contextBuilder = contextBuilder;
            this.jobId = jobId;
        }
    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, Void> {

        @Override
        public Void visitCountNode(CountPhase countNode, PreparerContext context) {
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
            context.contextBuilder.addSubContext(countNode.executionPhaseId(), countContext);
            return null;
        }

        @Override
        public Void visitMergeNode(final MergePhase node, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, node);
            ResultProvider downstream = resultProviderFactory.createDownstream(
                    context.nodeOperation, node.jobId(), Paging.DEFAULT_PAGE_SIZE);
            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            node,
                            downstream,
                            ramAccountingContext,
                            // no separate executor because TransportDistributedResultAction already runs in a threadPool
                            Optional.<Executor>absent());

            PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                    node.name(),
                    pageDownstreamProjectorChain.v1(),
                    DataTypes.getStreamer(node.inputTypes()),
                    ramAccountingContext,
                    node.numUpstreams(),
                    pageDownstreamProjectorChain.v2());

            context.contextBuilder.addSubContext(node.executionPhaseId(), pageDownstreamContext);
            return null;
        }

        @Override
        public Void visitCollectNode(final CollectPhase node, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, node);

            String localNodeId = clusterService.localNode().id();
            Routing routing = node.routing();
            int pageSize = Paging.getNodePageSize(node.limit(), routing.numShards(), routing.numShards(localNodeId));
            ResultProvider downstream = resultProviderFactory.createDownstream(context.nodeOperation, node.jobId(), pageSize);

            if (ExecutionPhases.hasDirectResponseDownstream(context.nodeOperation.downstreamNodes())) {
                context.directResultFuture = downstream.result();
            }
            final JobCollectContext jobCollectContext = new JobCollectContext(
                    context.jobId,
                    node,
                    collectOperation,
                    ramAccountingContext,
                    downstream
            );
            context.contextBuilder.addSubContext(node.executionPhaseId(), jobCollectContext);
            return null;
        }
    }
}
