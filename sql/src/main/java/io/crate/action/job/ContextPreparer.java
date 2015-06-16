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

import com.google.common.base.MoreObjects;
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
import io.crate.metadata.Routing;
import io.crate.operation.*;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.RowDownstreamFactory;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoopPhase;
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
import java.util.concurrent.Executor;

@Singleton
public class ContextPreparer {

    private static final ESLogger LOGGER = Loggers.getLogger(ContextPreparer.class);

    private final MapSideDataCollectOperation collectOperation;
    private ClusterService clusterService;
    private CountOperation countOperation;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final RowDownstreamFactory rowDownstreamFactory;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           CountOperation countOperation,
                           ThreadPool threadPool,
                           PageDownstreamFactory pageDownstreamFactory,
                           RowDownstreamFactory rowDownstreamFactory) {
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        this.threadPool = threadPool;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.rowDownstreamFactory = rowDownstreamFactory;
        innerPreparer = new InnerPreparer();
    }

    @Nullable
    public ListenableFuture<Bucket> prepare(UUID jobId,
                                            NodeOperation nodeOperation,
                                            JobExecutionContext.Builder contextBuilder) {
        PreparerContext preparerContext = new PreparerContext(jobId, nodeOperation, contextBuilder);
        innerPreparer.process(nodeOperation.executionPhase(), preparerContext);
        if (preparerContext.bucketBuilder != null){
            return preparerContext.bucketBuilder.result();
        }
        return null;
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final NodeOperation nodeOperation;
        private final JobExecutionContext.Builder contextBuilder;
        private final SingleBucketBuilder bucketBuilder;

        private PreparerContext(UUID jobId,
                                NodeOperation nodeOperation,
                                JobExecutionContext.Builder contextBuilder) {
            this.nodeOperation = nodeOperation;
            this.contextBuilder = contextBuilder;
            this.jobId = jobId;
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
                bucketBuilder = new SingleBucketBuilder(streamers);
            } else {
                bucketBuilder = null;
            }
        }


    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, Void> {

        RowDownstream getDownstream(PreparerContext context, int pageSize) {
            if (context.bucketBuilder != null) {
                return context.bucketBuilder;
            } else {
                return rowDownstreamFactory.createDownstream(context.nodeOperation, context.jobId, pageSize);
            }
        }

        @Override
        public Void visitCountPhase(CountPhase phase, PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = phase.routing().locations();
            if (locations == null) {
                throw new IllegalArgumentException("locations are empty. Can't start count operation");
            }
            String localNodeId = clusterService.localNode().id();
            Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countNode doesn't contain the current nodeId");
            }

            CountContext countContext = new CountContext(
                    countOperation,
                    getDownstream(context, 1),
                    indexShardMap,
                    phase.whereClause()
            );
            context.contextBuilder.addSubContext(phase.executionPhaseId(), countContext);
            return null;
        }

        @Override
        public Void visitMergePhase(final MergePhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            RowDownstream downstream = getDownstream(context,
                    Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.executionNodes().size()));
            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            phase,
                            downstream,
                            false,
                            ramAccountingContext,
                            // no separate executor because TransportDistributedResultAction already runs in a threadPool
                            Optional.<Executor>absent());

            PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                    phase.name(),
                    pageDownstreamProjectorChain.v1(),
                    DataTypes.getStreamer(phase.inputTypes()),
                    ramAccountingContext,
                    phase.numUpstreams(),
                    pageDownstreamProjectorChain.v2());

            context.contextBuilder.addSubContext(phase.executionPhaseId(), pageDownstreamContext);
            return null;
        }

        @Override
        public Void visitCollectPhase(final CollectPhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            String localNodeId = clusterService.localNode().id();
            Routing routing = phase.routing();
            int numTotalShards = routing.numShards();
            int numShardsOnNode = routing.numShards(localNodeId);
            int pageSize = Paging.getWeightedPageSize(
                    MoreObjects.firstNonNull(phase.limit(), Paging.PAGE_SIZE),
                    1.0 / numTotalShards * numShardsOnNode
            );
            LOGGER.trace("{} setting node page size to: {}, numShards in total: {} shards on node: {}",
                    localNodeId, pageSize, numTotalShards, numShardsOnNode);

            RowDownstream downstream = getDownstream(context, pageSize);
            final JobCollectContext jobCollectContext = new JobCollectContext(
                    context.jobId,
                    phase,
                    collectOperation,
                    ramAccountingContext,
                    downstream
            );
            context.contextBuilder.addSubContext(phase.executionPhaseId(), jobCollectContext);
            return null;
        }

        @Override
        public Void visitNestedLoopPhase(NestedLoopPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            RowDownstream downstream = getDownstream(context, Paging.PAGE_SIZE);
            NestedLoopContext nestedLoopContext = new NestedLoopContext(
                    phase,
                    downstream,
                    ramAccountingContext,
                    pageDownstreamFactory,
                    threadPool);

            context.contextBuilder.addSubContext(phase.executionPhaseId(), nestedLoopContext);
            return null;
        }
    }
}
