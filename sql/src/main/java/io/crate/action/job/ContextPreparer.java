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
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.*;
import io.crate.metadata.Functions;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.Routing;
import io.crate.operation.*;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.RowDownstreamFactory;
import io.crate.planner.RowGranularity;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
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
    private final ProjectorFactory projectorFactory;
    private final CircuitBreaker circuitBreaker;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final RowDownstreamFactory rowDownstreamFactory;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(Settings settings,
                           MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           CountOperation countOperation,
                           ThreadPool threadPool,
                           Functions functions,
                           TransportActionProvider transportActionProvider,
                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                           NestedReferenceResolver nestedReferenceResolver,
                           PageDownstreamFactory pageDownstreamFactory,
                           RowDownstreamFactory rowDownstreamFactory) {
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        this.threadPool = threadPool;
        this.projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                new ImplementationSymbolVisitor(
                        nestedReferenceResolver,
                        functions,
                        RowGranularity.CLUSTER
                )
        );
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.rowDownstreamFactory = rowDownstreamFactory;
        innerPreparer = new InnerPreparer();
    }

    public void prepare(UUID jobId,
                        NodeOperation nodeOperation,
                        JobExecutionContext.Builder contextBuilder,
                        @Nullable RowDownstream rowDownstream) {
        PreparerContext preparerContext = new PreparerContext(jobId, nodeOperation, rowDownstream);
        ExecutionSubContext subContext = innerPreparer.process(nodeOperation.executionPhase(), preparerContext);
        contextBuilder.addSubContext(nodeOperation.executionPhase().executionPhaseId(), subContext);
    }

    @SuppressWarnings("unchecked")
    public <T extends ExecutionSubContext> T prepare(UUID jobId, ExecutionPhase executionPhase, RowDownstream rowDownstream) {
        PreparerContext preparerContext = new PreparerContext(jobId, null, rowDownstream);
        return (T) innerPreparer.process(executionPhase, preparerContext);
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final NodeOperation nodeOperation;
        private final RowDownstream rowDownstream;

        private PreparerContext(UUID jobId, NodeOperation nodeOperation, @Nullable RowDownstream rowDownstream) {
            this.nodeOperation = nodeOperation;
            this.jobId = jobId;
            this.rowDownstream = rowDownstream;
        }
    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, ExecutionSubContext> {

        RowDownstream getDownstream(PreparerContext context, DistributionType distributionType, int pageSize) {
            if (context.rowDownstream == null) {
                assert context.nodeOperation != null : "nodeOperation shouldn't be null if context.rowDownstream hasn't been set";
                return rowDownstreamFactory.createDownstream(
                        context.nodeOperation,
                        distributionType,
                        context.jobId,
                        pageSize);
            }

            return context.rowDownstream;
        }

        @Override
        public ExecutionSubContext visitCountPhase(CountPhase phase, PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = phase.routing().locations();
            if (locations == null) {
                throw new IllegalArgumentException("locations are empty. Can't start count operation");
            }
            String localNodeId = clusterService.localNode().id();
            Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countNode doesn't contain the current nodeId");
            }

            return new CountContext(
                    countOperation,
                    context.rowDownstream,
                    indexShardMap,
                    phase.whereClause()
            );
        }

        @Override
        public ExecutionSubContext visitMergePhase(final MergePhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            RowDownstream downstream = getDownstream(context, phase.distributionType(),
                    Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.executionNodes().size()));
            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            phase,
                            downstream,
                            false,
                            ramAccountingContext,
                            // no separate executor because TransportDistributedResultAction already runs in a threadPool
                            Optional.<Executor>absent());

            return new PageDownstreamContext(
                    phase.name(),
                    pageDownstreamProjectorChain.v1(),
                    DataTypes.getStreamer(phase.inputTypes()),
                    ramAccountingContext,
                    phase.numUpstreams(),
                    pageDownstreamProjectorChain.v2());
        }

        @Override
        public ExecutionSubContext visitCollectPhase(final CollectPhase phase, final PreparerContext context) {
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

            RowDownstream downstream = getDownstream(context, phase.distributionType(), pageSize);
            return new JobCollectContext(
                    context.jobId,
                    phase,
                    collectOperation,
                    ramAccountingContext,
                    downstream
            );
        }

        @Override
        public ExecutionSubContext visitNestedLoopPhase(NestedLoopPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            RowDownstream downstream = getDownstream(context, DistributionType.BROADCAST, Paging.PAGE_SIZE);
            FlatProjectorChain flatProjectorChain = null;
            if (!phase.projections().isEmpty()) {
                flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                        projectorFactory,
                        ramAccountingContext,
                        phase.projections(),
                        downstream,
                        phase.jobId()
                );
                downstream = flatProjectorChain.firstProjector();
            }
            return new NestedLoopContext(
                    phase,
                    downstream,
                    ramAccountingContext,
                    pageDownstreamFactory,
                    threadPool,
                    flatProjectorChain
            );
        }
    }
}
