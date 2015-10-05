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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.*;
import io.crate.metadata.Routing;
import io.crate.operation.NodeOperation;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.Paging;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.fetch.FetchContext;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.RowDownstreamFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Executor;

@Singleton
public class ContextPreparer {

    private static final ESLogger LOGGER = Loggers.getLogger(ContextPreparer.class);

    private final MapSideDataCollectOperation collectOperation;
    private ClusterService clusterService;
    private CountOperation countOperation;
    private final CircuitBreaker circuitBreaker;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final RowDownstreamFactory rowDownstreamFactory;
    private final InnerPreparer innerPreparer;

    @Inject
    public ContextPreparer(MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           CountOperation countOperation,
                           PageDownstreamFactory pageDownstreamFactory,
                           RowDownstreamFactory rowDownstreamFactory) {
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.rowDownstreamFactory = rowDownstreamFactory;
        innerPreparer = new InnerPreparer();
    }

    public List<ListenableFuture<Bucket>> prepareOnRemote(UUID jobId,
                                                          Iterable<? extends NodeOperation> nodeOperations,
                                                          JobExecutionContext.Builder contextBuilder,
                                                          SharedShardContexts sharedShardContexts) {
        PreparerContext preparerContext = new PreparerContext(jobId, rowDownstreamFactory, nodeOperations,
                sharedShardContexts);
        List<ListenableFuture<Bucket>> directResponseFutures = new ArrayList<>();
        processDownstreamExecutionPhaseIds(nodeOperations, preparerContext);

        List<NodeOperation> reversedNodeOperations = Lists.reverse(Lists.newArrayList(nodeOperations));
        for (NodeOperation nodeOperation : reversedNodeOperations) {
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
                SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers);
                directResponseFutures.add(bucketBuilder.result());
                preparerContext.registerRowReceiverForUpstreamPhase(nodeOperation.executionPhase(), bucketBuilder);
            }
            processExecutionPhase(nodeOperation.executionPhase(), preparerContext, contextBuilder);
        }
        postPrepare(contextBuilder, preparerContext);

        return directResponseFutures;
    }

    public List<ExecutionSubContext> prepareOnHandler(UUID jobId,
                                                      Iterable<? extends NodeOperation> nodeOperations,
                                                      JobExecutionContext.Builder contextBuilder,
                                                      List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases,
                                                      @Nullable SharedShardContexts sharedShardContexts) {
        ContextPreparer.PreparerContext preparerContext = new PreparerContext(jobId, rowDownstreamFactory,
                nodeOperations, sharedShardContexts);
        processDownstreamExecutionPhaseIds(nodeOperations, preparerContext);


        // register handler phase row receiver
        // and build handler context, must be done first because it's downstream is already known
        // and it is needed as a row receiver by others
        List<ExecutionSubContext> handlerContexts = new ArrayList<>(handlerPhases.size());
        for (Tuple<ExecutionPhase, RowReceiver> handlerPhase : handlerPhases) {
            ExecutionPhase handlerExecutionPhase = handlerPhase.v1();
            preparerContext.registerRowReceiverForUpstreamPhase(handlerExecutionPhase, handlerPhase.v2());
            ExecutionSubContext finalLocalMergeContext = innerPreparer.process(handlerExecutionPhase, preparerContext);
            if (finalLocalMergeContext != null) {
                contextBuilder.addSubContext(finalLocalMergeContext);
                handlerContexts.add(finalLocalMergeContext);
            }
        }
        List<NodeOperation> reversedNodeOperations = Lists.reverse(Lists.newArrayList(nodeOperations));
        for (NodeOperation nodeOperation : reversedNodeOperations) {
            processExecutionPhase(nodeOperation.executionPhase(), preparerContext, contextBuilder);
        }
        postPrepare(contextBuilder, preparerContext);
        return handlerContexts;
    }


    /**
     * Build all contexts which could not build in first iteration due to missing downstreams
     */
    private void postPrepare(JobExecutionContext.Builder contextBuilder,
                             PreparerContext preparerContext) {


        /**
         * infinite loop protection
         * if a phase has its upstream on the same node it might need to be processes 2 times
         * (the first time it might be skipped if the upstream hasn't been processed yet)
         */
        int reProcessLimit = preparerContext.executionPhasesToProcess.size() * 2;
        for (int i = 0; i < reProcessLimit && !preparerContext.executionPhasesToProcess.isEmpty(); i++) {
            List<ExecutionPhase> executionPhasesToProcess = Lists.newArrayList(preparerContext.executionPhasesToProcess);
            preparerContext.executionPhasesToProcess.clear();
            for (ExecutionPhase executionPhase : executionPhasesToProcess) {
                processExecutionPhase(executionPhase, preparerContext, contextBuilder);
            }
        }
        if (!preparerContext.executionPhasesToProcess.isEmpty()) {
            throw new IllegalStateException("Aborted context preparation as an infinite loop was detected");
        }
    }

    private void processExecutionPhase(ExecutionPhase executionPhase,
                                       PreparerContext preparerContext,
                                       JobExecutionContext.Builder contextBuilder) {
        ExecutionSubContext subContext = innerPreparer.process(executionPhase, preparerContext);
        if (subContext != null) {
            contextBuilder.addSubContext(subContext);
        }
    }

    private void processDownstreamExecutionPhaseIds(Iterable<? extends NodeOperation> nodeOperations,
                                                    PreparerContext context) {
        for (NodeOperation nodeOperation : nodeOperations) {
            boolean val = false;
            ExecutionPhase phase = nodeOperation.executionPhase();
            if (phase instanceof UpstreamPhase) {
                val = isSameNodeUpstreamDistributionType((UpstreamPhase) phase);
            }
            context.setPhaseHasSameNodeUpstream(
                    nodeOperation.downstreamExecutionPhaseId(),
                    nodeOperation.downstreamExecutionPhaseInputId(),
                    val);
            context.setNodeOperation(nodeOperation.executionPhase().executionPhaseId(), nodeOperation);
        }
    }

    private boolean isSameNodeUpstreamDistributionType(UpstreamPhase phase) {
        return phase.distributionInfo().distributionType() == DistributionType.SAME_NODE;
    }

    private static class PreparerContext {

        private final UUID jobId;
        private final RowDownstreamFactory rowDownstreamFactory;
        private final Map<Tuple<Integer, Byte>, Boolean> phaseHasSameNodeUpstream = new HashMap<>();
        private final IntObjectOpenHashMap<NodeOperation> phaseIdToNodeOperations = new IntObjectOpenHashMap<>();
        private final IntObjectOpenHashMap<RowReceiver> phaseIdToRowReceivers = new IntObjectOpenHashMap<>();
        private final List<ExecutionPhase> executionPhasesToProcess = new ArrayList<>();
        private final Iterable<? extends NodeOperation> nodeOperations;

        @Nullable
        private final SharedShardContexts sharedShardContexts;

        public PreparerContext(UUID jobId,
                               RowDownstreamFactory rowDownstreamFactory,
                               Iterable<? extends NodeOperation> nodeOperations,
                               @Nullable SharedShardContexts sharedShardContexts) {
            this.jobId = jobId;
            this.rowDownstreamFactory = rowDownstreamFactory;
            this.nodeOperations = nodeOperations;
            this.sharedShardContexts = sharedShardContexts;
        }

        public boolean getPhaseHasSameNodeUpstream(int executionPhaseId, byte inputId) {
            Tuple<Integer, Byte> key = new Tuple<>(executionPhaseId, inputId);
            Boolean res = phaseHasSameNodeUpstream.get(key);
            if (res == null) {
                return false;
            }
            return res;
        }

        public void setPhaseHasSameNodeUpstream(int executionPhaseId, byte inputId, boolean val) {
            Tuple<Integer, Byte> key = new Tuple<>(executionPhaseId, inputId);
            phaseHasSameNodeUpstream.put(key, val);
        }

        public NodeOperation getNodeOperation(int executionPhaseId) {
            NodeOperation nodeOperation = phaseIdToNodeOperations.get(executionPhaseId);
            if (nodeOperation == null) {
                throw new IllegalStateException("NodeOperation not found, must be registered first");
            }
            return nodeOperation;
        }

        public void setNodeOperation(int executionPhaseId, NodeOperation nodeOperation) {
            phaseIdToNodeOperations.put(executionPhaseId, nodeOperation);
        }

        /**
         * Register a {@link RowReceiver} for an {@link UpstreamPhase}
         */
        public void registerRowReceiverForUpstreamPhase(ExecutionPhase executionPhase, RowReceiver rowReceiver) {
            assert executionPhase instanceof UpstreamPhase : "Given ExecutionPhase is not a UpstreamPhase";
            phaseIdToRowReceivers.put(executionPhase.executionPhaseId(), rowReceiver);
        }

        /**
         * Register a {@link RowReceiver} of a downstream {@link ExecutionPhase}
         */
        public void registerRowReceiver(int downstreamExecutionPhaseId,
                                        byte downstreamExecutionPhaseInputId,
                                        RowReceiver rowReceiver) {
            for (IntObjectCursor<NodeOperation> cursor : phaseIdToNodeOperations) {
                NodeOperation nodeOperation = cursor.value;
                if (nodeOperation.downstreamExecutionPhaseId() == downstreamExecutionPhaseId
                        && nodeOperation.downstreamExecutionPhaseInputId() == downstreamExecutionPhaseInputId) {
                    registerRowReceiverForUpstreamPhase(nodeOperation.executionPhase(), rowReceiver);
                }
            }
        }

        @Nullable
        public RowReceiver getRowReceiver(UpstreamPhase upstreamPhase, int pageSize) {
            if (upstreamPhase.distributionInfo().distributionType() == DistributionType.SAME_NODE) {
                LOGGER.trace("Phase uses SAME_NODE downstream: {}", upstreamPhase);
                return phaseIdToRowReceivers.get(upstreamPhase.executionPhaseId());
            }
            NodeOperation nodeOperation = getNodeOperation(upstreamPhase.executionPhaseId());
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                LOGGER.trace("Phase uses DIRECT_RESPONSE downstream: {}", upstreamPhase);
                return phaseIdToRowReceivers.get(upstreamPhase.executionPhaseId());
            }
            LOGGER.trace("Phase uses DISTRIBUTED downstream: {}", upstreamPhase);
            return rowDownstreamFactory.createDownstream(
                    nodeOperation,
                    upstreamPhase.distributionInfo(),
                    jobId,
                    pageSize);

        }

        public Iterable<? extends NodeOperation> nodeOperations() {
            return nodeOperations;
        }
    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, ExecutionSubContext> {

        @Override
        public ExecutionSubContext visitCountPhase(final CountPhase phase, final PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = phase.routing().locations();
            if (locations == null) {
                throw new IllegalArgumentException("locations are empty. Can't start count operation");
            }
            String localNodeId = clusterService.localNode().id();
            final Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countNode doesn't contain the current nodeId");
            }

            RowReceiver rowReceiver = context.getRowReceiver(phase, 0);
            if (rowReceiver == null) {
                context.executionPhasesToProcess.add(phase);
                return null;
            }

            return new CountContext(
                    phase.executionPhaseId(),
                    countOperation,
                    rowReceiver,
                    indexShardMap,
                    phase.whereClause()
            );
        }

        @Override
        public ExecutionSubContext visitMergePhase(final MergePhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            boolean upstreamOnSameNode = context.getPhaseHasSameNodeUpstream(phase.executionPhaseId(), (byte) 0);

            int pageSize = Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.executionNodes().size());
            RowReceiver rowReceiver = context.getRowReceiver(phase, pageSize);
            if (rowReceiver == null) {
                context.executionPhasesToProcess.add(phase);
                return null;
            }

            if (upstreamOnSameNode) {
                if (!phase.projections().isEmpty()) {
                    ProjectorChainContext projectorChainContext = new ProjectorChainContext(
                            phase.executionPhaseId(),
                            phase.name(),
                            context.jobId,
                            pageDownstreamFactory.projectorFactory(),
                            phase.projections(),
                            rowReceiver,
                            ramAccountingContext);
                    context.registerRowReceiver(phase.executionPhaseId(), (byte) 0, projectorChainContext.rowReceiver());
                    return projectorChainContext;
                }

                context.registerRowReceiver(phase.executionPhaseId(), (byte) 0, rowReceiver);
                return null;
            }

            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            phase,
                            rowReceiver,
                            ramAccountingContext,
                            // no separate executor because TransportDistributedResultAction already runs in a threadPool
                            Optional.<Executor>absent());


            return new PageDownstreamContext(
                    phase.executionPhaseId(),
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

            final int pageSize = Paging.getWeightedPageSize(
                    MoreObjects.firstNonNull(phase.limit(), Paging.PAGE_SIZE),
                    (1.0 / numTotalShards) * numShardsOnNode
            );
            LOGGER.trace("[{}] setting node page size to: {}, totalShards: {}, localShards: {}",
                    localNodeId, pageSize, numTotalShards, numShardsOnNode);

            RowReceiver rowReceiver = context.getRowReceiver(phase, pageSize);
            if (rowReceiver == null) {
                context.executionPhasesToProcess.add(phase);
                return null;
            }

            return new JobCollectContext(
                    phase,
                    collectOperation,
                    ramAccountingContext,
                    rowReceiver,
                    context.sharedShardContexts
            );
        }

        @Override
        public ExecutionSubContext visitFetchPhase(final FetchPhase phase, final PreparerContext context) {
            final FluentIterable<Routing> routings = FluentIterable.from(context.nodeOperations())
                    .transform(new Function<NodeOperation, ExecutionPhase>() {
                @Nullable
                @Override
                public ExecutionPhase apply(NodeOperation input) {
                    return input.executionPhase();
                }
            }).transform(new Function<ExecutionPhase, Routing>() {
                        @Nullable
                        @Override
                        public Routing apply(@Nullable ExecutionPhase input) {
                            if (input == null) {
                                return null;
                            }
                            if (phase.collectPhaseIds().contains(input.executionPhaseId())) {
                                assert input instanceof CollectPhase :
                                        "fetchPhase.collectPhaseIds must only contain ids of executionPhases that are an instanceof CollectPhase";
                                return ((CollectPhase) input).routing();
                            }
                            return null;
                        }
                    }).filter(Predicates.notNull());

            String localNodeId = clusterService.localNode().id();
            return new FetchContext(
                    phase.executionPhaseId(),
                    localNodeId,
                    context.sharedShardContexts,
                    routings,
                    phase.bases());
        }
    }
}
