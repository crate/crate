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

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.*;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.operation.*;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.count.CountOperation;
import io.crate.operation.fetch.FetchContext;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.DistributingDownstreamFactory;
import io.crate.operation.projectors.FlatProjectorChain;
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
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.BitSet;
import java.util.concurrent.Executor;

@Singleton
public class ContextPreparer extends AbstractComponent {

    private final MapSideDataCollectOperation collectOperation;
    private final ESLogger pageDownstreamContextLogger;
    private final ESLogger nlContextLogger;
    private final ClusterService clusterService;
    private final CountOperation countOperation;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final DistributingDownstreamFactory distributingDownstreamFactory;
    private final InnerPreparer innerPreparer;
    private final ImplementationSymbolVisitor symbolVisitor;


    @Inject
    public ContextPreparer(Settings settings,
                           MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           CrateCircuitBreakerService breakerService,
                           CountOperation countOperation,
                           ThreadPool threadPool,
                           PageDownstreamFactory pageDownstreamFactory,
                           DistributingDownstreamFactory distributingDownstreamFactory,
                           Functions functions) {
        super(settings);
        nlContextLogger = Loggers.getLogger(NestedLoopContext.class, settings);
        pageDownstreamContextLogger = Loggers.getLogger(PageDownstreamContext.class, settings);
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        this.threadPool = threadPool;
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.distributingDownstreamFactory = distributingDownstreamFactory;
        innerPreparer = new InnerPreparer();
        symbolVisitor = new ImplementationSymbolVisitor(functions);
    }

    public List<ListenableFuture<Bucket>> prepareOnRemote(Iterable<? extends NodeOperation> nodeOperations,
                                                          JobExecutionContext.Builder contextBuilder,
                                                          SharedShardContexts sharedShardContexts) {
        PreparerContext preparerContext = initContext(nodeOperations, contextBuilder, sharedShardContexts);
        logger.trace("prepareOnRemote: nodeOperations={}, targetSourceMap={}", nodeOperations, preparerContext.opCtx.targetToSourceMap);

        for (IntCursor cursor : preparerContext.opCtx.findLeafs()) {
            contextBuilder.addAllSubContexts(prepareSourceOperations(cursor.value, preparerContext));
        }
        assert preparerContext.opCtx.allNodeOperationContextsBuilt() : "some nodeOperations haven't been processed";
        return preparerContext.directResponseFutures;
    }

    public List<ListenableFuture<Bucket>> prepareOnHandler(Iterable<? extends NodeOperation> nodeOperations,
                                                           JobExecutionContext.Builder contextBuilder,
                                                           List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases,
                                                           @Nullable SharedShardContexts sharedShardContexts) {
        PreparerContext preparerContext = initContext(nodeOperations, contextBuilder, sharedShardContexts);
        logger.trace("prepareOnHandler: nodeOperations={}, handlerPhases={}, targetSourceMap={}",
                nodeOperations, handlerPhases, preparerContext.opCtx.targetToSourceMap);

        IntHashSet leafs = new IntHashSet();
        for (Tuple<ExecutionPhase, RowReceiver> handlerPhase : handlerPhases) {
            ExecutionPhase phase = handlerPhase.v1();
            preparerContext.handlerRowReceivers.put(phase.executionPhaseId(), handlerPhase.v2());
            createContexts(phase, preparerContext);
            leafs.add(phase.executionPhaseId());
        }
        leafs.addAll(preparerContext.opCtx.findLeafs());
        for (IntCursor cursor : leafs) {
            contextBuilder.addAllSubContexts(prepareSourceOperations(cursor.value, preparerContext));
        }
        assert preparerContext.opCtx.allNodeOperationContextsBuilt() : "some nodeOperations haven't been processed";
        return preparerContext.directResponseFutures;
    }

    private Boolean createContexts(ExecutionPhase phase, PreparerContext preparerContext) {
        try {
            return innerPreparer.process(phase, preparerContext);
        } catch (Throwable t) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Couldn't create executionContexts from%n" +
                    "NodeOperations: %s%n" +
                    "target-sources: %s%n" +
                    "original-error: %s", preparerContext.opCtx.nodeOperationMap, preparerContext.opCtx.targetToSourceMap, t.getMessage()), t);
        }
    }

    private PreparerContext initContext(Iterable<? extends NodeOperation> nodeOperations,
                                        JobExecutionContext.Builder contextBuilder,
                                        @Nullable SharedShardContexts sharedShardContexts) {
        ContextPreparer.PreparerContext preparerContext = new PreparerContext(
                contextBuilder, logger, distributingDownstreamFactory, nodeOperations, sharedShardContexts);

        for (NodeOperation nodeOperation : nodeOperations) {
            // context for nodeOperations without dependencies can be built immediately (e.g. FetchPhase)
            if (nodeOperation.downstreamExecutionPhaseId() == NodeOperation.NO_DOWNSTREAM) {
                if(createContexts(nodeOperation.executionPhase(), preparerContext)) {
                    preparerContext.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().executionPhaseId());
                }
            }
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
                SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers);
                preparerContext.directResponseFutures.add(bucketBuilder.result());
                preparerContext.registerRowReceiver(nodeOperation.downstreamExecutionPhaseId(), bucketBuilder);
            }
        }
        return preparerContext;
    }

    /**
     * recursively build all contexts that depend on startPhaseId (excl. startPhaseId)
     *
     * {@link PreparerContext#opCtx#targetToSourceMap} will be used to traverse the nodeOperations
     */
    private Collection<ExecutionSubContext> prepareSourceOperations(int startPhaseId, PreparerContext preparerContext) {
        Collection<Integer> sourcePhaseIds = preparerContext.opCtx.targetToSourceMap.get(startPhaseId);
        if (sourcePhaseIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<ExecutionSubContext> subContexts = new ArrayList<>();
        for (Integer sourcePhaseId : sourcePhaseIds) {
            NodeOperation nodeOperation = preparerContext.opCtx.nodeOperationMap.get(sourcePhaseId);
            Boolean created = createContexts(nodeOperation.executionPhase(), preparerContext);
            assert created : "a subContext is required to be created";
            preparerContext.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().executionPhaseId());
        }
        for (Integer sourcePhaseId : sourcePhaseIds) {
            subContexts.addAll(prepareSourceOperations(sourcePhaseId, preparerContext));
        }
        return subContexts;
    }

    static class NodeOperationCtx {

        /**
         * a map from target phase to source phase
         * <p/>
         * For example with NodeOperations as the following:
         * <p/>
         * NodeOp {0, target=1}
         * NodeOp {1, target=2}
         * (handlerMergePhase (2))
         * <p/>
         * This map contains
         * <p/>
         * 1 -> 0
         * 2 -> 1
         * <p/>
         * This map is used in {@link #prepareSourceOperations(int, PreparerContext)} to process to NodeOperations in the
         * correct order (last ones in the data flow first - this is done so that the RowReceivers are always registered)
         * <p/>
         * (In the example above, NodeOp 0 might depend on the context/RowReceiver of NodeOp 1 being built first.)
         */
        private final Multimap<Integer, Integer> targetToSourceMap;
        private final ImmutableMap<Integer, ? extends NodeOperation> nodeOperationMap;
        private final BitSet builtNodeOperations;

        public NodeOperationCtx(Iterable<? extends NodeOperation> nodeOperations) {
            targetToSourceMap = createTargetToSourceMap(nodeOperations);
            nodeOperationMap = Maps.uniqueIndex(nodeOperations, new Function<NodeOperation, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable NodeOperation input) {
                    return input == null ? null : input.executionPhase().executionPhaseId();
                }
            });
            builtNodeOperations = new BitSet(nodeOperationMap.size());
        }

        static Multimap<Integer, Integer> createTargetToSourceMap(Iterable<? extends NodeOperation> nodeOperations) {
            HashMultimap<Integer, Integer> targetToSource = HashMultimap.create();
            for (NodeOperation nodeOperation : nodeOperations) {
                if (nodeOperation.downstreamExecutionPhaseId() == NodeOperation.NO_DOWNSTREAM) {
                    continue;
                }
                targetToSource.put(nodeOperation.downstreamExecutionPhaseId(), nodeOperation.executionPhase().executionPhaseId());
            }
            return targetToSource;
        }

        /**
         * find all phases that don't have any downstreams.
         *
         * This is usually only one phase (the handlerPhase, but there might be more in case of bulk operations)
         */
        private static IntCollection findLeafs(Multimap<Integer, Integer> targetToSourceMap) {
            IntArrayList leafs = new IntArrayList();
            for (Integer targetPhaseId : targetToSourceMap.keySet()) {
                if (!targetToSourceMap.containsValue(targetPhaseId)) {
                    leafs.add(targetPhaseId);
                }
            }
            return leafs;
        }

        public boolean upstreamsAreOnSameNode(int phaseId) {
            Collection<Integer> sourcePhases = targetToSourceMap.get(phaseId);
            if (sourcePhases.isEmpty()) {
                return false;
            }
            boolean sameNode = true;
            for (Integer sourcePhase : sourcePhases) {
                NodeOperation nodeOperation = nodeOperationMap.get(sourcePhase);
                if (nodeOperation == null) {
                    return false;
                }
                ExecutionPhase executionPhase = nodeOperation.executionPhase();
                sameNode = sameNode & executionPhase instanceof UpstreamPhase &&
                           (((UpstreamPhase) executionPhase).distributionInfo().distributionType() == DistributionType.SAME_NODE);
            }
            return sameNode;
        }

        public Iterable<? extends IntCursor> findLeafs() {
            return findLeafs(targetToSourceMap);
        }

        boolean allNodeOperationContextsBuilt() {
            return builtNodeOperations.cardinality() == nodeOperationMap.size();
        }
    }

    private static class PreparerContext {

        private final DistributingDownstreamFactory distributingDownstreamFactory;

        /**
         * from toKey(phaseId, inputId) to RowReceiver.
         */
        private final LongObjectMap<RowReceiver> phaseIdToRowReceivers = new LongObjectHashMap<>();
        private final IntObjectMap<RowReceiver> handlerRowReceivers = new IntObjectHashMap<>();

        @Nullable
        private final SharedShardContexts sharedShardContexts;

        private final List<ListenableFuture<Bucket>> directResponseFutures = new ArrayList<>();
        private final NodeOperationCtx opCtx;
        private final JobExecutionContext.Builder contextBuilder;
        private final ESLogger logger;

        PreparerContext(JobExecutionContext.Builder contextBuilder,
                        ESLogger logger,
                        DistributingDownstreamFactory distributingDownstreamFactory,
                        Iterable<? extends NodeOperation> nodeOperations,
                        @Nullable SharedShardContexts sharedShardContexts) {
            this.contextBuilder = contextBuilder;
            this.logger = logger;
            this.opCtx = new NodeOperationCtx(nodeOperations);
            this.distributingDownstreamFactory = distributingDownstreamFactory;
            this.sharedShardContexts = sharedShardContexts;
        }

        public UUID jobId() {
            return contextBuilder.jobId();
        }

        /**
         * Retrieve the rowReceiver of the downstream of phase
         */
        RowReceiver getRowReceiver(UpstreamPhase phase, int pageSize) {
            NodeOperation nodeOperation = opCtx.nodeOperationMap.get(phase.executionPhaseId());
            if (nodeOperation == null) {
                return handlerPhaseRowReceiver(phase.executionPhaseId());
            }

            RowReceiver targetRowReceiver = phaseIdToRowReceivers.get(
                    toKey(nodeOperation.downstreamExecutionPhaseId(), nodeOperation.downstreamExecutionPhaseInputId()));
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                traceGetRowReceiver(phase, "DIRECT_RESPONSE", nodeOperation, targetRowReceiver);
                return safeReceiver(targetRowReceiver, nodeOperation);
            }
            switch (phase.distributionInfo().distributionType()) {
                case SAME_NODE:
                    traceGetRowReceiver(phase, "SAME_NODE", nodeOperation, targetRowReceiver);
                    return safeReceiver(targetRowReceiver, nodeOperation);
                case BROADCAST:
                case MODULO:
                    RowReceiver downstream = distributingDownstreamFactory.create(
                            nodeOperation, phase.distributionInfo(), jobId(), pageSize);
                    traceGetRowReceiver(
                            phase, phase.distributionInfo().distributionType().toString(), nodeOperation, downstream);
                    return downstream;
                default:
                    throw new AssertionError("unhandled distributionType: " + phase.distributionInfo().distributionType());
            }
        }

        private void traceGetRowReceiver(UpstreamPhase phase,
                                         String distributionTypeName,
                                         NodeOperation nodeOperation,
                                         RowReceiver targetRowReceiver) {
            logger.trace("action=getRowReceiver, distributionType={}, phase={}, targetRowReceiver={}, target={}/{},",
                    distributionTypeName,
                    phase.executionPhaseId(),
                    targetRowReceiver,
                    nodeOperation.downstreamExecutionPhaseId(),
                    nodeOperation.downstreamExecutionPhaseInputId()
            );
        }

        private RowReceiver safeReceiver(RowReceiver targetRowReceiver, NodeOperation nodeOperation) {
            if (targetRowReceiver == null) {
                String msg =  String.format(Locale.ENGLISH,
                        "targetRowReceiver %d/%d must be on the same node as phase %d, but it is null",
                        nodeOperation.downstreamExecutionPhaseId(),
                        nodeOperation.downstreamExecutionPhaseInputId(),
                        nodeOperation.executionPhase().executionPhaseId());
                throw new IllegalStateException(msg);
            }
            return targetRowReceiver;
        }

        /**
         * The rowReceiver for handlerPhases got passed into {@link #prepareOnHandler(Iterable, JobExecutionContext.Builder, List, SharedShardContexts)}
         * and is registered there.
         *
         * Retrieve it
         */
        private RowReceiver handlerPhaseRowReceiver(int phaseId) {
            RowReceiver rowReceiver = handlerRowReceivers.get(phaseId);
            logger.trace("Using rowReceiver {} for phase {}, this is a leaf/handlerPhase", rowReceiver, phaseId);
            assert rowReceiver != null : "No rowReceiver for handlerPhase " + phaseId;
            return rowReceiver;
        }

        void registerRowReceiver(int phaseId, RowReceiver rowReceiver) {
            phaseIdToRowReceivers.put(toKey(phaseId, (byte) 0), rowReceiver);
        }

        void registerSubContext(ExecutionSubContext subContext){
            contextBuilder.addSubContext(subContext);
        }

    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, Boolean> {

        @Override
        public Boolean visitCountPhase(final CountPhase phase, final PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = phase.routing().locations();
            String localNodeId = clusterService.localNode().id();
            final Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countNode doesn't contain the current nodeId");
            }

            RowReceiver rowReceiver = context.getRowReceiver(phase, 0);
            context.registerSubContext(new CountContext(
                    phase.executionPhaseId(),
                    countOperation,
                    rowReceiver,
                    indexShardMap,
                    phase.whereClause()
            ));
            return true;
        }

        @Override
        public Boolean visitMergePhase(final MergePhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);

            boolean upstreamOnSameNode = context.opCtx.upstreamsAreOnSameNode(phase.executionPhaseId());

            int pageSize = Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.executionNodes().size());
            RowReceiver rowReceiver = context.getRowReceiver(phase, pageSize);

            if (upstreamOnSameNode) {
                if (!phase.projections().isEmpty()) {
                    ProjectorChainContext projectorChainContext = new ProjectorChainContext(
                            phase.executionPhaseId(),
                            phase.name(),
                            context.jobId(),
                            pageDownstreamFactory.projectorFactory(),
                            phase.projections(),
                            rowReceiver,
                            ramAccountingContext);
                    context.registerRowReceiver(phase.executionPhaseId(), projectorChainContext.rowReceiver());
                    context.registerSubContext(projectorChainContext);
                    return true;
                }

                context.registerRowReceiver(phase.executionPhaseId(), rowReceiver);
                return false;
            }

            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                    pageDownstreamFactory.createMergeNodePageDownstream(
                            phase,
                            rowReceiver,
                            false,
                            ramAccountingContext,
                            // no separate executor because TransportDistributedResultAction already runs in a threadPool
                            Optional.<Executor>absent());


            context.registerSubContext(new PageDownstreamContext(
                    pageDownstreamContextLogger,
                    nodeName(),
                    phase.executionPhaseId(),
                    phase.name(),
                    pageDownstreamProjectorChain.v1(),
                    DataTypes.getStreamer(phase.inputTypes()),
                    ramAccountingContext,
                    phase.numUpstreams(),
                    pageDownstreamProjectorChain.v2()));
            return true;
        }


        @Override
        public Boolean visitRoutedCollectPhase(final RoutedCollectPhase phase, final PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            RowReceiver rowReceiver = context.getRowReceiver(phase,
                    MoreObjects.firstNonNull(phase.nodePageSizeHint(), Paging.PAGE_SIZE));
            context.registerSubContext(new JobCollectContext(
                    phase,
                    collectOperation,
                    clusterService.state().nodes().localNodeId(),
                    ramAccountingContext,
                    rowReceiver,
                    context.sharedShardContexts
            ));
            return true;
        }

        @Override
        public Boolean visitCollectPhase(CollectPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            RowReceiver rowReceiver = context.getRowReceiver(phase, Paging.PAGE_SIZE);
            context.registerSubContext(new JobCollectContext(
                    phase,
                    collectOperation,
                    clusterService.state().nodes().localNodeId(),
                    ramAccountingContext,
                    rowReceiver,
                    context.sharedShardContexts
            ));
            return true;
        }

        @Override
        public Boolean visitFetchPhase(final FetchPhase phase, final PreparerContext context) {
            final FluentIterable<Routing> routings = FluentIterable.from(context.opCtx.nodeOperationMap.values())
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
                            if (input instanceof RoutedCollectPhase) {
                                return ((RoutedCollectPhase) input).routing();
                            }
                            return null;
                        }
                    }).filter(Predicates.notNull());

            String localNodeId = clusterService.localNode().id();
            context.registerSubContext(new FetchContext(
                    phase,
                    localNodeId,
                    context.sharedShardContexts,
                    routings));
            return true;
        }

        @Override
        public Boolean visitNestedLoopPhase(NestedLoopPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            RowReceiver downstreamRowReceiver = context.getRowReceiver(phase, Paging.PAGE_SIZE);

            FlatProjectorChain flatProjectorChain;
            if (!phase.projections().isEmpty()) {
                flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                        pageDownstreamFactory.projectorFactory(),
                        ramAccountingContext,
                        phase.projections(),
                        downstreamRowReceiver,
                        phase.jobId()
                );
            } else {
                flatProjectorChain = FlatProjectorChain.withReceivers(Collections.singletonList(downstreamRowReceiver));
            }

            Predicate<Row> rowFilter = RowFilter.create(symbolVisitor, phase.filterSymbol());
            Predicate<Row> joinCondition = RowFilter.create(symbolVisitor, phase.joinCondition());

            NestedLoopOperation nestedLoopOperation = new NestedLoopOperation(
                phase.executionPhaseId(),
                flatProjectorChain.firstProjector(),
                rowFilter,
                joinCondition,
                phase.joinType(),
                phase.numLeftOutputs(),
                phase.numRightOutputs());
            PageDownstreamContext left = pageDownstreamContextForNestedLoop(
                phase.executionPhaseId(),
                context,
                (byte) 0,
                phase.leftMergePhase(),
                nestedLoopOperation.leftRowReceiver(),
                ramAccountingContext);
            if (left != null) {
                context.registerSubContext(left);
            }
            PageDownstreamContext right = pageDownstreamContextForNestedLoop(
                phase.executionPhaseId(),
                context,
                (byte) 1,
                phase.rightMergePhase(),
                nestedLoopOperation.rightRowReceiver(),
                ramAccountingContext
            );
            if (right != null) {
                context.registerSubContext(right);
            }
            context.registerSubContext(new NestedLoopContext(
                    nlContextLogger,
                    phase,
                    flatProjectorChain,
                    nestedLoopOperation,
                left,
                right
            ));
            return true;
        }

        @Nullable
        private PageDownstreamContext pageDownstreamContextForNestedLoop(int nlPhaseId,
                                                                         PreparerContext ctx,
                                                                         byte inputId,
                                                                         @Nullable MergePhase mergePhase,
                                                                         RowReceiver downstream,
                                                                         RamAccountingContext ramAccountingContext) {
            if (mergePhase == null) {
                ctx.phaseIdToRowReceivers.put(toKey(nlPhaseId, inputId), downstream);
                return null;
            }
            Tuple<PageDownstream, FlatProjectorChain> pageDownstreamWithChain = pageDownstreamFactory.createMergeNodePageDownstream(
                    mergePhase,
                    downstream,
                    true,
                    ramAccountingContext,
                    Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
            );
            return new PageDownstreamContext(
                    pageDownstreamContextLogger,
                    nodeName(),
                    mergePhase.executionPhaseId(),
                    mergePhase.name(),
                    pageDownstreamWithChain.v1(),
                    StreamerVisitor.streamerFromOutputs(mergePhase),
                    ramAccountingContext,
                    mergePhase.numUpstreams(),
                    pageDownstreamWithChain.v2()
            );
        }
    }

    private static long toKey(int phaseId, byte inputId) {
        long l = (long) phaseId;
        return (l << 32) | (inputId & 0xffffffffL);
    }
}
