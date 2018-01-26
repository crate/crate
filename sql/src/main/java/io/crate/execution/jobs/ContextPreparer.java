/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.jobs;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.base.MoreObjects;
import io.crate.Streamer;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.engine.distribution.SingleBucketBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.expression.InputFactory;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.support.Paging;
import io.crate.expression.RowFilter;
import io.crate.execution.engine.collect.JobCollectContext;
import io.crate.execution.engine.collect.MapSideDataCollectOperation;
import io.crate.execution.engine.collect.sources.SystemCollectSource;
import io.crate.execution.engine.collect.count.CountOperation;
import io.crate.execution.engine.fetch.FetchContext;
import io.crate.execution.engine.join.NestedLoopOperation;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.execution.engine.distribution.DistributingConsumerFactory;
import io.crate.execution.engine.pipeline.ProjectingRowConsumer;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.planner.distribution.DistributionType;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

@Singleton
public class ContextPreparer extends AbstractComponent {

    private final MapSideDataCollectOperation collectOperation;
    private final Logger pageDownstreamContextLogger;
    private final Logger nlContextLogger;
    private final ClusterService clusterService;
    private final CountOperation countOperation;
    private final CircuitBreaker circuitBreaker;
    private final DistributingConsumerFactory distributingConsumerFactory;
    private final InnerPreparer innerPreparer;
    private final InputFactory inputFactory;
    private final ProjectorFactory projectorFactory;
    private final PKLookupOperation pkLookupOperation;

    @Inject
    public ContextPreparer(Settings settings,
                           MapSideDataCollectOperation collectOperation,
                           ClusterService clusterService,
                           NodeJobsCounter nodeJobsCounter,
                           CrateCircuitBreakerService breakerService,
                           CountOperation countOperation,
                           ThreadPool threadPool,
                           DistributingConsumerFactory distributingConsumerFactory,
                           TransportActionProvider transportActionProvider,
                           IndicesService indicesService,
                           Functions functions,
                           SystemCollectSource systemCollectSource,
                           BigArrays bigArrays) {
        super(settings);
        nlContextLogger = Loggers.getLogger(NestedLoopContext.class, settings);
        pageDownstreamContextLogger = Loggers.getLogger(PageDownstreamContext.class, settings);
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.countOperation = countOperation;
        this.pkLookupOperation = new PKLookupOperation(indicesService);
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        this.distributingConsumerFactory = distributingConsumerFactory;
        innerPreparer = new InnerPreparer();
        inputFactory = new InputFactory(functions);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        this.projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            functions,
            threadPool,
            settings,
            transportActionProvider,
            inputFactory,
            normalizer,
            systemCollectSource::getRowUpdater,
            systemCollectSource::tableDefinition,
            bigArrays
        );
    }

    public List<CompletableFuture<Bucket>> prepareOnRemote(Collection<? extends NodeOperation> nodeOperations,
                                                           JobExecutionContext.Builder contextBuilder,
                                                           SharedShardContexts sharedShardContexts) {
        ContextPreparer.PreparerContext preparerContext = new PreparerContext(
            clusterService.localNode().getId(),
            contextBuilder,
            logger,
            distributingConsumerFactory,
            nodeOperations,
            sharedShardContexts);
        registerContextPhases(nodeOperations, preparerContext);
        logger.trace("prepareOnRemote: nodeOperations={}, targetSourceMap={}",
            nodeOperations, preparerContext.opCtx.targetToSourceMap);

        for (IntCursor cursor : preparerContext.opCtx.findLeafs()) {
            prepareSourceOperations(cursor.value, preparerContext);
        }
        assert preparerContext.opCtx.allNodeOperationContextsBuilt() : "some nodeOperations haven't been processed";
        return preparerContext.directResponseFutures;
    }

    public List<CompletableFuture<Bucket>> prepareOnHandler(Collection<? extends NodeOperation> nodeOperations,
                                                            JobExecutionContext.Builder contextBuilder,
                                                            List<Tuple<ExecutionPhase, RowConsumer>> handlerPhases,
                                                            SharedShardContexts sharedShardContexts) {
        ContextPreparer.PreparerContext preparerContext = new PreparerContext(
            clusterService.localNode().getId(),
            contextBuilder,
            logger,
            distributingConsumerFactory,
            nodeOperations,
            sharedShardContexts);
        for (Tuple<ExecutionPhase, RowConsumer> handlerPhase : handlerPhases) {
            preparerContext.registerLeaf(handlerPhase.v1(), handlerPhase.v2());
        }
        registerContextPhases(nodeOperations, preparerContext);
        logger.trace("prepareOnHandler: nodeOperations={}, handlerPhases={}, targetSourceMap={}",
            nodeOperations, handlerPhases, preparerContext.opCtx.targetToSourceMap);

        IntHashSet leafs = new IntHashSet();
        for (Tuple<ExecutionPhase, RowConsumer> handlerPhase : handlerPhases) {
            ExecutionPhase phase = handlerPhase.v1();
            createContexts(phase, preparerContext);
            leafs.add(phase.phaseId());
        }
        leafs.addAll(preparerContext.opCtx.findLeafs());
        for (IntCursor cursor : leafs) {
            prepareSourceOperations(cursor.value, preparerContext);
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
                "Leafs: %s%n" +
                "target-sources: %s%n" +
                "original-error: %s",
                preparerContext.opCtx.nodeOperationByPhaseId,
                preparerContext.leafs,
                preparerContext.opCtx.targetToSourceMap,
                t.getClass().getSimpleName() + ": " + t.getMessage()),
                t);
        }
    }

    private void registerContextPhases(Iterable<? extends NodeOperation> nodeOperations,
                                       PreparerContext preparerContext) {
        for (NodeOperation nodeOperation : nodeOperations) {
            // context for nodeOperations without dependencies can be built immediately (e.g. FetchPhase)
            if (nodeOperation.downstreamExecutionPhaseId() == NodeOperation.NO_DOWNSTREAM) {
                logger.trace("Building context for nodeOp without downstream: {}", nodeOperation);
                if (createContexts(nodeOperation.executionPhase(), preparerContext)) {
                    preparerContext.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().phaseId());
                }
            }
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                Streamer<?>[] streamers = StreamerVisitor.streamersFromOutputs(nodeOperation.executionPhase());
                SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers);
                preparerContext.directResponseFutures.add(bucketBuilder.completionFuture());
                preparerContext.registerBatchConsumer(nodeOperation.downstreamExecutionPhaseId(), bucketBuilder);
            }
        }
    }

    /**
     * recursively build all contexts that depend on startPhaseId (excl. startPhaseId)
     * <p>
     * {@link PreparerContext#opCtx#targetToSourceMap} will be used to traverse the nodeOperations
     */
    private void prepareSourceOperations(int startPhaseId, PreparerContext preparerContext) {
        IntContainer sourcePhaseIds = preparerContext.opCtx.targetToSourceMap.get(startPhaseId);
        if (sourcePhaseIds == null) {
            return;
        }
        for (IntCursor sourcePhaseId : sourcePhaseIds) {
            NodeOperation nodeOperation = preparerContext.opCtx.nodeOperationByPhaseId.get(sourcePhaseId.value);
            Boolean created = createContexts(nodeOperation.executionPhase(), preparerContext);
            assert created : "a subContext is required to be created";
            preparerContext.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().phaseId());
        }
        for (IntCursor sourcePhaseId : sourcePhaseIds) {
            prepareSourceOperations(sourcePhaseId.value, preparerContext);
        }
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
        private final IntObjectMap<? extends IntContainer> targetToSourceMap;
        private final IntObjectMap<NodeOperation> nodeOperationByPhaseId;
        private final BitSet builtNodeOperations;
        private final String localNodeId;

        public NodeOperationCtx(String localNodeId, Collection<? extends NodeOperation> nodeOperations) {
            this.localNodeId = localNodeId;
            targetToSourceMap = createTargetToSourceMap(nodeOperations);
            nodeOperationByPhaseId = groupNodeOperationsByPhase(nodeOperations);
            builtNodeOperations = new BitSet(nodeOperationByPhaseId.size());
        }

        private static IntObjectHashMap<NodeOperation> groupNodeOperationsByPhase(Collection<? extends NodeOperation> nodeOperations) {
            IntObjectHashMap<NodeOperation> map = new IntObjectHashMap<>(nodeOperations.size());
            for (NodeOperation nodeOperation : nodeOperations) {
                map.put(nodeOperation.executionPhase().phaseId(), nodeOperation);
            }
            return map;
        }

        static IntObjectHashMap<? extends IntContainer> createTargetToSourceMap(Iterable<? extends NodeOperation> nodeOperations) {
            IntObjectHashMap<IntArrayList> targetToSource = new IntObjectHashMap<>();
            for (NodeOperation nodeOp : nodeOperations) {
                if (nodeOp.downstreamExecutionPhaseId() == NodeOperation.NO_DOWNSTREAM) {
                    continue;
                }
                IntArrayList sourceIds = targetToSource.get(nodeOp.downstreamExecutionPhaseId());
                if (sourceIds == null) {
                    sourceIds = new IntArrayList();
                    targetToSource.put(nodeOp.downstreamExecutionPhaseId(), sourceIds);
                }
                sourceIds.add(nodeOp.executionPhase().phaseId());
            }
            return targetToSource;
        }

        /**
         * find all phases that don't have any downstreams.
         * <p>
         * This is usually only one phase (the handlerPhase, but there might be more in case of bulk operations)
         * <pre>
         *
         *     flow:            targetToSourceMap:
         *
         *     0    1               2 -> [0, 1]
         *      \  /                3 -> [2]
         *       2
         *       |
         *       3
         *
         *     leafs = all keys in targetToSource map which have no entry in values (3 in the example above)
         * </pre>
         */
        private static IntCollection findLeafs(IntObjectMap<? extends IntContainer> targetToSourceMap) {
            IntArrayList leafs = new IntArrayList();
            BitSet sources = new BitSet();
            for (IntObjectCursor<? extends IntContainer> sourceIds : targetToSourceMap) {
                for (IntCursor sourceId : sourceIds.value) {
                    sources.set(sourceId.value);
                }
            }
            for (IntCursor targetPhaseCursor : targetToSourceMap.keys()) {
                int targetPhase = targetPhaseCursor.value;
                if (!sources.get(targetPhase)) {
                    leafs.add(targetPhase);
                }
            }
            return leafs;
        }

        public boolean upstreamsAreOnSameNode(int phaseId) {
            IntContainer sourcePhases = targetToSourceMap.get(phaseId);
            if (sourcePhases == null) {
                return false;
            }
            for (IntCursor sourcePhase : sourcePhases) {
                NodeOperation nodeOperation = nodeOperationByPhaseId.get(sourcePhase.value);
                if (nodeOperation == null) {
                    return false;
                }
                ExecutionPhase executionPhase = nodeOperation.executionPhase();
                // explicit SAME_NODE distribution enforced by the planner
                if (executionPhase instanceof UpstreamPhase &&
                    ((UpstreamPhase) executionPhase).distributionInfo().distributionType() == DistributionType.SAME_NODE) {
                    continue;
                }

                // implicit same node optimization because the upstreamPhase is running ONLY on this node
                Collection<String> executionNodes = executionPhase.nodeIds();
                if (executionNodes.size() == 1 && executionNodes.iterator().next().equals(localNodeId)) {
                    continue;
                }
                return false;
            }
            return true;
        }

        public Iterable<? extends IntCursor> findLeafs() {
            return findLeafs(targetToSourceMap);
        }

        boolean allNodeOperationContextsBuilt() {
            return builtNodeOperations.cardinality() == nodeOperationByPhaseId.size();
        }
    }

    private static class PreparerContext {

        private final DistributingConsumerFactory distributingConsumerFactory;

        /**
         * from toKey(phaseId, inputId) to BatchConsumer.
         */
        private final LongObjectMap<RowConsumer> consumersByPhaseInputId = new LongObjectHashMap<>();
        private final LongObjectMap<RamAccountingContext> ramAccountingContextByPhaseInputId = new LongObjectHashMap<>();
        private final IntObjectMap<RowConsumer> handlerConsumersByPhaseId = new IntObjectHashMap<>();

        private final SharedShardContexts sharedShardContexts;

        private final List<CompletableFuture<Bucket>> directResponseFutures = new ArrayList<>();
        private final NodeOperationCtx opCtx;
        private final JobExecutionContext.Builder contextBuilder;
        private final Logger logger;
        private final List<ExecutionPhase> leafs = new ArrayList<>();

        PreparerContext(String localNodeId,
                        JobExecutionContext.Builder contextBuilder,
                        Logger logger,
                        DistributingConsumerFactory distributingConsumerFactory,
                        Collection<? extends NodeOperation> nodeOperations,
                        SharedShardContexts sharedShardContexts) {
            this.contextBuilder = contextBuilder;
            this.logger = logger;
            this.opCtx = new NodeOperationCtx(localNodeId, nodeOperations);
            this.distributingConsumerFactory = distributingConsumerFactory;
            this.sharedShardContexts = sharedShardContexts;
        }

        public UUID jobId() {
            return contextBuilder.jobId();
        }

        /**
         * Retrieve the rowReceiver of the downstream of phase
         */
        RowConsumer getRowConsumer(UpstreamPhase phase, int pageSize) {
            NodeOperation nodeOperation = opCtx.nodeOperationByPhaseId.get(phase.phaseId());
            if (nodeOperation == null) {
                return handlerPhaseConsumer(phase.phaseId());
            }

            long phaseIdKey = toKey(nodeOperation.downstreamExecutionPhaseId(), nodeOperation.downstreamExecutionPhaseInputId());
            RowConsumer rowConsumer = consumersByPhaseInputId.get(phaseIdKey);
            if (rowConsumer != null) {
                // targetBatchConsumer is available because of same node optimization or direct result;
                return rowConsumer;
            }

            DistributionType distributionType = phase.distributionInfo().distributionType();
            switch (distributionType) {
                case BROADCAST:
                case MODULO:
                    RowConsumer consumer = distributingConsumerFactory.create(
                        nodeOperation, phase.distributionInfo(), jobId(), pageSize);
                    traceGetBatchConsumer(phase, distributionType.toString(), nodeOperation, consumer);
                    return consumer;

                default:
                    throw new AssertionError(
                        "unhandled distributionType: " + distributionType);
            }
        }

        private void traceGetBatchConsumer(UpstreamPhase phase,
                                           String distributionTypeName,
                                           NodeOperation nodeOperation,
                                           RowConsumer consumer) {
            logger.trace("action=getRowReceiver, distributionType={}, phase={}, targetConsumer={}, target={}/{},",
                distributionTypeName,
                phase.phaseId(),
                consumer,
                nodeOperation.downstreamExecutionPhaseId(),
                nodeOperation.downstreamExecutionPhaseInputId()
            );
        }

        /**
         * The rowReceiver for handlerPhases got passed into {@link #prepareOnHandler(Collection, JobExecutionContext.Builder, List, SharedShardContexts)}
         * and is registered there.
         * <p>
         * Retrieve it
         */
        private RowConsumer handlerPhaseConsumer(int phaseId) {
            RowConsumer consumer = handlerConsumersByPhaseId.get(phaseId);
            if (logger.isTraceEnabled()) {
                logger.trace("Using BatchConsumer {} for phase {}, this is a leaf/handlerPhase", consumer, phaseId);
            }
            assert consumer != null : "No rowReceiver for handlerPhase " + phaseId;
            return consumer;
        }

        void registerBatchConsumer(int phaseId, RowConsumer consumer) {
            consumersByPhaseInputId.put(toKey(phaseId, (byte) 0), consumer);
        }

        void registerRamAccountingContext(int phaseId, RamAccountingContext ramAccountingContext) {
            long key = toKey(phaseId, (byte) 0);
            ramAccountingContextByPhaseInputId.put(key, ramAccountingContext);
        }

        @Nullable
        RamAccountingContext getRamAccountingContext(UpstreamPhase phase) {
            NodeOperation nodeOperation = opCtx.nodeOperationByPhaseId.get(phase.phaseId());
            if (nodeOperation == null) {
                return null;
            }

            long phaseIdKey = toKey(nodeOperation.downstreamExecutionPhaseId(), nodeOperation.downstreamExecutionPhaseInputId());
            return ramAccountingContextByPhaseInputId.get(phaseIdKey);
        }

        void registerSubContext(ExecutionSubContext subContext) {
            contextBuilder.addSubContext(subContext);
        }

        void registerLeaf(ExecutionPhase phase, RowConsumer consumer) {
            handlerConsumersByPhaseId.put(phase.phaseId(), consumer);
            leafs.add(phase);
        }
    }

    private class InnerPreparer extends ExecutionPhaseVisitor<PreparerContext, Boolean> {

        @Override
        public Boolean visitCountPhase(final CountPhase phase, final PreparerContext context) {
            Map<String, Map<String, List<Integer>>> locations = phase.routing().locations();
            String localNodeId = clusterService.localNode().getId();
            final Map<String, List<Integer>> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countPhase doesn't contain the current nodeId");
            }

            RowConsumer consumer = context.getRowConsumer(phase, 0);
            context.registerSubContext(new CountContext(
                phase.phaseId(),
                countOperation,
                consumer,
                indexShardMap,
                phase.whereClause()
            ));
            return true;
        }

        @Override
        public Boolean visitPKLookup(PKLookupPhase pkLookupPhase, PreparerContext context) {
            RowConsumer rowConsumer = ProjectingRowConsumer.create(
                context.getRowConsumer(pkLookupPhase, 0),
                pkLookupPhase.projections(),
                pkLookupPhase.jobId(),
                RamAccountingContext.forExecutionPhase(circuitBreaker, pkLookupPhase),
                projectorFactory
            );
            context.registerSubContext(new PKLookupContext(
                pkLookupPhase.phaseId(),
                inputFactory,
                pkLookupOperation,
                pkLookupPhase.partitionedByColumns(),
                pkLookupPhase.toCollect(),
                pkLookupPhase.getIdsByShardId(clusterService.localNode().getId()),
                rowConsumer
            ));
            return true;
        }

        @Override
        public Boolean visitMergePhase(final MergePhase phase, final PreparerContext context) {

            boolean upstreamOnSameNode = context.opCtx.upstreamsAreOnSameNode(phase.phaseId());

            int pageSize = Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.nodeIds().size());
            RowConsumer consumer = context.getRowConsumer(phase, pageSize);
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            consumer = ProjectingRowConsumer.create(
                consumer,
                phase.projections(),
                phase.jobId(),
                ramAccountingContext,
                projectorFactory
            );

            if (upstreamOnSameNode && phase.numInputs() == 1) {
                context.registerBatchConsumer(phase.phaseId(), consumer);
                context.registerRamAccountingContext(phase.phaseId(), ramAccountingContext);
                return true;
            }

            context.registerSubContext(new PageDownstreamContext(
                pageDownstreamContextLogger,
                nodeName(),
                phase.phaseId(),
                phase.name(),
                consumer,
                PagingIterator.create(
                    phase.numUpstreams(),
                    false,
                    phase.orderByPositions(),
                    () -> new RowAccounting(
                        phase.inputTypes(),
                        RamAccountingContext.forExecutionPhase(circuitBreaker, phase))),
                DataTypes.getStreamers(phase.inputTypes()),
                ramAccountingContext,
                phase.numUpstreams()
            ));
            return true;
        }


        @Override
        public Boolean visitRoutedCollectPhase(final RoutedCollectPhase phase, final PreparerContext context) {
            RowConsumer consumer = context.getRowConsumer(phase,
                MoreObjects.firstNonNull(phase.nodePageSizeHint(), Paging.PAGE_SIZE));

            RamAccountingContext ramAccountingContext = context.getRamAccountingContext(phase);
            if (ramAccountingContext == null) {
                ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            }

            context.registerSubContext(new JobCollectContext(
                phase,
                collectOperation,
                ramAccountingContext,
                consumer,
                context.sharedShardContexts
            ));
            return true;
        }

        @Override
        public Boolean visitCollectPhase(CollectPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            RowConsumer consumer = context.getRowConsumer(phase, Paging.PAGE_SIZE);
            context.registerSubContext(new JobCollectContext(
                phase,
                collectOperation,
                ramAccountingContext,
                consumer,
                context.sharedShardContexts
            ));
            return true;
        }

        @Override
        public Boolean visitFetchPhase(final FetchPhase phase, final PreparerContext context) {
            List<Routing> routings = new ArrayList<>();
            context.opCtx.nodeOperationByPhaseId.values().forEach((ObjectProcedure<NodeOperation>) value -> {
                ExecutionPhase executionPhase = value.executionPhase();
                if (executionPhase instanceof RoutedCollectPhase) {
                    routings.add(((RoutedCollectPhase) executionPhase).routing());
                }
            });

            context.leafs.forEach(executionPhase -> {
                if (executionPhase instanceof RoutedCollectPhase) {
                    routings.add(((RoutedCollectPhase) executionPhase).routing());
                }
            });
            assert !routings.isEmpty()
                : "Routings must be present. " +
                  "It doesn't make sense to have a FetchPhase on a node without at least one CollectPhase on the same node";
            String localNodeId = clusterService.localNode().getId();
            context.registerSubContext(new FetchContext(
                phase,
                localNodeId,
                context.sharedShardContexts,
                clusterService.state().metaData(),
                routings));
            return true;
        }

        @Override
        public Boolean visitNestedLoopPhase(NestedLoopPhase phase, PreparerContext context) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(circuitBreaker, phase);
            RowConsumer lastConsumer = context.getRowConsumer(phase, Paging.PAGE_SIZE);

            RowConsumer firstConsumer = ProjectingRowConsumer.create(
                lastConsumer, phase.projections(), phase.jobId(), ramAccountingContext, projectorFactory);
            Predicate<Row> joinCondition = RowFilter.create(inputFactory, phase.joinCondition());

            NestedLoopOperation nestedLoopOperation = new NestedLoopOperation(
                phase.numLeftOutputs(),
                phase.numRightOutputs(),
                firstConsumer,
                joinCondition,
                phase.joinType()
            );
            PageDownstreamContext left = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 0,
                phase.leftMergePhase(),
                nestedLoopOperation.leftConsumer(),
                ramAccountingContext);
            if (left != null) {
                context.registerSubContext(left);
            }
            PageDownstreamContext right = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 1,
                phase.rightMergePhase(),
                nestedLoopOperation.rightConsumer(),
                ramAccountingContext
            );
            if (right != null) {
                context.registerSubContext(right);
            }
            context.registerSubContext(new NestedLoopContext(
                nlContextLogger,
                phase,
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
                                                                         RowConsumer rowConsumer,
                                                                         RamAccountingContext ramAccountingContext) {
            if (mergePhase == null) {
                ctx.consumersByPhaseInputId.put(toKey(nlPhaseId, inputId), rowConsumer);
                return null;
            }

            // In case of join on virtual table the left or right merge phase of the nl might have projections (TopN)
            if (mergePhase.hasProjections()) {
                rowConsumer = ProjectingRowConsumer.create(
                    rowConsumer,
                    mergePhase.projections(),
                    mergePhase.jobId(),
                    ramAccountingContext,
                    projectorFactory
                );
            }
            return new PageDownstreamContext(
                pageDownstreamContextLogger,
                nodeName(),
                mergePhase.phaseId(),
                mergePhase.name(),
                rowConsumer,
                PagingIterator.create(
                    mergePhase.numUpstreams(),
                    true,
                    mergePhase.orderByPositions(),
                    () -> new RowAccounting(
                        mergePhase.inputTypes(),
                        RamAccountingContext.forExecutionPhase(circuitBreaker, mergePhase))),
                StreamerVisitor.streamersFromOutputs(mergePhase),
                ramAccountingContext,
                mergePhase.numUpstreams()
            );
        }
    }


    private static long toKey(int phaseId, byte inputId) {
        long l = (long) phaseId;
        return (l << 32) | (inputId & 0xffffffffL);
    }
}
