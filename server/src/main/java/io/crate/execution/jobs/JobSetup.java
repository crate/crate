/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.jobs;

import static io.crate.execution.dsl.projection.Projections.nodeProjections;
import static io.crate.execution.dsl.projection.Projections.shardProjections;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntCollection;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.ObjectProcedure;

import io.crate.Streamer;
import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.breaker.RowCellsAccountingWithEstimators;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.IncrementalPageBucketReceiver;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.JobLauncher.HandlerPhase;
import io.crate.execution.engine.aggregation.AggregationPipe;
import io.crate.execution.engine.aggregation.GroupingProjector;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.MapSideDataCollectOperation;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.engine.collect.count.CountOperation;
import io.crate.execution.engine.collect.sources.ShardCollectSource;
import io.crate.execution.engine.collect.sources.SystemCollectSource;
import io.crate.execution.engine.distribution.DistributingConsumerFactory;
import io.crate.execution.engine.distribution.SingleBucketBuilder;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.execution.engine.fetch.FetchTask;
import io.crate.execution.engine.join.HashJoinOperation;
import io.crate.execution.engine.join.NestedLoopOperation;
import io.crate.execution.engine.pipeline.ProjectingRowConsumer;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.memory.MemoryManager;
import io.crate.memory.MemoryManagerFactory;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.operators.PKAndVersion;
import io.crate.types.DataTypes;

@Singleton
public class JobSetup {

    private static final Logger LOGGER = LogManager.getLogger(JobSetup.class);

    private final MapSideDataCollectOperation collectOperation;
    private final ClusterService clusterService;
    private final CircuitBreakerService circuitBreakerService;
    private final CountOperation countOperation;
    private final MemoryManagerFactory memoryManagerFactory;
    private final DistributingConsumerFactory distributingConsumerFactory;
    private final InnerPreparer innerPreparer;
    private final InputFactory inputFactory;
    private final ProjectorFactory projectorFactory;
    private final PKLookupOperation pkLookupOperation;
    private final Executor searchTp;
    private final String nodeName;
    private final Schemas schemas;

    @Inject
    public JobSetup(Settings settings,
                    Schemas schemas,
                    MapSideDataCollectOperation collectOperation,
                    ClusterService clusterService,
                    NodeLimits nodeJobsCounter,
                    CircuitBreakerService circuitBreakerService,
                    CountOperation countOperation,
                    ThreadPool threadPool,
                    DistributingConsumerFactory distributingConsumerFactory,
                    Node node,
                    IndicesService indicesService,
                    NodeContext nodeCtx,
                    SystemCollectSource systemCollectSource,
                    ShardCollectSource shardCollectSource,
                    MemoryManagerFactory memoryManagerFactory) {
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.schemas = schemas;
        this.collectOperation = collectOperation;
        this.clusterService = clusterService;
        this.circuitBreakerService = circuitBreakerService;
        this.countOperation = countOperation;
        this.memoryManagerFactory = memoryManagerFactory;
        this.pkLookupOperation = new PKLookupOperation(indicesService, shardCollectSource);
        this.distributingConsumerFactory = distributingConsumerFactory;
        innerPreparer = new InnerPreparer();
        inputFactory = new InputFactory(nodeCtx);
        searchTp = threadPool.executor(ThreadPool.Names.SEARCH);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
        this.projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            schemas,
            nodeJobsCounter,
            circuitBreakerService,
            nodeCtx,
            threadPool,
            settings,
            node.client(),
            inputFactory,
            normalizer,
            systemCollectSource::getRowUpdater,
            systemCollectSource::tableDefinition
        );
    }

    public List<CompletableFuture<StreamBucket>> prepareOnRemote(SessionSettings sessionInfo,
                                                                 Collection<? extends NodeOperation> nodeOperations,
                                                                 RootTask.Builder contextBuilder,
                                                                 SharedShardContexts sharedShardContexts) {
        Context context = new Context(
            clusterService.localNode().getId(),
            sessionInfo,
            contextBuilder,
            LOGGER,
            distributingConsumerFactory,
            nodeOperations,
            sharedShardContexts);
        registerContextPhases(nodeOperations, context);
        LOGGER.trace(
            "prepareOnRemote: job={} nodeOperations={} targetSourceMap={}",
            contextBuilder.jobId(),
            nodeOperations,
            context.opCtx.targetToSourceMap
        );

        for (IntCursor cursor : context.opCtx.findLeafs()) {
            prepareSourceOperations(cursor.value, context);
        }
        assert context.opCtx.allNodeOperationContextsBuilt() : "some nodeOperations haven't been processed";
        return context.directResponseFutures;
    }

    public List<CompletableFuture<StreamBucket>> prepareOnHandler(SessionSettings sessionInfo,
                                                                  Collection<? extends NodeOperation> nodeOperations,
                                                                  RootTask.Builder taskBuilder,
                                                                  List<HandlerPhase> handlerPhases,
                                                                  SharedShardContexts sharedShardContexts) {
        Context context = new Context(
            clusterService.localNode().getId(),
            sessionInfo,
            taskBuilder,
            LOGGER,
            distributingConsumerFactory,
            nodeOperations,
            sharedShardContexts);
        for (var handlerPhase : handlerPhases) {
            context.registerLeaf(handlerPhase.phase(), handlerPhase.consumer());
        }
        registerContextPhases(nodeOperations, context);
        LOGGER.trace(
            "prepareOnHandler: jobId={} nodeOperations={} handlerPhases={} targetSourceMap={}",
            taskBuilder.jobId(),
            nodeOperations,
            handlerPhases,
            context.opCtx.targetToSourceMap
        );

        IntHashSet leafs = new IntHashSet();
        for (var handlerPhase : handlerPhases) {
            ExecutionPhase phase = handlerPhase.phase();
            createContexts(phase, context);
            leafs.add(phase.phaseId());
        }
        leafs.addAll(context.opCtx.findLeafs());
        for (IntCursor cursor : leafs) {
            prepareSourceOperations(cursor.value, context);
        }
        assert context.opCtx.allNodeOperationContextsBuilt() : "some nodeOperations haven't been processed";
        return context.directResponseFutures;
    }

    private void createContexts(ExecutionPhase phase, Context context) {
        try {
            phase.accept(innerPreparer, context);
        } catch (Throwable t) {
            IllegalArgumentException e = new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Couldn't create executionContexts from%n" +
                "NodeOperations: %s%n" +
                "Leafs: %s%n" +
                "target-sources: %s%n" +
                "original-error: %s",
                context.opCtx.nodeOperationByPhaseId,
                context.leafs,
                context.opCtx.targetToSourceMap,
                t.getClass().getSimpleName() + ": " + t.getMessage()),
                t);
            e.setStackTrace(t.getStackTrace());
            throw e;
        }
    }

    private void registerContextPhases(Iterable<? extends NodeOperation> nodeOperations, Context context) {
        for (NodeOperation nodeOperation : nodeOperations) {
            // context for nodeOperations without dependencies can be built immediately (e.g. FetchPhase)
            if (nodeOperation.downstreamExecutionPhaseId() == NodeOperation.NO_DOWNSTREAM) {
                LOGGER.trace("Building context for nodeOp without downstream: {}", nodeOperation);
                createContexts(nodeOperation.executionPhase(), context);
                context.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().phaseId());
            }
            if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                var executionPhase = nodeOperation.executionPhase();
                CircuitBreaker breaker = breaker();
                int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
                var ramAccounting = new BlockBasedRamAccounting(
                    b -> breaker.addEstimateBytesAndMaybeBreak(b, executionPhase.label()),
                    ramAccountingBlockSizeInBytes);
                Streamer<?>[] streamers = StreamerVisitor.streamersFromOutputs(executionPhase);
                SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers, ramAccounting);
                context.directResponseFutures.add(bucketBuilder.completionFuture().whenComplete((res, err) -> ramAccounting.close()));
                context.registerBatchConsumer(nodeOperation.downstreamExecutionPhaseId(), bucketBuilder);
            }
        }
    }

    /**
     * recursively build all contexts that depend on startPhaseId (excl. startPhaseId)
     * <p>
     * {@link Context#opCtx#targetToSourceMap} will be used to traverse the nodeOperations
     */
    private void prepareSourceOperations(int startPhaseId, Context context) {
        IntContainer sourcePhaseIds = context.opCtx.targetToSourceMap.get(startPhaseId);
        if (sourcePhaseIds == null) {
            return;
        }
        for (IntCursor sourcePhaseId : sourcePhaseIds) {
            NodeOperation nodeOperation = context.opCtx.nodeOperationByPhaseId.get(sourcePhaseId.value);
            createContexts(nodeOperation.executionPhase(), context);
            context.opCtx.builtNodeOperations.set(nodeOperation.executionPhase().phaseId());
        }
        for (IntCursor sourcePhaseId : sourcePhaseIds) {
            prepareSourceOperations(sourcePhaseId.value, context);
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
         * This map is used in {@link #prepareSourceOperations(int, Context)} to process to NodeOperations in the
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
                    ((UpstreamPhase) executionPhase).distributionInfo().distributionType() ==
                    DistributionType.SAME_NODE) {
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

    private static class Context {

        private final DistributingConsumerFactory distributingConsumerFactory;

        /**
         * from toKey(phaseId, inputId) to BatchConsumer.
         */
        private final LongObjectMap<RowConsumer> consumersByPhaseInputId = new LongObjectHashMap<>();
        private final IntObjectMap<RowConsumer> handlerConsumersByPhaseId = new IntObjectHashMap<>();

        private final SharedShardContexts sharedShardContexts;

        private final List<CompletableFuture<StreamBucket>> directResponseFutures = new ArrayList<>();
        private final NodeOperationCtx opCtx;
        private final RootTask.Builder taskBuilder;
        private final Logger logger;
        private final List<ExecutionPhase> leafs = new ArrayList<>();
        private final TransactionContext transactionContext;

        Context(String localNodeId,
                SessionSettings sessionInfo,
                RootTask.Builder taskBuilder,
                Logger logger,
                DistributingConsumerFactory distributingConsumerFactory,
                Collection<? extends NodeOperation> nodeOperations,
                SharedShardContexts sharedShardContexts) {
            this.taskBuilder = taskBuilder;
            this.logger = logger;
            this.opCtx = new NodeOperationCtx(localNodeId, nodeOperations);
            this.distributingConsumerFactory = distributingConsumerFactory;
            this.sharedShardContexts = sharedShardContexts;
            this.transactionContext = TransactionContext.of(sessionInfo);
        }

        public UUID jobId() {
            return taskBuilder.jobId();
        }

        public int operationMemoryLimitInBytes() {
            return transactionContext.sessionSettings().memoryLimitInBytes();
        }

        /**
         * Retrieve the rowReceiver of the downstream of phase
         */
        RowConsumer getRowConsumer(UpstreamPhase phase, int pageSize, RamAccounting ramAccounting) {
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
                        nodeOperation, ramAccounting, phase.distributionInfo(), jobId(), pageSize);
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "action=getRowReceiver, distributionType={}, phase={}, targetConsumer={}, target={}/{},",
                            distributionType.toString(),
                            phase.phaseId(),
                            consumer,
                            nodeOperation.downstreamExecutionPhaseId(),
                            nodeOperation.downstreamExecutionPhaseInputId()
                        );
                    }
                    return consumer;

                default:
                    throw new AssertionError(
                        "unhandled distributionType: " + distributionType);
            }
        }

        /**
         * The rowReceiver for handlerPhases got passed into
         * {@link #prepareOnHandler(SessionSettings, Collection, RootTask.Builder, List, SharedShardContexts)}
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

        void registerSubContext(Task subContext) {
            taskBuilder.addTask(subContext);
        }

        void registerLeaf(ExecutionPhase phase, RowConsumer consumer) {
            handlerConsumersByPhaseId.put(phase.phaseId(), consumer);
            leafs.add(phase);
        }

        public TransactionContext txnCtx() {
            return transactionContext;
        }
    }

    private class InnerPreparer extends ExecutionPhaseVisitor<Context, Void> {

        @Override
        public Void visitCountPhase(final CountPhase phase, final Context context) {
            Map<String, Map<String, IntIndexedContainer>> locations = phase.routing().locations();
            String localNodeId = clusterService.localNode().getId();
            final Map<String, IntIndexedContainer> indexShardMap = locations.get(localNodeId);
            if (indexShardMap == null) {
                throw new IllegalArgumentException("The routing of the countPhase doesn't contain the current nodeId");
            }
            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
            var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            RowConsumer consumer = context.getRowConsumer(
                phase,
                Paging.NO_PAGING,
                new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes));
            consumer.completionFuture().whenComplete((result, error) -> ramAccounting.close());
            context.registerSubContext(new CountTask(
                phase,
                context.transactionContext,
                countOperation,
                consumer,
                indexShardMap
            ));
            return null;
        }

        @Override
        public Void visitPKLookup(PKLookupPhase pkLookupPhase, Context context) {
            Collection<? extends Projection> shardProjections = shardProjections(pkLookupPhase.projections());
            Collection<? extends Projection> nodeProjections = nodeProjections(pkLookupPhase.projections());

            Map<ShardId, List<PKAndVersion>> idsByShardId =
                pkLookupPhase.getIdsByShardId(clusterService.localNode().getId());

            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytesPerShard(
                breaker.getLimit(),
                idsByShardId.size()
            );
            var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(pkLookupPhase.label(), breaker, context.operationMemoryLimitInBytes());
            var consumerRamAccounting = new BlockBasedRamAccounting(
                ramAccounting::addBytes,
                ramAccountingBlockSizeInBytes);
            var consumerMemoryManager = memoryManagerFactory.getMemoryManager(ramAccounting);

            RowConsumer lastConsumer = context.getRowConsumer(
                pkLookupPhase,
                Paging.NO_PAGING,
                consumerRamAccounting
            );
            lastConsumer.completionFuture().whenComplete((result, error) -> {
                consumerMemoryManager.close();
                ramAccounting.close();
            });
            RowConsumer nodeRowConsumer = ProjectingRowConsumer.create(
                lastConsumer,
                nodeProjections,
                pkLookupPhase.jobId(),
                context.txnCtx(),
                consumerRamAccounting,
                consumerMemoryManager,
                projectorFactory
            );
            context.registerSubContext(new PKLookupTask(
                pkLookupPhase.jobId(),
                pkLookupPhase.phaseId(),
                pkLookupPhase.name(),
                ramAccounting,
                memoryManagerFactory,
                ramAccountingBlockSizeInBytes,
                context.transactionContext,
                schemas,
                inputFactory,
                pkLookupOperation,
                pkLookupPhase.partitionedByColumns(),
                pkLookupPhase.toCollect(),
                idsByShardId,
                shardProjections,
                nodeRowConsumer
            ));
            return null;
        }

        @Override
        public Void visitMergePhase(final MergePhase phase, final Context context) {
            boolean upstreamOnSameNode = context.opCtx.upstreamsAreOnSameNode(phase.phaseId());
            int pageSize = Paging.getWeightedPageSize(Paging.PAGE_SIZE, 1.0d / phase.nodeIds().size());

            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
            var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            var ramAccountingForMerge = new BlockBasedRamAccounting(
                ramAccounting::addBytes,
                ramAccountingBlockSizeInBytes);

            RowConsumer finalRowConsumer = context.getRowConsumer(phase, pageSize, ramAccountingForMerge);
            MemoryManager memoryManager = memoryManagerFactory.getMemoryManager(ramAccounting);
            finalRowConsumer.completionFuture().whenComplete((result, error) -> {
                memoryManager.close();
                ramAccounting.close();
            });

            if (upstreamOnSameNode && phase.numInputs() == 1) {
                RowConsumer projectingRowConsumer = ProjectingRowConsumer.create(
                    finalRowConsumer,
                    phase.projections(),
                    phase.jobId(),
                    context.txnCtx(),
                    ramAccountingForMerge,
                    memoryManager,
                    projectorFactory
                );
                context.registerBatchConsumer(phase.phaseId(), projectingRowConsumer);
                return null;
            }

            Collector<Row, ?, Iterable<Row>> collector = null;
            List<Projection> projections = phase.projections();
            if (projections.size() > 0) {
                Projection firstProjection = projections.get(0);
                if (firstProjection instanceof GroupProjection) {
                    GroupProjection groupProjection = (GroupProjection) firstProjection;
                    GroupingProjector groupingProjector = (GroupingProjector) projectorFactory.create(
                        groupProjection,
                        context.txnCtx(),
                        ramAccountingForMerge,
                        memoryManager,
                        phase.jobId()
                    );
                    collector = groupingProjector.getCollector();
                    projections = projections.subList(1, projections.size());
                } else if (firstProjection instanceof AggregationProjection) {
                    AggregationProjection aggregationProjection = (AggregationProjection) firstProjection;
                    AggregationPipe aggregationPipe = (AggregationPipe) projectorFactory.create(
                        aggregationProjection,
                        context.txnCtx(),
                        ramAccountingForMerge,
                        memoryManager,
                        phase.jobId()
                    );
                    collector = aggregationPipe.getCollector();
                    projections = projections.subList(1, projections.size());
                }
            }

            RowConsumer projectingRowConsumer = ProjectingRowConsumer.create(
                finalRowConsumer,
                projections,
                phase.jobId(),
                context.txnCtx(),
                ramAccountingForMerge,
                memoryManager,
                projectorFactory
            );
            PageBucketReceiver pageBucketReceiver;
            if (collector == null) {
                pageBucketReceiver = new CumulativePageBucketReceiver(
                    nodeName,
                    phase.phaseId(),
                    searchTp,
                    DataTypes.getStreamers(phase.inputTypes()),
                    projectingRowConsumer,
                    PagingIterator.create(
                        phase.numUpstreams(),
                        phase.inputTypes(),
                        projectingRowConsumer.requiresScroll(),
                        phase.orderByPositions(),
                        () -> new RowAccountingWithEstimators(
                            phase.inputTypes(),
                            new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes))),
                    phase.numUpstreams());
            } else {
                pageBucketReceiver = new IncrementalPageBucketReceiver<>(
                    collector,
                    projectingRowConsumer,
                    searchTp,
                    DataTypes.getStreamers(phase.inputTypes()),
                    phase.numUpstreams());
            }
            context.registerSubContext(new DistResultRXTask(
                phase.phaseId(),
                phase.name(),
                pageBucketReceiver,
                ramAccounting,
                phase.numUpstreams()
            ));
            return null;
        }

        @Override
        public Void visitRoutedCollectPhase(final RoutedCollectPhase phase, final Context context) {
            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytesPerShard(
                breaker.getLimit(),
                phase.routing().numShards(clusterService.localNode().getId())
            );
            var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            RowConsumer consumer = context.getRowConsumer(
                phase,
                Objects.requireNonNullElse(phase.nodePageSizeHint(), Paging.PAGE_SIZE),
                new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes)
            );
            consumer.completionFuture().whenComplete((result, error) -> ramAccounting.close());
            context.registerSubContext(new CollectTask(
                phase,
                context.txnCtx(),
                collectOperation,
                ramAccounting,
                memoryManagerFactory,
                consumer,
                context.sharedShardContexts,
                clusterService.state().nodes().getMinNodeVersion(),
                ramAccountingBlockSizeInBytes
            ));
            return null;
        }

        @Override
        public Void visitCollectPhase(CollectPhase phase, Context context) {
            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
            RamAccounting ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            RowConsumer consumer = context.getRowConsumer(
                phase,
                Paging.PAGE_SIZE,
                new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes)
            );
            consumer.completionFuture().whenComplete((result, error) -> ramAccounting.close());
            context.registerSubContext(new CollectTask(
                phase,
                context.txnCtx(),
                collectOperation,
                ramAccounting,
                memoryManagerFactory,
                consumer,
                context.sharedShardContexts,
                clusterService.state().nodes().getMinNodeVersion(),
                ramAccountingBlockSizeInBytes
            ));
            return null;
        }

        @Override
        public Void visitFetchPhase(final FetchPhase phase, final Context context) {
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
            context.registerSubContext(new FetchTask(
                context.jobId(),
                phase,
                context.operationMemoryLimitInBytes(),
                localNodeId,
                context.sharedShardContexts,
                clusterService.state().metadata(),
                relationName -> schemas.getTableInfo(relationName, Operation.READ),
                routings));
            return null;
        }

        @Override
        public Void visitNestedLoopPhase(NestedLoopPhase phase, Context context) {
            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
            var concurrentRamAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            var ramAccountingOfOperation = new BlockBasedRamAccounting(
                concurrentRamAccounting::addBytes,
                ramAccountingBlockSizeInBytes);
            RowConsumer lastConsumer = context.getRowConsumer(phase, Paging.PAGE_SIZE, ramAccountingOfOperation);
            var memoryManager = memoryManagerFactory.getMemoryManager(concurrentRamAccounting);
            lastConsumer.completionFuture().whenComplete((result, error) -> {
                memoryManager.close();
                concurrentRamAccounting.close();
            });

            RowConsumer firstConsumer = ProjectingRowConsumer.create(
                lastConsumer,
                phase.projections(),
                phase.jobId(),
                context.txnCtx(),
                ramAccountingOfOperation,
                memoryManager,
                projectorFactory
            );
            Predicate<Row> joinCondition = RowFilter.create(context.transactionContext, inputFactory, phase.joinCondition());

            NestedLoopOperation joinOperation = new NestedLoopOperation(
                phase.numLeftOutputs(),
                phase.numRightOutputs(),
                firstConsumer,
                joinCondition,
                phase.joinType(),
                breaker(),
                ramAccountingOfOperation,
                phase.leftSideColumnTypes,
                phase.estimatedRowsSizeLeft,
                phase.estimatedNumberOfRowsLeft,
                phase.blockNestedLoop);

            DistResultRXTask left = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 0,
                phase.leftMergePhase(),
                joinOperation.leftConsumer(),
                new BlockBasedRamAccounting(concurrentRamAccounting::addBytes, ramAccountingBlockSizeInBytes),
                memoryManager
            );

            if (left != null) {
                context.registerSubContext(left);
            }

            DistResultRXTask right = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 1,
                phase.rightMergePhase(),
                joinOperation.rightConsumer(),
                new BlockBasedRamAccounting(concurrentRamAccounting::addBytes, ramAccountingBlockSizeInBytes),
                memoryManager
            );
            if (right != null) {
                context.registerSubContext(right);
            }
            context.registerSubContext(new JoinTask(
                phase,
                joinOperation,
                left != null ? left.getBucketReceiver((byte) 0) : null,
                right != null ? right.getBucketReceiver((byte) 0) : null
            ));
            return null;
        }

        @Override
        public Void visitHashJoinPhase(HashJoinPhase phase, Context context) {
            CircuitBreaker breaker = breaker();
            int ramAccountingBlockSizeInBytes = BlockBasedRamAccounting.blockSizeInBytes(breaker.getLimit());
            var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(phase.label(), breaker, context.operationMemoryLimitInBytes());
            var ramAccountingOfOperation = new BlockBasedRamAccounting(
                ramAccounting::addBytes,
                ramAccountingBlockSizeInBytes);
            RowConsumer lastConsumer = context.getRowConsumer(phase, Paging.PAGE_SIZE, ramAccountingOfOperation);
            var memoryManager = memoryManagerFactory.getMemoryManager(ramAccounting);
            lastConsumer.completionFuture().whenComplete((result, error) -> {
                memoryManager.close();
                ramAccounting.close();
            });

            RowConsumer firstConsumer = ProjectingRowConsumer.create(
                lastConsumer,
                phase.projections(),
                phase.jobId(),
                context.txnCtx(),
                ramAccountingOfOperation,
                memoryManager,
                projectorFactory
            );
            Predicate<Row> joinCondition = RowFilter.create(context.transactionContext, inputFactory, phase.joinCondition());

            HashJoinOperation joinOperation = new HashJoinOperation(
                phase.numLeftOutputs(),
                phase.numRightOutputs(),
                firstConsumer,
                joinCondition,
                phase.leftJoinConditionInputs(),
                phase.rightJoinConditionInputs(),
                // 110 extra bytes per row =
                //    96 bytes for each ArrayList +
                //    7 bytes per key for the IntHashObjectHashMap  (should be 4 but the map pre-allocates more)
                //    7 bytes perv value (pointer from the map to the list) (should be 4 but the map pre-allocates more)
                new RowCellsAccountingWithEstimators(phase.leftOutputTypes(), ramAccountingOfOperation, 110),
                context.transactionContext,
                inputFactory,
                breaker(),
                phase.estimatedRowSizeForLeft()
            );
            DistResultRXTask left = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 0,
                phase.leftMergePhase(),
                joinOperation.leftConsumer(),
                new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes),
                memoryManager
            );
            if (left != null) {
                context.registerSubContext(left);
            }
            DistResultRXTask right = pageDownstreamContextForNestedLoop(
                phase.phaseId(),
                context,
                (byte) 1,
                phase.rightMergePhase(),
                joinOperation.rightConsumer(),
                new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes),
                memoryManager
            );
            if (right != null) {
                context.registerSubContext(right);
            }
            context.registerSubContext(new JoinTask(
                phase,
                joinOperation,
                left != null ? left.getBucketReceiver((byte) 0) : null,
                right != null ? right.getBucketReceiver((byte) 0) : null
            ));
            return null;
        }

        @Nullable
        private DistResultRXTask pageDownstreamContextForNestedLoop(int nlPhaseId,
                                                                    Context ctx,
                                                                    byte inputId,
                                                                    @Nullable MergePhase mergePhase,
                                                                    RowConsumer rowConsumer,
                                                                    RamAccounting ramAccounting,
                                                                    MemoryManager memoryManager) {
            if (mergePhase == null) {
                ctx.consumersByPhaseInputId.put(toKey(nlPhaseId, inputId), rowConsumer);
                return null;
            }

            // In case of join on virtual table the left or right merge phase
            // of the nl might have projections (LimitAndOffset)
            if (mergePhase.hasProjections()) {
                rowConsumer = ProjectingRowConsumer.create(
                    rowConsumer,
                    mergePhase.projections(),
                    mergePhase.jobId(),
                    ctx.txnCtx(),
                    ramAccounting,
                    memoryManager,
                    projectorFactory
                );
            }

            PageBucketReceiver pageBucketReceiver = new CumulativePageBucketReceiver(
                nodeName,
                mergePhase.phaseId(),
                searchTp,
                StreamerVisitor.streamersFromOutputs(mergePhase),
                rowConsumer,
                PagingIterator.create(
                    mergePhase.numUpstreams(),
                    mergePhase.inputTypes(),
                    rowConsumer.requiresScroll(),
                    mergePhase.orderByPositions(),
                    () -> new RowAccountingWithEstimators(
                        mergePhase.inputTypes(),
                        ramAccounting)),
                mergePhase.numUpstreams());

            return new DistResultRXTask(
                mergePhase.phaseId(),
                mergePhase.name(),
                pageBucketReceiver,
                ramAccounting,
                mergePhase.numUpstreams()
            );
        }
    }

    private CircuitBreaker breaker() {
        return circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
    }

    private static long toKey(int phaseId, byte inputId) {
        return ((long) phaseId << 32) | (inputId & 0xffffffffL);
    }
}
