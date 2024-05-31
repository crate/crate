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

package io.crate.execution.engine.pipeline;

import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_OFFSET;
import static io.crate.planner.operators.InsertFromValues.checkConstraints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.SymbolEvaluator;
import io.crate.breaker.TypedCellsAccounting;
import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.SysUpdateProjector;
import io.crate.execution.dml.SysUpdateResultSetProjector;
import io.crate.execution.dml.delete.ShardDeleteAction;
import io.crate.execution.dml.delete.ShardDeleteRequest;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.CorrelatedJoinProjection;
import io.crate.execution.dsl.projection.DeleteProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.LimitDistinctProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.OrderedLimitAndOffsetProjection;
import io.crate.execution.dsl.projection.ProjectSetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.ProjectionVisitor;
import io.crate.execution.dsl.projection.SourceIndexWriterProjection;
import io.crate.execution.dsl.projection.SourceIndexWriterReturnSummaryProjection;
import io.crate.execution.dsl.projection.SysUpdateProjection;
import io.crate.execution.dsl.projection.UpdateProjection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.CorrelatedJoinProjector;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.AggregationPipe;
import io.crate.execution.engine.aggregation.GroupingProjector;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.execution.engine.export.FileWriterProjector;
import io.crate.execution.engine.fetch.FetchNodeAction;
import io.crate.execution.engine.fetch.FetchProjector;
import io.crate.execution.engine.fetch.TransportFetchOperation;
import io.crate.execution.engine.indexing.ColumnIndexWriterProjector;
import io.crate.execution.engine.indexing.DMLProjector;
import io.crate.execution.engine.indexing.IndexNameResolver;
import io.crate.execution.engine.indexing.IndexWriterProjector;
import io.crate.execution.engine.indexing.ShardDMLExecutor;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.execution.engine.indexing.UpsertResultContext;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.execution.engine.sort.SortingLimitAndOffsetProjector;
import io.crate.execution.engine.sort.SortingProjector;
import io.crate.execution.engine.sort.UnboundedSortingLimitAndOffsetCollector;
import io.crate.execution.engine.window.WindowProjector;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.support.ThreadPools;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.SysRowUpdater;
import io.crate.expression.reference.sys.check.node.SysNodeCheck;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.memory.MemoryManager;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.sys.SysNodeChecksTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

public class ProjectionToProjectorVisitor
    extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> implements ProjectorFactory {

    /**
     * Represents the minimum number of rows that have to be processed in a sorted LimitAndOffset projector context
     * after which we'll use an unbounded collector ie.
     * {@link UnboundedSortingLimitAndOffsetCollector}. If less rows are needed
     * (i.e. limit + offset < threshold) a bounded collector will be used (a collector that pre-allocates
     * the underlying structure in order to execute the sort and limit operation).
     * <p>
     * We've chosen 10_000 because the usual user required limits are lower and our default limit applied over HTTP is
     * 10_000. These most common cases will be accommodated by a bounded collector which is slightly faster than the
     * unbounded one (however benchmarks have shown that using a collector that grows the underlying structure adds
     * an overhead of around 1-5% so performance will not be grossly affected even for larger limits).
     */
    private static final int UNBOUNDED_COLLECTOR_THRESHOLD = 10_000;

    private final ClusterService clusterService;
    private final NodeLimits nodeJobsCounter;
    private final NodeContext nodeCtx;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final ElasticsearchClient elasticsearchClient;
    private final InputFactory inputFactory;
    private final EvaluatingNormalizer normalizer;
    private final Function<RelationName, SysRowUpdater<?>> sysUpdaterGetter;
    private final Function<RelationName, StaticTableDefinition<?>> staticTableDefinitionGetter;
    private final CircuitBreakerService circuitBreakerService;
    private final Version indexVersionCreated;
    @Nullable
    private final ShardId shardId;
    private final int numProcessors;
    private final Map<String, FileOutputFactory> fileOutputFactoryMap;
    private final Schemas schemas;


    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Schemas schemas,
                                        NodeLimits nodeJobsCounter,
                                        CircuitBreakerService circuitBreakerService,
                                        NodeContext nodeCtx,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        ElasticsearchClient elasticsearchClient,
                                        InputFactory inputFactory,
                                        EvaluatingNormalizer normalizer,
                                        Function<RelationName, SysRowUpdater<?>> sysUpdaterGetter,
                                        Function<RelationName, StaticTableDefinition<?>> staticTableDefinitionGetter,
                                        Version indexVersionCreated,
                                        @Nullable ShardId shardId,
                                        Map<String, FileOutputFactory> fileOutputFactoryMap) {
        this.clusterService = clusterService;
        this.schemas = schemas;
        this.nodeJobsCounter = nodeJobsCounter;
        this.circuitBreakerService = circuitBreakerService;
        this.nodeCtx = nodeCtx;
        this.threadPool = threadPool;
        this.settings = settings;
        this.elasticsearchClient = elasticsearchClient;
        this.inputFactory = inputFactory;
        this.normalizer = normalizer;
        this.sysUpdaterGetter = sysUpdaterGetter;
        this.staticTableDefinitionGetter = staticTableDefinitionGetter;
        this.indexVersionCreated = indexVersionCreated;
        this.shardId = shardId;
        this.numProcessors = EsExecutors.numberOfProcessors(settings);
        this.fileOutputFactoryMap = fileOutputFactoryMap;
    }

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Schemas schemas,
                                        NodeLimits nodeJobsCounter,
                                        CircuitBreakerService circuitBreakerService,
                                        NodeContext nodeCtx,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        ElasticsearchClient elasticsearchClient,
                                        InputFactory inputFactory,
                                        EvaluatingNormalizer normalizer,
                                        Function<RelationName, SysRowUpdater<?>> sysUpdaterGetter,
                                        Function<RelationName, StaticTableDefinition<?>> staticTableDefinitionGetter) {
        this(clusterService,
            schemas,
            nodeJobsCounter,
            circuitBreakerService,
            nodeCtx,
            threadPool,
            settings,
            elasticsearchClient,
            inputFactory,
            normalizer,
            sysUpdaterGetter,
            staticTableDefinitionGetter,
            Version.CURRENT,
            null,
            null
        );
    }

    @Override
    public RowGranularity supportedGranularity() {
        if (this.shardId == null) {
            return RowGranularity.NODE;
        } else {
            return RowGranularity.SHARD;
        }
    }

    @Override
    public Projector visitOrderedLimitAndOffset(OrderedLimitAndOffsetProjection projection, Context context) {
        /* OrderBy symbols are added to the rows to enable sorting on them post-collect. E.g.:
         *
         * outputs: [x]
         * orderBy: [y]
         *
         * topLevelInputs: [x, y]
         *                    /
         * orderByIndices: [1]
         */
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx);
        ctx.add(projection.outputs());
        ctx.add(projection.orderBy());

        int numOutputs = projection.outputs().size();
        List<DataType<?>> rowTypes = Symbols.typeView(Lists.concat(projection.outputs(), projection.orderBy()));
        List<Input<?>> inputs = ctx.topLevelInputs();
        int[] orderByIndices = new int[inputs.size() - numOutputs];
        int idx = 0;
        for (int i = numOutputs; i < inputs.size(); i++) {
            orderByIndices[idx++] = i;
        }

        int rowMemoryOverhead = 32; // priority queues implementation are backed by an arrayList
        TypedCellsAccounting rowAccounting = new TypedCellsAccounting(
            rowTypes,
            context.ramAccounting,
            rowMemoryOverhead
        );
        if (projection.limit() > NO_LIMIT) {
            return new SortingLimitAndOffsetProjector(
                rowAccounting,
                inputs,
                ctx.expressions(),
                numOutputs,
                OrderingByPosition.arrayOrdering(rowTypes, orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
                projection.limit(),
                projection.offset(),
                UNBOUNDED_COLLECTOR_THRESHOLD
            );
        }
        return new SortingProjector(
            rowAccounting,
            inputs,
            ctx.expressions(),
            numOutputs,
            OrderingByPosition.arrayOrdering(rowTypes, orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
            projection.offset()
        );
    }

    @Override
    public Projector visitLimitDistinct(LimitDistinctProjection limitDistinct, Context context) {
        var rowAccounting = new TypedCellsAccounting(
            Symbols.typeView(limitDistinct.outputs()),
            context.ramAccounting,
            0
        );
        return new TopNDistinctProjector(limitDistinct.limit(), rowAccounting);
    }

    @Override
    public Projector visitLimitAndOffsetProjection(LimitAndOffsetProjection projection, Context context) {
        if (projection.limit() != NO_LIMIT) {
            return new LimitAndOffsetProjector(projection.limit(), projection.offset());
        } else if (projection.offset() != NO_OFFSET) {
            return new OffsetProjector(projection.offset());
        } else {
            throw new IllegalArgumentException("LimitAndOffsetProjection should have at least one " +
                                               "of the limit and offset set");
        }
    }

    @Override
    public Projector visitEvalProjection(EvalProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx, projection.outputs());
        return new InputRowProjector(ctx.topLevelInputs(), ctx.expressions());
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForAggregations(context.txnCtx);

        ctx.add(projection.keys());
        ctx.add(projection.values());

        List<Input<?>> keyInputs = ctx.topLevelInputs();
        return new GroupingProjector(
            projection.keys(),
            keyInputs,
            Iterables.toArray(ctx.expressions(), CollectExpression.class),
            projection.mode(),
            ctx.aggregations().toArray(new AggregationContext[0]),
            context.ramAccounting,
            context.memoryManager,
            clusterService.state().nodes().getMinNodeVersion(),
            indexVersionCreated
        );
    }

    @Override
    public Projector visitMergeCountProjection(MergeCountProjection projection, Context context) {
        return new MergeCountProjector();
    }

    @Override
    public Projector visitAggregationProjection(AggregationProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForAggregations(context.txnCtx);
        ctx.add(projection.aggregations());
        return new AggregationPipe(
            ctx.expressions(),
            projection.mode(),
            ctx.aggregations().toArray(new AggregationContext[0]),
            context.ramAccounting,
            context.memoryManager,
            clusterService.state().nodes().getMinNodeVersion(),
            indexVersionCreated
        );
    }

    @Override
    public Projector visitWriterProjection(WriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx);

        List<Input<?>> inputs = null;
        if (!projection.inputs().isEmpty()) {
            ctx.add(projection.inputs());
            inputs = ctx.topLevelInputs();
        }

        projection = projection.normalize(normalizer, context.txnCtx);
        String uri = DataTypes.STRING.sanitizeValue(
            SymbolEvaluator.evaluate(context.txnCtx, nodeCtx, projection.uri(), Row.EMPTY, SubQueryResults.EMPTY));
        assert uri != null : "URI must not be null";

        StringBuilder sb = new StringBuilder(uri);
        Symbol resolvedFileName = normalizer.normalize(WriterProjection.DIRECTORY_TO_FILENAME, context.txnCtx);
        assert resolvedFileName instanceof Literal : "resolvedFileName must be a Literal, but is: " + resolvedFileName;
        assert resolvedFileName.valueType().id() == StringType.ID :
            "resolvedFileName.valueType() must be " + StringType.INSTANCE;

        String fileName = (String) ((Literal<?>) resolvedFileName).value();
        if (!uri.endsWith("/")) {
            sb.append("/");
        }
        sb.append(fileName);
        if (projection.compressionType() == WriterProjection.CompressionType.GZIP) {
            sb.append(".gz");
        }
        uri = sb.toString();

        Map<ColumnIdent, Object> overwrites =
            symbolMapToObject(projection.overwrites(), ctx, context.txnCtx);

        return new FileWriterProjector(
            threadPool.generic(),
            uri,
            projection.compressionType(),
            inputs,
            ctx.expressions(),
            overwrites,
            projection.outputNames(),
            projection.outputFormat(),
            fileOutputFactoryMap,
            projection.withClauseOptions()
        );
    }

    private Map<ColumnIdent, Object> symbolMapToObject(
            Map<ColumnIdent, Symbol> symbolMap,
            InputFactory.Context<CollectExpression<Row, ?>> symbolContext,
            TransactionContext txnCtx) {
        Map<ColumnIdent, Object> objectMap = new HashMap<>(symbolMap.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : symbolMap.entrySet()) {
            Symbol symbol = entry.getValue();
            assert symbol != null : "symbol must not be null";
            objectMap.put(
                entry.getKey(),
                symbolContext.add(normalizer.normalize(symbol, txnCtx)).value()
            );
        }
        return objectMap;
    }

    @Override
    public Projector visitSourceIndexWriterProjection(SourceIndexWriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx);
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(ctx.add(partitionedBySymbol));
        }
        Input<?> sourceInput = ctx.add(projection.rawSource());
        Supplier<String> indexNameResolver =
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs);
        ClusterState state = clusterService.state();
        Settings tableSettings = TableSettingsResolver.get(state.metadata(),
            projection.tableIdent(), !projection.partitionedBySymbols().isEmpty());

        int targetTableNumShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(tableSettings);
        int targetTableNumReplicas = NumberOfReplicas.fromSettings(tableSettings, state.nodes().getSize());

        UpsertResultContext upsertResultContext;
        if (projection instanceof SourceIndexWriterReturnSummaryProjection) {
            upsertResultContext = UpsertResultContext.forReturnSummary(
                context.txnCtx,
                (SourceIndexWriterReturnSummaryProjection) projection,
                clusterService.localNode(),
                inputFactory);
        } else {
            upsertResultContext = UpsertResultContext.forRowCount();
        }
        return new IndexWriterProjector(
            clusterService,
            nodeJobsCounter,
            circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY),
            context.ramAccounting,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            context.txnCtx,
            nodeCtx,
            state.metadata().settings(),
            targetTableNumShards,
            targetTableNumReplicas,
            elasticsearchClient,
            indexNameResolver,
            projection.rawSourceReference(),
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            sourceInput,
            ctx.expressions(),
            projection.bulkActions(),
            projection.excludes(),
            projection.autoCreateIndices(),
            projection.overwriteDuplicates(),
            context.jobId,
            upsertResultContext,
            projection.failFast()
        );
    }

    @Override
    public Projector visitColumnIndexWriterProjection(ColumnIndexWriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx);
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(ctx.add(partitionedBySymbol));
        }
        List<Input<?>> insertInputs = new ArrayList<>(projection.allTargetColumns().size());

        List<Symbol> columnSymbols = InputColumns.create(
            projection.allTargetColumns(),
            new InputColumns.SourceSymbols(projection.allTargetColumns()));

        for (Symbol symbol : columnSymbols) {
            insertInputs.add(ctx.add(symbol));
        }
        ClusterState state = clusterService.state();
        Settings tableSettings = TableSettingsResolver.get(state.metadata(),
            projection.tableIdent(), !projection.partitionedBySymbols().isEmpty());

        int targetTableNumShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(tableSettings);
        int targetTableNumReplicas = NumberOfReplicas.fromSettings(tableSettings, state.nodes().getSize());

        final Map<String, Consumer<IndexItem>> validatorsCache = new HashMap<>();
        BiConsumer<String, IndexItem> constraintsChecker = (indexName, indexItem) -> checkConstraints(
            indexItem,
            indexName,
            schemas.getTableInfo(projection.tableIdent()),
            context.txnCtx,
            nodeCtx,
            validatorsCache,
            projection.allTargetColumns()
        );
        return new ColumnIndexWriterProjector(
            clusterService,
            constraintsChecker,
            nodeJobsCounter,
            circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY),
            context.ramAccounting,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            context.txnCtx,
            nodeCtx,
            state.metadata().settings(),
            targetTableNumShards,
            targetTableNumReplicas,
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs),
            elasticsearchClient,
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            projection.allTargetColumns(),
            insertInputs,
            ctx.expressions(),
            projection.isIgnoreDuplicateKeys(),
            projection.onDuplicateKeyAssignments(),
            projection.bulkActions(),
            projection.autoCreateIndices(),
            projection.returnValues(),
            context.jobId
        );
    }

    @Override
    public Projector visitFilterProjection(FilterProjection projection, Context context) {
        Predicate<Row> rowFilter = RowFilter.create(context.txnCtx, inputFactory, projection.query());
        return new FilterProjector(rowFilter);
    }

    @Override
    public Projector visitUpdateProjection(final UpdateProjection projection, Context context) {
        checkShardLevel("Update projection can only be executed on a shard");
        ShardDMLExecutor<?, ?, ?, ?> shardDMLExecutor;
        if (projection.returnValues() == null) {
            shardDMLExecutor = buildUpdateShardDMLExecutor(context, projection, ShardDMLExecutor.ROW_COUNT_COLLECTOR);
        } else {
            shardDMLExecutor = buildUpdateShardDMLExecutor(context, projection, ShardDMLExecutor.RESULT_ROW_COLLECTOR);
        }
        return new DMLProjector(shardDMLExecutor);
    }

    private <A> ShardDMLExecutor<?, ?, A, ?> buildUpdateShardDMLExecutor(
        Context context, UpdateProjection projection,
        Collector<ShardResponse, A, Iterable<Row>> collector) {

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            context.txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(settings),
            ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            projection.assignmentsColumns(),
            null,
            projection.returnValues(),
            context.jobId
        );

        return new ShardDMLExecutor<>(
            context.jobId,
            ShardDMLExecutor.DEFAULT_BULK_SIZE,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            resolveUidCollectExpression(context.txnCtx, projection.uidSymbol()),
            clusterService,
            context.ramAccounting,
            circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY),
            nodeJobsCounter,
            () -> builder.newRequest(shardId),
            id -> {
                Long requiredVersion = projection.requiredVersion();
                return ShardUpsertRequest.Item.forUpdate(
                    id,
                    projection.assignments(),
                    requiredVersion == null ? Versions.MATCH_ANY : requiredVersion,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                );
            },
            (req, resp) -> elasticsearchClient.execute(ShardUpsertAction.INSTANCE, req).whenComplete(resp),
            collector);
    }

    @Override
    public Projector visitDeleteProjection(DeleteProjection projection, Context context) {
        checkShardLevel("Delete projection can only be executed on a shard");
        TimeValue reqTimeout = ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(settings);

        ShardDMLExecutor<?, ?, ?, ?> shardDMLExecutor = new ShardDMLExecutor<>(
            context.jobId,
            ShardDMLExecutor.DEFAULT_BULK_SIZE,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            resolveUidCollectExpression(context.txnCtx, projection.uidSymbol()),
            clusterService,
            context.ramAccounting,
            circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY),
            nodeJobsCounter,
            () -> new ShardDeleteRequest(shardId, context.jobId).timeout(reqTimeout),
            ShardDeleteRequest.Item::new,
            (req, resp) -> elasticsearchClient.execute(ShardDeleteAction.INSTANCE, req).whenComplete(resp),
            ShardDMLExecutor.ROW_COUNT_COLLECTOR
        );
        return new DMLProjector(shardDMLExecutor);
    }

    private void checkShardLevel(String errorMessage) {
        if (shardId == null) {
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private CollectExpression<Row, ?> resolveUidCollectExpression(TransactionContext txnCtx, Symbol uidSymbol) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
        ctx.add(uidSymbol);
        return Iterables.getOnlyElement(ctx.expressions());
    }

    @Override
    public Projector visitFetchProjection(FetchProjection projection, Context context) {
        return FetchProjector.create(
            projection,
            context.ramAccounting,
            () -> FetchProjector.computeReaderBucketsByteThreshold(
                circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY)
            ),
            context.txnCtx,
            nodeCtx,
            new TransportFetchOperation(
                req -> elasticsearchClient.execute(FetchNodeAction.INSTANCE, req),
                projection.generateStreamersGroupedByReaderAndNode(),
                context.jobId,
                projection.fetchPhaseId(),
                context.ramAccounting
            )
        );
    }

    @Override
    public Projector visitSysUpdateProjection(SysUpdateProjection projection, Context context) {
        Map<Reference, Symbol> assignments = projection.assignments();
        assert !assignments.isEmpty() : "at least one assignment is required";

        List<Input<?>> valueInputs = new ArrayList<>(assignments.size());
        List<ColumnIdent> assignmentCols = new ArrayList<>(assignments.size());

        RelationName relationName = null;
        InputFactory.Context<NestableCollectExpression<?, ?>> readCtx = null;

        for (Map.Entry<Reference, Symbol> e : assignments.entrySet()) {
            Reference ref = e.getKey();
            assert
                relationName == null || relationName.equals(ref.ident().tableIdent()) : "mixed table assignments found";
            relationName = ref.ident().tableIdent();
            if (readCtx == null) {
                StaticTableDefinition<?> tableDefinition = staticTableDefinitionGetter.apply(relationName);
                readCtx = inputFactory.ctxForRefs(context.txnCtx, tableDefinition.getReferenceResolver());
            }
            assignmentCols.add(ref.column());
            Input<?> sourceInput = readCtx.add(e.getValue());
            valueInputs.add(sourceInput);
        }

        SysRowUpdater<?> rowUpdater = sysUpdaterGetter.apply(relationName);
        assert readCtx != null : "readCtx must not be null";
        assert rowUpdater != null : "row updater needs to exist";
        Consumer<Object> rowWriter = rowUpdater.newRowWriter(assignmentCols, valueInputs, readCtx.expressions());

        if (projection.returnValues() == null) {
            return new SysUpdateProjector(rowWriter);
        } else {
            InputFactory.Context<NestableCollectExpression<SysNodeCheck, ?>> cntx = new InputFactory(
                nodeCtx).ctxForRefs(
                context.txnCtx, new StaticTableReferenceResolver<>(SysNodeChecksTableInfo.create().expressions()));
            cntx.add(List.of(projection.returnValues()));
            return new SysUpdateResultSetProjector(rowUpdater,
                                                   rowWriter,
                                                   cntx.expressions(),
                                                   cntx.topLevelInputs());
        }
    }

    @Override
    public Projector visitProjectSet(ProjectSetProjection projectSet, Context context) {
        ArrayList<Input<Iterable<Row>>> tableFunctions = new ArrayList<>(projectSet.tableFunctions().size());
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(context.txnCtx);

        for (int i = 0; i < projectSet.tableFunctions().size(); i++) {
            Symbol tableFunction = projectSet.tableFunctions().get(i);
            Input<Iterable<Row>> implementation = (Input<Iterable<Row>>) ctx.add(tableFunction);
            tableFunctions.add(implementation);
        }
        ctx.add(projectSet.standalone());
        TableFunctionApplier tableFunctionApplier = new TableFunctionApplier(
            tableFunctions,
            ctx.topLevelInputs(),
            ctx.expressions()
        );
        return new FlatMapProjector(tableFunctionApplier);
    }

    @Override
    public Projector visitWindowAgg(WindowAggProjection windowAgg, Context context) {
        var searchThreadPool = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        return WindowProjector.fromProjection(
            windowAgg,
            nodeCtx,
            inputFactory,
            context.txnCtx,
            context.ramAccounting,
            context.memoryManager,
            clusterService.state().nodes().getMinNodeVersion(),
            indexVersionCreated,
            ThreadPools.numIdleThreads(searchThreadPool, numProcessors),
            searchThreadPool
        );
    }

    @Override
    public Projector visitCorrelatedJoin(CorrelatedJoinProjection correlatedJoin, Context context) {
        return new CorrelatedJoinProjector(
            correlatedJoin.subQueryPlan(),
            correlatedJoin.correlatedSubQuery(),
            correlatedJoin.plannerContext(),
            correlatedJoin.executor(),
            correlatedJoin.subQueryResults(),
            correlatedJoin.params(),
            correlatedJoin.inputPlanOutputs()
        );
    }

    @Override
    public Projector create(Projection projection,
                            TransactionContext txnCtx,
                            RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            UUID jobId) {
        return process(projection, new Context(txnCtx, ramAccounting, memoryManager, jobId));
    }

    @Override
    protected Projector visitProjection(Projection projection, Context context) {
        throw new UnsupportedOperationException("Unsupported projection");
    }

    static class Context {

        private final RamAccounting ramAccounting;
        private final MemoryManager memoryManager;
        private final UUID jobId;
        private final TransactionContext txnCtx;

        public Context(TransactionContext txnCtx,
                       RamAccounting ramAccounting,
                       MemoryManager memoryManager,
                       UUID jobId) {
            this.txnCtx = txnCtx;
            this.ramAccounting = ramAccounting;
            this.memoryManager = memoryManager;
            this.jobId = jobId;
        }
    }
}
