/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.google.common.collect.Iterables;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.executor.transport.ShardDeleteRequest;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.operation.AggregationContext;
import io.crate.operation.InputFactory;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.RowFilter;
import io.crate.operation.TableSettingsResolver;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.operation.projectors.fetch.FetchProjectorContext;
import io.crate.operation.projectors.fetch.TransportFetchOperation;
import io.crate.operation.projectors.sharding.ShardingUpsertExecutor;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.StaticTableDefinition;
import io.crate.operation.reference.sys.SysRowUpdater;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.DeleteProjection;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.ProjectionVisitor;
import io.crate.planner.projection.SourceIndexWriterProjection;
import io.crate.planner.projection.SysUpdateProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.types.StringType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ProjectionToProjectorVisitor
    extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> implements ProjectorFactory {

    private final ClusterService clusterService;
    private final NodeJobsCounter nodeJobsCounter;
    private final Functions functions;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final InputFactory inputFactory;
    private final EvaluatingNormalizer normalizer;
    private final Function<TableIdent, SysRowUpdater<?>> sysUpdaterGetter;
    private final Function<TableIdent, StaticTableDefinition<?>> staticTableDefinitionGetter;
    private final Version indexVersionCreated;
    private final BigArrays bigArrays;
    @Nullable
    private final ShardId shardId;

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        NodeJobsCounter nodeJobsCounter,
                                        Functions functions,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        InputFactory inputFactory,
                                        EvaluatingNormalizer normalizer,
                                        Function<TableIdent, SysRowUpdater<?>> sysUpdaterGetter,
                                        Function<TableIdent, StaticTableDefinition<?>> staticTableDefinitionGetter,
                                        Version indexVersionCreated,
                                        BigArrays bigArrays,
                                        @Nullable ShardId shardId) {
        this.clusterService = clusterService;
        this.nodeJobsCounter = nodeJobsCounter;
        this.functions = functions;
        this.threadPool = threadPool;
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.inputFactory = inputFactory;
        this.normalizer = normalizer;
        this.sysUpdaterGetter = sysUpdaterGetter;
        this.staticTableDefinitionGetter = staticTableDefinitionGetter;
        this.indexVersionCreated = indexVersionCreated;
        this.bigArrays = bigArrays;
        this.shardId = shardId;
    }

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        NodeJobsCounter nodeJobsCounter,
                                        Functions functions,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        InputFactory inputFactory,
                                        EvaluatingNormalizer normalizer,
                                        Function<TableIdent, SysRowUpdater<?>> sysUpdaterGetter,
                                        Function<TableIdent, StaticTableDefinition<?>> staticTableDefinitionGetter,
                                        BigArrays bigArrays) {
        this(clusterService,
            nodeJobsCounter,
            functions,
            threadPool,
            settings,
            transportActionProvider,
            inputFactory,
            normalizer,
            sysUpdaterGetter,
            staticTableDefinitionGetter,
            Version.CURRENT,
            bigArrays,
            null
        );
    }

    @Override
    public Projector visitOrderedTopN(OrderedTopNProjection projection, Context context) {
        /* OrderBy symbols are added to the rows to enable sorting on them post-collect. E.g.:
         *
         * outputs: [x]
         * orderBy: [y]
         *
         * topLevelInputs: [x, y]
         *                    /
         * orderByIndices: [1]
         */
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        ctx.add(projection.outputs());
        ctx.add(projection.orderBy());

        int numOutputs = projection.outputs().size();
        List<Input<?>> inputs = ctx.topLevelInputs();
        int[] orderByIndices = new int[inputs.size() - numOutputs];
        int idx = 0;
        for (int i = numOutputs; i < inputs.size(); i++) {
            orderByIndices[idx++] = i;
        }
        if (projection.limit() > TopN.NO_LIMIT) {
            return new SortingTopNProjector(
                inputs,
                ctx.expressions(),
                numOutputs,
                OrderingByPosition.arrayOrdering(orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
                projection.limit(),
                projection.offset()
            );
        }
        return new SortingProjector(
            inputs,
            ctx.expressions(),
            numOutputs,
            OrderingByPosition.arrayOrdering(orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
            projection.offset()
        );
    }

    @Override
    public Projector visitTopNProjection(TopNProjection projection, Context context) {
        assert projection.limit() > TopN.NO_LIMIT : "TopNProjection must have a limit";
        return new SimpleTopNProjector(projection.limit(), projection.offset());
    }

    @Override
    public Projector visitEvalProjection(EvalProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(projection.outputs());
        return new InputRowProjector(ctx.topLevelInputs(), ctx.expressions());
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForAggregations();

        ctx.add(projection.keys());
        ctx.add(projection.values());

        List<Input<?>> keyInputs = ctx.topLevelInputs();
        return new GroupingProjector(
            Symbols.typeView(projection.keys()),
            keyInputs,
            Iterables.toArray(ctx.expressions(), CollectExpression.class),
            projection.mode(),
            ctx.aggregations().toArray(new AggregationContext[0]),
            context.ramAccountingContext,
            indexVersionCreated,
            bigArrays
        );
    }

    @Override
    public Projector visitMergeCountProjection(MergeCountProjection projection, Context context) {
        return new MergeCountProjector();
    }

    @Override
    public Projector visitAggregationProjection(AggregationProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForAggregations();
        ctx.add(projection.aggregations());
        return new AggregationPipe(
            ctx.expressions(),
            projection.mode(),
            ctx.aggregations().toArray(new AggregationContext[0]),
            context.ramAccountingContext,
            indexVersionCreated,
            bigArrays);
    }

    @Override
    public Projector visitWriterProjection(WriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();

        List<Input<?>> inputs = null;
        if (!projection.inputs().isEmpty()) {
            ctx.add(projection.inputs());
            inputs = ctx.topLevelInputs();
        }

        projection = projection.normalize(normalizer, context.transactionContext);
        String uri = ValueSymbolVisitor.STRING.process(projection.uri());
        assert uri != null : "URI must not be null";

        StringBuilder sb = new StringBuilder(uri);
        Symbol resolvedFileName = normalizer.normalize(WriterProjection.DIRECTORY_TO_FILENAME, context.transactionContext);
        assert resolvedFileName instanceof Literal : "resolvedFileName must be a Literal, but is: " + resolvedFileName;
        assert resolvedFileName.valueType() == StringType.INSTANCE :
            "resolvedFileName.valueType() must be " + StringType.INSTANCE;
        String fileName = ValueSymbolVisitor.STRING.process(resolvedFileName);
        if (!uri.endsWith("/")) {
            sb.append("/");
        }
        sb.append(fileName);
        if (projection.compressionType() == WriterProjection.CompressionType.GZIP) {
            sb.append(".gz");
        }
        uri = sb.toString();

        Map<ColumnIdent, Object> overwrites =
            symbolMapToObject(projection.overwrites(), ctx, context.transactionContext);

        return new FileWriterProjector(
            threadPool.generic(),
            uri,
            projection.compressionType(),
            inputs,
            ctx.expressions(),
            overwrites,
            projection.outputNames(),
            projection.outputFormat()
        );
    }

    private Map<ColumnIdent, Object> symbolMapToObject(Map<ColumnIdent, Symbol> symbolMap,
                                                       InputFactory.Context symbolContext,
                                                       TransactionContext transactionContext) {
        Map<ColumnIdent, Object> objectMap = new HashMap<>(symbolMap.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : symbolMap.entrySet()) {
            Symbol symbol = entry.getValue();
            assert symbol != null : "symbol must not be null";
            objectMap.put(
                entry.getKey(),
                symbolContext.add(normalizer.normalize(symbol, transactionContext)).value()
            );
        }
        return objectMap;
    }

    @Override
    public Projector visitSourceIndexWriterProjection(SourceIndexWriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(ctx.add(partitionedBySymbol));
        }
        Input<?> sourceInput = ctx.add(projection.rawSource());
        Supplier<String> indexNameResolver =
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs);
        Settings tableSettings = TableSettingsResolver.get(clusterService.state().getMetaData(),
            projection.tableIdent(), !projection.partitionedBySymbols().isEmpty());
        return new IndexWriterProjector(
            clusterService,
            nodeJobsCounter,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            functions,
            clusterService.state().metaData().settings(),
            tableSettings,
            transportActionProvider.transportBulkCreateIndicesAction(),
            transportActionProvider.transportShardUpsertAction()::execute,
            indexNameResolver,
            projection.rawSourceReference(),
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            sourceInput,
            ctx.expressions(),
            projection.bulkActions(),
            projection.includes(),
            projection.excludes(),
            projection.autoCreateIndices(),
            projection.overwriteDuplicates(),
            context.jobId
        );
    }

    @Override
    public Projector visitColumnIndexWriterProjection(ColumnIndexWriterProjection projection, Context context) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(ctx.add(partitionedBySymbol));
        }
        List<Input<?>> insertInputs = new ArrayList<>(projection.columnSymbols().size());
        for (Symbol symbol : projection.columnSymbols()) {
            insertInputs.add(ctx.add(symbol));
        }
        Settings tableSettings = TableSettingsResolver.get(clusterService.state().getMetaData(),
            projection.tableIdent(), !projection.partitionedBySymbols().isEmpty());
        return new ColumnIndexWriterProjector(
            clusterService,
            nodeJobsCounter,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            functions,
            clusterService.state().metaData().settings(),
            tableSettings,
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs),
            transportActionProvider,
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            projection.columnReferences(),
            insertInputs,
            ctx.expressions(),
            projection.onDuplicateKeyAssignments(),
            projection.bulkActions(),
            projection.autoCreateIndices(),
            context.jobId
        );
    }

    @Override
    public Projector visitFilterProjection(FilterProjection projection, Context context) {
        Predicate<Row> rowFilter = RowFilter.create(inputFactory, projection.query());
        return new FilterProjector(rowFilter);
    }

    @Override
    public Projector visitUpdateProjection(final UpdateProjection projection, Context context) {
        checkShardLevel("Update projection can only be executed on a shard");
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(settings),
            false,
            false,
            projection.assignmentsColumns(),
            null,
            context.jobId
        );
        ShardDMLExecutor<ShardUpsertRequest, ShardUpsertRequest.Item> shardDMLExecutor = new ShardDMLExecutor<>(
            ShardDMLExecutor.DEFAULT_BULK_SIZE,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            resolveUidCollectExpression(projection.uidSymbol()),
            clusterService,
            nodeJobsCounter,
            () -> builder.newRequest(shardId, null),
            id -> new ShardUpsertRequest.Item(id, projection.assignments(), null, projection.requiredVersion()),
            transportActionProvider.transportShardUpsertAction()::execute
        );

        return new DMLProjector(shardDMLExecutor);
    }

    @Override
    public Projector visitDeleteProjection(DeleteProjection projection, Context context) {
        checkShardLevel("Delete projection can only be executed on a shard");
        ShardDeleteRequest.Builder builder = new ShardDeleteRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(settings),
            context.jobId
        );

        ShardDMLExecutor<ShardDeleteRequest, ShardDeleteRequest.Item> shardDMLExecutor = new ShardDMLExecutor<>(
            ShardDMLExecutor.DEFAULT_BULK_SIZE,
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            resolveUidCollectExpression(projection.uidSymbol()),
            clusterService,
            nodeJobsCounter,
            () -> builder.newRequest(shardId),
            ShardDeleteRequest.Item::new,
            transportActionProvider.transportShardDeleteAction()::execute
        );
        return new DMLProjector(shardDMLExecutor);
    }

    private void checkShardLevel(String errorMessage) {
        if (shardId == null) {
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private CollectExpression<Row, ?> resolveUidCollectExpression(Symbol uidSymbol) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        ctx.add(uidSymbol);
        return Iterables.getOnlyElement(ctx.expressions());
    }

    @Override
    public Projector visitFetchProjection(FetchProjection projection, Context context) {
        FetchProjectorContext projectorContext = new FetchProjectorContext(
            projection.fetchSources(),
            projection.nodeReaders(),
            projection.readerIndices(),
            projection.indicesToIdents()
        );
        return new FetchProjector(
            new TransportFetchOperation(
                transportActionProvider.transportFetchNodeAction(),
                projectorContext.nodeIdsToStreamers(),
                context.jobId,
                projection.collectPhaseId(),
                context.ramAccountingContext
            ),
            functions,
            projection.outputSymbols(),
            projectorContext,
            projection.getFetchSize()
        );
    }

    @Override
    public Projector visitSysUpdateProjection(SysUpdateProjection projection, Context context) {
        Map<Reference, Symbol> assignments = projection.assignments();
        assert !assignments.isEmpty() : "at least one assignement is required";

        List<Input<?>> valueInputs = new ArrayList<>(assignments.size());
        List<ColumnIdent> assignmentCols = new ArrayList<>(assignments.size());

        TableIdent tableIdent = null;
        InputFactory.Context<RowCollectExpression<?, ?>> readCtx = null;

        for (Map.Entry<Reference, Symbol> e : assignments.entrySet()) {
            Reference ref = e.getKey();
            assert tableIdent == null || tableIdent.equals(ref.ident().tableIdent()) : "mixed table assignments found";
            tableIdent = ref.ident().tableIdent();
            if (readCtx == null) {
                StaticTableDefinition<?> tableDefinition = staticTableDefinitionGetter.apply(tableIdent);
                readCtx = inputFactory.ctxForRefs(tableDefinition.getReferenceResolver());
            }
            assignmentCols.add(ref.column());
            Input<?> sourceInput = readCtx.add(e.getValue());
            valueInputs.add(sourceInput);
        }

        SysRowUpdater<?> rowUpdater = sysUpdaterGetter.apply(tableIdent);
        assert readCtx != null : "readCtx must not be null";
        assert rowUpdater != null : "row updater needs to exist";
        Consumer<Object> rowWriter = rowUpdater.newRowWriter(assignmentCols, valueInputs, readCtx.expressions());
        return new SysUpdateProjector(rowWriter);
    }

    @Override
    public Projector create(Projection projection, RamAccountingContext ramAccountingContext, UUID jobId) {
        return process(projection, new Context(ramAccountingContext, jobId));
    }

    @Override
    protected Projector visitProjection(Projection projection, Context context) {
        throw new UnsupportedOperationException("Unsupported projection");
    }

    static class Context {

        private final RamAccountingContext ramAccountingContext;
        private final UUID jobId;
        private final TransactionContext transactionContext = new TransactionContext(SessionContext.create());

        public Context(RamAccountingContext ramAccountingContext, UUID jobId) {
            this.ramAccountingContext = ramAccountingContext;
            this.jobId = jobId;
        }
    }
}
