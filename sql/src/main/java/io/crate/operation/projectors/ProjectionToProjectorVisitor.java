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

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.*;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.executor.transport.ShardDeleteRequest;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.metadata.expressions.WritableExpression;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowFilter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.operation.projectors.fetch.FetchProjectorContext;
import io.crate.operation.projectors.fetch.TransportFetchOperation;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.planner.projection.*;
import io.crate.types.StringType;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

public class ProjectionToProjectorVisitor
    extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> implements ProjectorFactory {

    private final ClusterService clusterService;
    private final Functions functions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final ImplementationSymbolVisitor symbolVisitor;
    private final EvaluatingNormalizer normalizer;

    @Nullable
    private final ShardId shardId;

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Functions functions,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer,
                                        @Nullable ShardId shardId) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.symbolVisitor = symbolVisitor;
        this.normalizer = normalizer;
        this.shardId = shardId;
    }

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Functions functions,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer) {
        this(clusterService, functions, indexNameExpressionResolver, threadPool, settings, transportActionProvider, bulkRetryCoordinatorPool, symbolVisitor, normalizer, null);
    }

    @Override
    public Projector visitTopNProjection(TopNProjection projection, Context context) {
        Projector projector;
        List<Input<?>> inputs = new ArrayList<>();
        List<CollectExpression<Row, ?>> collectExpressions = new ArrayList<>();

        ImplementationSymbolVisitor.Context ctx = symbolVisitor.extractImplementations(projection.outputs());
        inputs.addAll(ctx.topLevelInputs());
        collectExpressions.addAll(ctx.collectExpressions());

        if (projection.isOrdered()) {
            int numOutputs = inputs.size();
            ImplementationSymbolVisitor.Context orderByCtx = symbolVisitor.extractImplementations(projection.orderBy());

            // append orderby inputs to row, needed for sorting on them
            inputs.addAll(orderByCtx.topLevelInputs());
            collectExpressions.addAll(orderByCtx.collectExpressions());

            int[] orderByIndices = new int[inputs.size() - numOutputs];
            int idx = 0;
            for (int i = numOutputs; i < inputs.size(); i++) {
                orderByIndices[idx++] = i;
            }

            if (projection.limit() > TopN.NO_LIMIT) {
                projector = new SortingTopNProjector(
                    inputs,
                    collectExpressions,
                    numOutputs,
                    OrderingByPosition.arrayOrdering(orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
                    projection.limit(),
                    projection.offset()
                );
            } else {
                projector = new SortingProjector(
                    inputs,
                    collectExpressions,
                    numOutputs,
                    OrderingByPosition.arrayOrdering(orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
                    projection.offset()
                );
            }
        } else if (projection.limit() == TopN.NO_LIMIT
                   && projection.offset() == TopN.NO_OFFSET) {
            projector = new InputRowProjector(inputs, collectExpressions);
        } else {
            projector = new SimpleTopNProjector(
                inputs,
                collectExpressions,
                projection.limit(),
                projection.offset());
        }
        return projector;
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = symbolVisitor.extractImplementations(projection.keys());
        List<Input<?>> keyInputs = symbolContext.topLevelInputs();

        for (Aggregation aggregation : projection.values()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        return new GroupingProjector(
            Symbols.extractTypes(projection.keys()),
            keyInputs,
            symbolContext.collectExpressions().toArray(new CollectExpression[symbolContext.collectExpressions().size()]),
            symbolContext.aggregations(),
            context.ramAccountingContext
        );
    }

    @Override
    public Projector visitMergeCountProjection(MergeCountProjection projection, Context context) {
        return new MergeCountProjector();
    }

    @Override
    public Projector visitAggregationProjection(AggregationProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        for (Aggregation aggregation : projection.aggregations()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        return new AggregationPipe(
            symbolContext.collectExpressions(),
            symbolContext.aggregations(),
            context.ramAccountingContext);
    }

    @Override
    public Projector visitWriterProjection(WriterProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();

        List<Input<?>> inputs = null;
        if (!projection.inputs().isEmpty()) {
            inputs = new ArrayList<>(projection.inputs().size());
            for (Symbol symbol : projection.inputs()) {
                inputs.add(symbolVisitor.process(symbol, symbolContext));
            }
        }
        Map<ColumnIdent, Object> overwrites = symbolMapToObject(projection.overwrites(), symbolContext, context.transactionContext);

        projection = projection.normalize(normalizer, context.transactionContext);
        String uri = ValueSymbolVisitor.STRING.process(projection.uri());
        assert uri != null : "URI must not be null";

        StringBuilder sb = new StringBuilder(uri);
        Symbol resolvedFileName = normalizer.normalize(WriterProjection.DIRECTORY_TO_FILENAME, context.transactionContext);
        assert resolvedFileName instanceof Literal : "resolvedFileName must be a Literal, but is: " + resolvedFileName;
        assert resolvedFileName.valueType() == StringType.INSTANCE;
        String fileName = ValueSymbolVisitor.STRING.process(resolvedFileName);
        if (!uri.endsWith("/")) {
            sb.append("/");
        }
        sb.append(fileName);
        if (projection.compressionType() == WriterProjection.CompressionType.GZIP) {
            sb.append(".gz");
        }
        uri = sb.toString();

        return new WriterProjector(
            ((ThreadPoolExecutor) threadPool.generic()),
            uri,
            projection.compressionType(),
            inputs,
            symbolContext.collectExpressions(),
            overwrites,
            projection.outputNames(),
            projection.outputFormat()
        );
    }

    private Map<ColumnIdent, Object> symbolMapToObject(Map<ColumnIdent, Symbol> symbolMap,
                                                       ImplementationSymbolVisitor.Context symbolContext,
                                                       TransactionContext transactionContext) {
        Map<ColumnIdent, Object> objectMap = new HashMap<>(symbolMap.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : symbolMap.entrySet()) {
            Symbol symbol = entry.getValue();
            assert symbol != null;
            objectMap.put(
                entry.getKey(),
                symbolVisitor.process(normalizer.normalize(symbol, transactionContext), symbolContext).value()
            );
        }
        return objectMap;
    }

    @Override
    public Projector visitSourceIndexWriterProjection(SourceIndexWriterProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(symbolVisitor.process(partitionedBySymbol, symbolContext));
        }
        Input<?> sourceInput = symbolVisitor.process(projection.rawSource(), symbolContext);
        Supplier<String> indexNameResolver =
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs);
        return new IndexWriterProjector(
            clusterService,
            functions,
            indexNameExpressionResolver,
            clusterService.state().metaData().settings(),
            transportActionProvider,
            indexNameResolver,
            bulkRetryCoordinatorPool,
            projection.rawSourceReference(),
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            sourceInput,
            symbolContext.collectExpressions(),
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
        final ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(symbolVisitor.process(partitionedBySymbol, symbolContext));
        }
        List<Input<?>> insertInputs = new ArrayList<>(projection.columnSymbols().size());
        for (Symbol symbol : projection.columnSymbols()) {
            insertInputs.add(symbolVisitor.process(symbol, symbolContext));
        }
        return new ColumnIndexWriterProjector(
            clusterService,
            functions,
            indexNameExpressionResolver,
            clusterService.state().metaData().settings(),
            IndexNameResolver.create(projection.tableIdent(), projection.partitionIdent(), partitionedByInputs),
            transportActionProvider,
            bulkRetryCoordinatorPool,
            projection.primaryKeys(),
            projection.ids(),
            projection.clusteredBy(),
            projection.clusteredByIdent(),
            projection.columnReferences(),
            insertInputs,
            symbolContext.collectExpressions(),
            projection.onDuplicateKeyAssignments(),
            projection.bulkActions(),
            projection.autoCreateIndices(),
            context.jobId
        );
    }

    @Override
    public Projector visitFilterProjection(FilterProjection projection, Context context) {
        Predicate<Row> rowFilter = RowFilter.create(symbolVisitor, projection.query());
        return new FilterProjector(rowFilter);
    }

    @Override
    public Projector visitUpdateProjection(final UpdateProjection projection, Context context) {
        checkShardLevel("Update projection can only be executed on a shard");

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
            false,
            false,
            projection.assignmentsColumns(),
            null,
            context.jobId
        );
        BulkShardProcessor<ShardUpsertRequest> bulkShardProcessor = new BulkShardProcessor<>(
            clusterService,
            transportActionProvider.transportBulkCreateIndicesAction(),
            indexNameExpressionResolver,
            settings,
            bulkRetryCoordinatorPool,
            false, // autoCreateIndices -> can only update existing things
            BulkShardProcessor.DEFAULT_BULK_SIZE,
            builder,
            transportActionProvider.transportShardUpsertActionDelegate(),
            context.jobId
        );

        return new DMLProjector<>(
            shardId,
            resolveUidCollectExpression(projection.uidSymbol()),
            bulkShardProcessor,
            id -> new ShardUpsertRequest.Item(id, projection.assignments(), null, projection.requiredVersion())
        );
    }

    @Override
    public Projector visitDeleteProjection(DeleteProjection projection, Context context) {
        checkShardLevel("Delete projection can only be executed on a shard");
        ShardDeleteRequest.Builder builder = new ShardDeleteRequest.Builder(
            CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
            context.jobId
        );
        BulkShardProcessor<ShardDeleteRequest> bulkShardProcessor = new BulkShardProcessor<>(
            clusterService,
            transportActionProvider.transportBulkCreateIndicesAction(),
            indexNameExpressionResolver,
            settings,
            bulkRetryCoordinatorPool,
            false,
            BulkShardProcessor.DEFAULT_BULK_SIZE,
            builder,
            transportActionProvider.transportShardDeleteActionDelegate(),
            context.jobId
        );
        return new DMLProjector<>(
            shardId,
            resolveUidCollectExpression(projection.uidSymbol()),
            bulkShardProcessor,
            ShardDeleteRequest.Item::new
        );
    }

    private void checkShardLevel(String errorMessage) {
        if (shardId == null) {
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private CollectExpression<Row, ?> resolveUidCollectExpression(Symbol uidSymbol) {
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();
        symbolVisitor.process(uidSymbol, ctx);
        assert ctx.collectExpressions().size() == 1 : "uidSymbol must resolve to 1 collectExpression";
        return ctx.collectExpressions().iterator().next();
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
                projection.collectPhaseId()
            ),
            threadPool.executor(ThreadPool.Names.SUGGEST),
            symbolVisitor.functions(),
            projection.outputsPerRelation(),
            projectorContext,
            projection.getFetchSize()
        );
    }

    @Override
    public Projector visitSysUpdateProjection(SysUpdateProjection projection, Context context) {
        Map<Reference, Symbol> assignments = projection.assignments();

        CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor =
            new CollectInputSymbolVisitor<>(functions, RowContextReferenceResolver.INSTANCE);
        CollectInputSymbolVisitor.Context readCtx = inputSymbolVisitor.newContext();
        CollectInputSymbolVisitor.Context writeCtx = inputSymbolVisitor.newContext();

        List<Tuple<WritableExpression, Input<?>>> assignmentExpressions = new ArrayList<>(assignments.size());
        for (Map.Entry<Reference, Symbol> e : assignments.entrySet()) {
            Reference ref = e.getKey();

            Input<?> targetCol = inputSymbolVisitor.process(ref, writeCtx);
            if (!(targetCol instanceof WritableExpression)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Column \"%s\" cannot be updated", ref.ident().columnIdent()));
            }

            Input<?> sourceInput = inputSymbolVisitor.process(e.getValue(), readCtx);
            assignmentExpressions.add(
                new Tuple<>(((WritableExpression) targetCol), sourceInput));
        }
        return new SysUpdateProjector(assignmentExpressions, readCtx.docLevelExpressions());
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
        private final TransactionContext transactionContext = new TransactionContext(SessionContext.SYSTEM_SESSION);

        public Context(RamAccountingContext ramAccountingContext, UUID jobId) {
            this.ramAccountingContext = ramAccountingContext;
            this.jobId = jobId;
        }
    }
}
