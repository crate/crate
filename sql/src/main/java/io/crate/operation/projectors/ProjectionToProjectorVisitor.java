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

import com.google.common.base.Supplier;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.*;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.planner.projection.*;
import io.crate.types.StringType;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

public class ProjectionToProjectorVisitor
        extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> implements ProjectorFactory {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private ThreadPool threadPool;
    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final ImplementationSymbolVisitor symbolVisitor;
    private final EvaluatingNormalizer normalizer;

    @Nullable
    private final ShardId shardId;

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer,
                                        @Nullable ShardId shardId) {
        this.clusterService = clusterService;
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
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer) {
        this(clusterService, indexNameExpressionResolver, threadPool, settings, transportActionProvider, bulkRetryCoordinatorPool, symbolVisitor, normalizer, null);
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

            projector = new SortingTopNProjector(
                    inputs,
                    collectExpressions,
                    numOutputs,
                    OrderingByPosition.arrayOrdering(orderByIndices, projection.reverseFlags(), projection.nullsFirst()),
                    projection.limit(),
                    projection.offset()
            );
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
        Map<ColumnIdent, Object> overwrites = symbolMapToObject(projection.overwrites(), symbolContext);

        projection = projection.normalize(normalizer);
        String uri = ValueSymbolVisitor.STRING.process(projection.uri());
        assert uri != null : "URI must not be null";
        if (projection.isDirectoryUri()) {
            StringBuilder sb = new StringBuilder(uri);
            Symbol resolvedFileName = normalizer.normalize(WriterProjection.DIRECTORY_TO_FILENAME);
            assert resolvedFileName instanceof Literal;
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
        }
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

    protected Map<ColumnIdent, Object> symbolMapToObject(Map<ColumnIdent, Symbol> symbolMap,
                                                         ImplementationSymbolVisitor.Context symbolContext) {
        Map<ColumnIdent, Object> objectMap = new HashMap<>(symbolMap.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : symbolMap.entrySet()) {
            Symbol symbol = entry.getValue();
            assert symbol != null;
            objectMap.put(
                    entry.getKey(),
                    symbolVisitor.process(normalizer.normalize(symbol), symbolContext).value()
            );
        }
        return objectMap;
    }

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
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();

        Input<Boolean> condition;
        if (projection.query() != null) {
            condition = (Input) symbolVisitor.process(projection.query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new FilterProjector(ctx.collectExpressions(), condition);
    }

    @Override
    public Projector visitUpdateProjection(UpdateProjection projection, Context context) {
        checkShardLevel("Update projection can only be executed on a shard");

        return new UpdateProjector(
                clusterService,
                indexNameExpressionResolver,
                settings,
                shardId,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                resolveUidCollectExpression(projection),
                projection.assignmentsColumns(),
                projection.assignments(),
                projection.requiredVersion(),
                context.jobId);
    }

    @Override
    public Projector visitDeleteProjection(DeleteProjection projection, Context context) {
        checkShardLevel("Delete projection can only be executed on a shard");

        return new DeleteProjector(
                clusterService,
                indexNameExpressionResolver,
                settings,
                shardId,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                resolveUidCollectExpression(projection),
                context.jobId);
    }

    private void checkShardLevel(String errorMessage) {
        if (shardId == null) {
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private CollectExpression<Row, ?> resolveUidCollectExpression(DMLProjection projection) {
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();
        symbolVisitor.process(projection.uidSymbol(), ctx);
        assert ctx.collectExpressions().size() == 1;

        return ctx.collectExpressions().iterator().next();
    }

    @Override
    public Projector visitFetchProjection(FetchProjection projection, Context context) {
        return new FetchProjector(
                transportActionProvider.transportFetchNodeAction(),
                threadPool,
                symbolVisitor.functions(),
                context.jobId,
                projection.collectPhaseId(),
                projection.fetchSources(),
                projection.outputSymbols(),
                projection.nodeReaders(),
                projection.readerIndices(),
                projection.indicesToIdents());
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

        public Context(RamAccountingContext ramAccountingContext, UUID jobId) {
            this.ramAccountingContext = ramAccountingContext;
            this.jobId = jobId;
        }
    }
}
