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

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.types.StringType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectionToProjectorVisitor extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> {

    private final ClusterService clusterService;
    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final ImplementationSymbolVisitor symbolVisitor;
    private final EvaluatingNormalizer normalizer;
    @Nullable
    private final ShardId shardId;
    @Nullable
    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer,
                                        @Nullable ShardId shardId,
                                        @Nullable CollectInputSymbolVisitor docInputSymbolVisitor) {
        this.clusterService = clusterService;
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.symbolVisitor = symbolVisitor;
        this.normalizer = normalizer;
        this.shardId = shardId;
        this.docInputSymbolVisitor = docInputSymbolVisitor;
    }

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        ImplementationSymbolVisitor symbolVisitor,
                                        EvaluatingNormalizer normalizer) {
        this(clusterService, settings, transportActionProvider, symbolVisitor, normalizer, null, null);
    }

    public ProjectionToProjectorVisitor(ClusterService clusterService,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        ImplementationSymbolVisitor symbolVisitor) {
        this(clusterService, settings, transportActionProvider, symbolVisitor,
                new EvaluatingNormalizer(
                        symbolVisitor.functions(),
                        symbolVisitor.rowGranularity(),
                        symbolVisitor.referenceResolver())
        );
    }

    public Projector process(Projection projection, RamAccountingContext ramAccountingContext) {
        return super.process(projection, new Context(ramAccountingContext));
    }

    @Override
    public Projector visitTopNProjection(TopNProjection projection, Context context) {
        Projector projector;
        List<Input<?>> inputs = new ArrayList<>();
        List<CollectExpression<?>> collectExpressions = new ArrayList<>();

        ImplementationSymbolVisitor.Context ctx = symbolVisitor.process(projection.outputs());
        inputs.addAll(ctx.topLevelInputs());
        collectExpressions.addAll(ctx.collectExpressions());

        if (projection.isOrdered()) {
            int numOutputs = inputs.size();
            ImplementationSymbolVisitor.Context orderByCtx = symbolVisitor.process(projection.orderBy());

            // append orderby inputs to row, needed for sorting on them
            inputs.addAll(orderByCtx.topLevelInputs());
            collectExpressions.addAll(orderByCtx.collectExpressions());

            int[] orderByIndices = new int[inputs.size() - numOutputs];
            int idx = 0;
            for (int i = numOutputs; i < inputs.size(); i++) {
                orderByIndices[idx++] = i;
            }

            projector = new SortingTopNProjector(
                    inputs.toArray(new Input<?>[inputs.size()]),
                    collectExpressions.toArray(new CollectExpression[collectExpressions.size()]),
                    numOutputs,
                    orderByIndices,
                    projection.reverseFlags(),
                    projection.nullsFirst(),
                    projection.limit(),
                    projection.offset());
        } else {
            projector = new SimpleTopNProjector(
                    inputs,
                    collectExpressions.toArray(new CollectExpression[collectExpressions.size()]),
                    projection.limit(),
                    projection.offset());
        }
        return projector;
    }

    @Override
    public Projector visitMergeProjection(MergeProjection projection, Context context) {
        int[] orderByIndices = new int[projection.orderBy().size()];
        int notInOutputs = 0;
        for (int i = 0; i < projection.orderBy().size(); i++) {
            int idx = projection.outputs().indexOf(projection.orderBy().get(i));
            if (idx >= 0) {
                orderByIndices[i] = idx;
            } else { // symbol is not in the outputs so it must be appended to the inputs
                orderByIndices[i] = projection.outputs().size() + notInOutputs;
                notInOutputs++;
            }
        }
        return new MergeProjector(
                orderByIndices,
                projection.reverseFlags(),
                projection.nullsFirst());
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = symbolVisitor.process(projection.keys());
        List<Input<?>> keyInputs = symbolContext.topLevelInputs();

        for (Aggregation aggregation : projection.values()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        return new GroupingProjector(
                Symbols.extractTypes(projection.keys()),
                keyInputs,
                symbolContext.collectExpressions().toArray(new CollectExpression[symbolContext.collectExpressions().size()]),
                symbolContext.aggregations(),
                context.ramAccountingContext,
                projection.limit()
        );
    }

    @Override
    public Projector visitAggregationProjection(AggregationProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        for (Aggregation aggregation : projection.aggregations()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        return new AggregationProjector(
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
            if (projection.settings().get("compression", "").equalsIgnoreCase("gzip")) {
                sb.append(".gz");
            }
            uri = sb.toString();
        }
        return new WriterProjector(
                uri,
                projection.settings(),
                inputs,
                symbolContext.collectExpressions(),
                overwrites
        );
    }

    protected Map<ColumnIdent, Object> symbolMapToObject(Map<ColumnIdent, Symbol> symbolMap,
                                                         ImplementationSymbolVisitor.Context symbolContext) {
        Map<ColumnIdent, Object> objectMap = new HashMap<>(symbolMap.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : symbolMap.entrySet()) {
            objectMap.put(
                    entry.getKey(),
                    symbolVisitor.process(normalizer.normalize(entry.getValue()), symbolContext).value()
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
        return new IndexWriterProjector(
                clusterService,
                settings,
                transportActionProvider.transportShardUpsertActionDelegate(),
                transportActionProvider.transportCreateIndexAction(),
                projection.tableIdent(),
                projection.partitionIdent(),
                projection.rawSourceReference(),
                projection.primaryKeys(),
                projection.ids(),
                partitionedByInputs,
                projection.clusteredBy(),
                sourceInput,
                projection.rawSource(),
                symbolContext.collectExpressions().toArray(new CollectExpression[symbolContext.collectExpressions().size()]),
                projection.bulkActions(),
                projection.includes(),
                projection.excludes(),
                projection.autoCreateIndices(),
                projection.overwriteDuplicates()
        );
    }

    @Override
    public Projector visitColumnIndexWriterProjection(ColumnIndexWriterProjection projection, Context context) {
        final ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        List<Input<?>> partitionedByInputs = new ArrayList<>(projection.partitionedBySymbols().size());
        for (Symbol partitionedBySymbol : projection.partitionedBySymbols()) {
            partitionedByInputs.add(symbolVisitor.process(partitionedBySymbol, symbolContext));
        }
        return new ColumnIndexWriterProjector(
                clusterService,
                settings,
                transportActionProvider.transportShardUpsertActionDelegate(),
                transportActionProvider.transportCreateIndexAction(),
                projection.tableIdent(),
                projection.partitionIdent(),
                projection.primaryKeys(),
                projection.ids(),
                partitionedByInputs,
                projection.clusteredBy(),
                projection.columnReferences(),
                projection.columnSymbols(),
                symbolContext.collectExpressions().toArray(new CollectExpression[symbolContext.collectExpressions().size()]),
                projection.onDuplicateKeyAssignments(),
                projection.bulkActions(),
                projection.autoCreateIndices()
        );
    }

    @Override
    public Projector visitFilterProjection(FilterProjection projection, Context context) {
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();

        Input<Boolean> condition;
        if (projection.query() != null) {
            condition = (Input)symbolVisitor.process(projection.query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new FilterProjector(
                ctx.collectExpressions().toArray(new CollectExpression[ctx.collectExpressions().size()]),
                condition);
    }

    @Override
    public Projector visitUpdateProjection(UpdateProjection projection, Context context) {
        if (shardId == null || docInputSymbolVisitor == null) {
            throw new UnsupportedOperationException("Update projection can only be executed on a shard");
        }

        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();
        symbolVisitor.process(projection.uidSymbol(), ctx);
        assert ctx.collectExpressions().size() == 1;

        return new UpdateProjector(
                shardId,
                transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                ctx.collectExpressions().toArray(new CollectExpression[ctx.collectExpressions().size()])[0],
                projection.assignmentsColumns(),
                projection.assignments(),
                projection.requiredVersion());
    }

    public static class Context {

        private final RamAccountingContext ramAccountingContext;

        public Context(RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
        }

    }
}
