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

package io.crate.execution.engine.collect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.Exceptions;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataTypes;

public class DocValuesAggregates {

    @Nullable
    public static BatchIterator<Row> tryOptimize(Functions functions,
                                                 IndexShard indexShard,
                                                 DocTableInfo table,
                                                 LuceneQueryBuilder luceneQueryBuilder,
                                                 RoutedCollectPhase phase,
                                                 CollectTask collectTask) {
        var shardProjections = Projections.shardProjections(phase.projections());
        AggregationProjection aggregateProjection = aggregateProjection(shardProjections);
        if (aggregateProjection == null) {
            return null;
        }
        var aggregators = createAggregators(
            functions,
            aggregateProjection.aggregations(),
            phase.toCollect(),
            collectTask.txnCtx().sessionSettings().searchPath(),
            table
        );
        if (aggregators == null) {
            return null;
        }
        ShardId shardId = indexShard.shardId();
        SharedShardContext shardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = shardContext.acquireSearcher("doc-value-aggregates: " + LuceneShardCollectorProvider.formatSource(phase));
        collectTask.addSearcher(shardContext.readerId(), searcher);
        QueryShardContext queryShardContext = shardContext.indexService().newQueryShardContext();
        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            phase.where(),
            collectTask.txnCtx(),
            indexShard.mapperService(),
            indexShard.shardId().getIndexName(),
            queryShardContext,
            table,
            shardContext.indexService().cache()
        );

        AtomicReference<Throwable> killed = new AtomicReference<>();
        return CollectingBatchIterator.newInstance(
            () -> killed.set(BatchIterator.CLOSED),
            killed::set,
            () -> {
                try {
                    return CompletableFuture.completedFuture(getRow(
                        collectTask.getRamAccounting(),
                        collectTask.memoryManager(),
                        collectTask.minNodeVersion(),
                        killed,
                        searcher.item(),
                        queryContext.query(),
                        aggregators
                    ));
                } catch (Throwable t) {
                    return CompletableFuture.failedFuture(t);
                }
            },
            true
        );
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    public static List<DocValueAggregator> createAggregators(Functions functions,
                                                             List<Aggregation> aggregations,
                                                             List<Symbol> toCollect,
                                                             SearchPath searchPath,
                                                             DocTableInfo table) {
        ArrayList<DocValueAggregator> aggregator = new ArrayList<>(aggregations.size());
        for (int i = 0; i < aggregations.size(); i++) {
            Aggregation aggregation = aggregations.get(i);
            if (!aggregation.filter().equals(Literal.BOOLEAN_TRUE)) {
                return null;
            }

            var aggregationReferences = new ArrayList<Reference>(aggregation.inputs().size());
            var literals = new ArrayList<Literal<?>>();
            for (var input : aggregation.inputs()) {
                if (input instanceof Literal<?> literal) {
                    literals.add(literal);
                } else {
                    var reference = input.accept(AggregationInputToReferenceResolver.INSTANCE, toCollect);
                    if (reference == null) {
                        // We can extend this to instead return an adapter
                        // to the normal aggregation implementation
                        return null;
                    }
                    aggregationReferences.add(reference);
                }
            }
            FunctionImplementation func = functions.getQualified(aggregation);
            if (!(func instanceof AggregationFunction<?, ?> aggFunc)) {
                throw new IllegalStateException(
                    "Expected an aggregationFunction for " + aggregation + " got: " + func);
            }
            if (aggregationReferences.isEmpty()) {
                return null;
            }
            DocValueAggregator<?> docValueAggregator = aggFunc.getDocValueAggregator(
                aggregationReferences,
                table,
                literals
            );
            if (docValueAggregator == null) {
                return null;
            } else {
                aggregator.add(docValueAggregator);
            }
        }
        return aggregator;
    }

    private static class AggregationInputToReferenceResolver extends SymbolVisitor<List<Symbol>, Reference> {

        public static final AggregationInputToReferenceResolver INSTANCE =
            new AggregationInputToReferenceResolver();

        @Override
        public Reference visitFunction(io.crate.expression.symbol.Function function, List<Symbol> toCollect) {
            if (function.name().equals(ExplicitCastFunction.NAME)) {
                var arg = function.arguments().get(0);
                // Currently, it is the concrete case for the ::numeric explicit cast only.
                // We have to resolve the target column type to be able to potentially get
                // the doc values aggregator.
                if (arg != null && function.valueType().id() == DataTypes.NUMERIC.id()) {
                    return arg.accept(this, toCollect);
                }
            }
            return null;
        }

        @Override
        public Reference visitReference(Reference reference, List<Symbol> context) {
            return reference;
        }

        @Override
        public Reference visitInputColumn(InputColumn inputColumn, List<Symbol> toCollect) {
            Symbol collectSymbol = toCollect.get(inputColumn.index());
            if (collectSymbol == null) {
                return null;
            }
            return collectSymbol.accept(this, toCollect);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Iterable<Row> getRow(RamAccounting ramAccounting,
                                        MemoryManager memoryManager,
                                        Version minNodeVersion,
                                        AtomicReference<Throwable> killed,
                                        IndexSearcher searcher,
                                        Query query,
                                        List<DocValueAggregator> aggregators) throws IOException {
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
        Object[] cells = new Object[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            cells[i] = aggregators.get(i).initialState(ramAccounting, memoryManager, minNodeVersion);
        }
        for (var leaf : leaves) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0; i < aggregators.size(); i++) {
                aggregators.get(i).loadDocValues(leaf.reader());
            }
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                if (liveDocs != null && !liveDocs.get(doc)) {
                    continue;
                }
                Throwable killCause = killed.get();
                if (killCause != null) {
                    Exceptions.rethrowUnchecked(killCause);
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    aggregators.get(i).apply(ramAccounting, doc, cells[i]);
                }
            }
        }
        for (int i = 0; i < aggregators.size(); i++) {
            cells[i] = aggregators.get(i).partialResult(ramAccounting, cells[i]);
        }
        return List.of(new RowN(cells));
    }


    @Nullable
    private static AggregationProjection aggregateProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        var projection = shardProjections.iterator().next();
        if (!(projection instanceof AggregationProjection)) {
            return null;
        }
        return (AggregationProjection) projection;
    }
}
