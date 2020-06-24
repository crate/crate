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

package io.crate.execution.engine.collect;

import io.crate.common.collections.Lists2;
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
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class DocValuesAggregates {

    @Nullable
    public static BatchIterator<Row> tryOptimize(Functions functions,
                                                 IndexShard indexShard,
                                                 DocTableInfo table,
                                                 LuceneQueryBuilder luceneQueryBuilder,
                                                 FieldTypeLookup fieldTypeLookup,
                                                 RoutedCollectPhase phase,
                                                 CollectTask collectTask) {
        var shardProjections = Projections.shardProjections(phase.projections());
        AggregationProjection aggregateProjection = aggregateProjection(shardProjections);
        if (aggregateProjection == null) {
            return null;
        }
        var aggregators = createAggregators(
            functions,
            aggregateProjection,
            fieldTypeLookup,
            phase.toCollect(),
            collectTask.txnCtx().sessionSettings().searchPath()
        );
        if (aggregators == null) {
            return null;
        }
        ShardId shardId = indexShard.shardId();
        SharedShardContext shardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Searcher searcher = shardContext.acquireSearcher(LuceneShardCollectorProvider.formatSource(phase));
        try {
            QueryShardContext queryShardContext = shardContext.indexService().newQueryShardContext();
            collectTask.addSearcher(shardContext.readerId(), searcher);
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
                            killed,
                            searcher,
                            queryContext.query(),
                            aggregators
                        ));
                    } catch (Throwable t) {
                        return CompletableFuture.failedFuture(t);
                    }
                },
                true
            );
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }

    @Nullable
    private static MappedFieldType resolveInputToFieldType(FieldTypeLookup fieldTypeLookup,
                                                           List<Symbol> toCollect,
                                                           Symbol input) {
        if (!(input instanceof InputColumn)) {
            return null;
        }
        Symbol collectSymbol = toCollect.get(((InputColumn) input).index());
        if (!(collectSymbol instanceof Reference)) {
            return null;
        }
        MappedFieldType mappedFieldType = fieldTypeLookup.get(((Reference) collectSymbol).column().fqn());
        if (mappedFieldType != null && !mappedFieldType.hasDocValues()) {
            return null;
        }
        return mappedFieldType;
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    private static List<DocValueAggregator> createAggregators(Functions functions,
                                                              AggregationProjection aggregateProjection,
                                                              FieldTypeLookup fieldTypeLookup,
                                                              List<Symbol> toCollect,
                                                              SearchPath searchPath) {
        List<Aggregation> aggregations = aggregateProjection.aggregations();
        ArrayList<DocValueAggregator> aggregator = new ArrayList<>(aggregations.size());
        Function<Symbol, MappedFieldType> resolveFieldType =
            symbol -> resolveInputToFieldType(fieldTypeLookup, toCollect, symbol);

        for (int i = 0; i < aggregations.size(); i++) {
            Aggregation aggregation = aggregations.get(i);
            if (!aggregation.filter().equals(Literal.BOOLEAN_TRUE)) {
                return null;
            }
            List<MappedFieldType> fieldTypes = Lists2.map(aggregation.inputs(), resolveFieldType);
            if (fieldTypes.stream().anyMatch(Objects::isNull)) {
                // We can extend this to instead return an adapter to the normal aggregation implementation
                return null;
            }

            FunctionImplementation func = functions.getQualified(aggregation, searchPath);
            if (!(func instanceof AggregationFunction)) {
                throw new IllegalStateException(
                    "Expected an aggregationFunction for " + aggregation + " got: " + func);
            }
            DocValueAggregator<?> docValueAggregator = ((AggregationFunction<?, ?>) func).getDocValueAggregator(
                Symbols.typeView(aggregation.inputs()),
                fieldTypes
            );
            if (docValueAggregator == null) {
                return null;
            } else {
                aggregator.add(docValueAggregator);
            }
        }
        return aggregator;
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Iterable<Row> getRow(AtomicReference<Throwable> killed,
                                        Searcher searcher,
                                        Query query,
                                        List<DocValueAggregator> aggregators) throws IOException {
        IndexSearcher indexSearcher = searcher.searcher();
        Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        Object[] cells = new Object[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            cells[i] = aggregators.get(i).initialState();
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
                    aggregators.get(i).apply(cells[i], doc);
                }
            }
        }
        for (int i = 0; i < aggregators.size(); i++) {
            cells[i] = aggregators.get(i).partialResult(cells[i]);
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
