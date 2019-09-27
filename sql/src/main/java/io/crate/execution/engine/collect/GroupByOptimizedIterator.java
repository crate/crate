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

import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.StringSizeEstimator;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.breaker.RamAccountingContext.roundUp;
import static io.crate.execution.dsl.projection.Projections.shardProjections;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.formatSource;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.getCollectorContext;

final class GroupByOptimizedIterator {

    /**
     * This was chosen after benchmarking different ratios with this optimization always enabled:
     *
     * Q: select count(*) from (select distinct x from t) t
     *
     * cardinality-ratio | mean difference
     * ------------------+-----------------
     *              0.90 |      -5.65%
     *              0.75 |      -5.06%
     *              0.50 |      +1.51%
     *              0.25 |     +38.79%
     *
     * (+ being faster, - being slower)
     */
    private static final double CARDINALITY_RATIO_THRESHOLD = 0.5;

    @Nullable
    static BatchIterator<Row> tryOptimizeSingleStringKey(IndexShard indexShard,
                                                         LuceneQueryBuilder luceneQueryBuilder,
                                                         FieldTypeLookup fieldTypeLookup,
                                                         BigArrays bigArrays,
                                                         InputFactory inputFactory,
                                                         DocInputFactory docInputFactory,
                                                         RoutedCollectPhase collectPhase,
                                                         CollectTask collectTask) {
        Collection<? extends Projection> shardProjections = shardProjections(collectPhase.projections());
        GroupProjection groupProjection = getSingleStringKeyGroupProjection(shardProjections);
        if (groupProjection == null) {
            return null;
        }
        assert groupProjection.keys().size() == 1 : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";
        Reference keyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (keyRef == null) {
            return null; // group by on non-reference
        }
        MappedFieldType keyFieldType = fieldTypeLookup.get(keyRef.column().fqn());
        if (keyFieldType == null || !keyFieldType.hasDocValues()) {
            return null;
        }
        if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE)
            || Symbols.containsColumn(collectPhase.where(), DocSysColumns.SCORE)) {
            // We could optimize this, but since it's assumed to be an uncommon case we fallback to generic group-by
            // to keep the optimized implementation a bit simpler
            return null;
        }
        if (hasHighCardinalityRatio(() -> indexShard.acquireSearcher("group-by-cardinality-check"), keyFieldType.name())) {
            return null;
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.acquireSearcher(formatSource(collectPhase));
        try {
            QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext();
            collectTask.addSearcher(sharedShardContext.readerId(), searcher);

            InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx(collectTask.txnCtx());
            docCtx.add(collectPhase.toCollect().stream()::iterator);
            IndexOrdinalsFieldData keyIndexFieldData = queryShardContext.getForField(keyFieldType);

            InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations(collectTask.txnCtx());
            ctxForAggregations.add(groupProjection.values());

            List<AggregationContext> aggregations = ctxForAggregations.aggregations();
            List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();

            RamAccountingContext ramAccounting = collectTask.queryPhaseRamAccountingContext();

            CollectorContext collectorContext = getCollectorContext(
                sharedShardContext.readerId(), queryShardContext::getForField);

            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).startCollect(collectorContext);
            }
            InputRow inputRow = new InputRow(docCtx.topLevelInputs());

            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                collectPhase.where(),
                collectTask.txnCtx(),
                indexShard.mapperService(),
                queryShardContext,
                sharedShardContext.indexService().cache()
            );
            return CollectingBatchIterator.newInstance(
                searcher::close,
                t -> {},
                () -> {
                    try {
                        return CompletableFuture.completedFuture(
                            getRows(
                                applyAggregatesGroupedByKey(
                                    bigArrays,
                                    searcher,
                                    keyIndexFieldData,
                                    ctxForAggregations,
                                    aggregations,
                                    expressions,
                                    ramAccounting,
                                    inputRow,
                                    queryContext
                                ),
                                ramAccounting,
                                aggregations,
                                groupProjection.mode()
                            )
                        );
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

    static boolean hasHighCardinalityRatio(Supplier<Engine.Searcher> acquireSearcher, String fieldName) {
        // acquire separate searcher:
        // Can't use sharedShardContexts() yet, if we bail out the "getOrCreateContext" causes issues later on in the fallback logic
        try (Engine.Searcher searcher = acquireSearcher.get()) {
            for (LeafReaderContext leaf : searcher.reader().leaves()) {
                Terms terms = leaf.reader().terms(fieldName);
                if (terms == null) {
                    return true;
                }
                double cardinalityRatio = terms.size() / (double) leaf.reader().numDocs();
                if (cardinalityRatio > CARDINALITY_RATIO_THRESHOLD) {
                    return true;
                }
            }
        } catch (IOException e) {
            return true;
        }
        return false;
    }

    private static Iterable<Row> getRows(Map<BytesRef, Object[]> groupedStates,
                                         RamAccountingContext ramAccounting,
                                         List<AggregationContext> aggregations,
                                         AggregateMode mode) {
        return () -> groupedStates.entrySet().stream()
            .map(new Function<Map.Entry<BytesRef, Object[]>, Row>() {

                final Object[] cells = new Object[1 + aggregations.size()];
                final RowN row = new RowN(cells);

                @Override
                public Row apply(Map.Entry<BytesRef, Object[]> entry) {
                    cells[0] = BytesRefs.toString(entry.getKey());
                    Object[] states = entry.getValue();
                    for (int i = 0, c = 1; i < states.length; i++, c++) {
                        //noinspection unchecked
                        cells[c] = mode.finishCollect(ramAccounting, aggregations.get(i).function(), states[i]);
                    }
                    return row;
                }
            })
            .iterator();
    }

    private static Map<BytesRef, Object[]> applyAggregatesGroupedByKey(BigArrays bigArrays,
                                                                       Engine.Searcher searcher,
                                                                       IndexOrdinalsFieldData keyIndexFieldData,
                                                                       InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations,
                                                                       List<AggregationContext> aggregations,
                                                                       List<? extends LuceneCollectorExpression<?>> expressions,
                                                                       RamAccountingContext ramAccounting,
                                                                       InputRow inputRow,
                                                                       LuceneQueryBuilder.Context queryContext) throws IOException {
        final Map<BytesRef, Object[]> statesByKey = new HashMap<>();
        IndexSearcher indexSearcher = searcher.searcher();
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(queryContext.query()), ScoreMode.COMPLETE, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        final List<CollectExpression<Row, ?>> aggExpressions = ctxForAggregations.expressions();
        Object[] nullStates = null;

        for (LeafReaderContext leaf: leaves) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).setNextReader(leaf);
            }
            SortedSetDocValues values = keyIndexFieldData.load(leaf).getOrdinalsValues();
            try (ObjectArray<Object[]> statesByOrd = bigArrays.newObjectArray(values.getValueCount())) {
                DocIdSetIterator docs = scorer.iterator();
                Bits liveDocs = leaf.reader().getLiveDocs();
                for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                    if (docDeleted(liveDocs, doc)) {
                        continue;
                    }
                    for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                        expressions.get(i).setNextDocId(doc);
                    }
                    for (int i = 0, expressionsSize = aggExpressions.size(); i < expressionsSize; i++) {
                        aggExpressions.get(i).setNextRow(inputRow);
                    }
                    if (values.advanceExact(doc)) {
                        long ord = values.nextOrd();
                        Object[] states = statesByOrd.get(ord);
                        if (states == null) {
                            statesByOrd.set(ord, initStates(aggregations, ramAccounting));
                        } else {
                            aggregateValues(aggregations, ramAccounting, states);
                        }
                        if (values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                            throw new GroupByOnArrayUnsupportedException(keyIndexFieldData.getFieldName());
                        }
                    } else {
                        if (nullStates == null) {
                            nullStates = initStates(aggregations, ramAccounting);
                        } else {
                            aggregateValues(aggregations, ramAccounting, nullStates);
                        }
                    }
                }
                for (long ord = 0; ord < statesByOrd.size(); ord++) {
                    Object[] states = statesByOrd.get(ord);
                    if (states == null) {
                        continue;
                    }
                    BytesRef sharedKey = values.lookupOrd(ord);
                    Object[] prevStates = statesByKey.get(sharedKey);
                    if (prevStates == null) {
                        long hashMapEntryOverhead = 36L;
                        ramAccounting.addBytes(roundUp(
                            StringSizeEstimator.INSTANCE.estimateSize(sharedKey) + hashMapEntryOverhead));
                        statesByKey.put(BytesRef.deepCopyOf(sharedKey), states);
                    } else {
                        for (int i = 0; i < aggregations.size(); i++) {
                            AggregationContext aggregation = aggregations.get(i);
                            //noinspection unchecked
                            prevStates[i] = aggregation.function().reduce(
                                ramAccounting,
                                prevStates[i],
                                states[i]
                            );
                        }
                    }
                }
            }
        }
        if (nullStates != null) {
            statesByKey.put(null, nullStates);
        }
        return statesByKey;
    }

    private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
        return liveDocs != null && !liveDocs.get(doc);
    }

    private static void aggregateValues(List<AggregationContext> aggregations,
                                        RamAccountingContext ramAccounting,
                                        Object[] states) {
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);

            if (InputCondition.matches(aggregation.filter())) {
                //noinspection unchecked
                states[i] = aggregation.function().iterate(
                    ramAccounting,
                    states[i],
                    aggregation.inputs()
                );
            }
        }
    }

    private static Object[] initStates(List<AggregationContext> aggregations, RamAccountingContext ramAccounting) {
        Object[] states = new Object[aggregations.size()];
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);
            AggregationFunction function = aggregation.function();

            var newState = function.newState(ramAccounting, Version.CURRENT);
            if (InputCondition.matches(aggregation.filter())) {
                //noinspection unchecked
                states[i] = function.iterate(
                    ramAccounting,
                    newState,
                    aggregation.inputs()
                );
            } else {
                states[i] = newState;
            }
        }
        return states;
    }


    @Nullable
    private static Reference getKeyRef(List<Symbol> toCollect, Symbol key) {
        if (key instanceof InputColumn) {
            Symbol keyRef = toCollect.get(((InputColumn) key).index());
            if (keyRef instanceof Reference) {
                return ((Reference) keyRef);
            }
        }
        return null;
    }

    private static GroupProjection getSingleStringKeyGroupProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        Projection shardProjection = shardProjections.iterator().next();
        if (!(shardProjection instanceof GroupProjection)) {
            return null;
        }
        GroupProjection groupProjection = (GroupProjection) shardProjection;
        if (groupProjection.keys().size() != 1 || groupProjection.keys().get(0).valueType() != DataTypes.STRING) {
            return null;
        }
        return groupProjection;
    }
}
