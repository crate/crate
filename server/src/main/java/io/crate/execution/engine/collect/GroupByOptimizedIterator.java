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

import static io.crate.execution.dsl.projection.Projections.shardProjections;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.formatSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.fetch.ReaderContext;
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
import io.crate.memory.MemoryManager;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataTypes;

final class GroupByOptimizedIterator {

    private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

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
    private static final long HASH_MAP_ENTRY_OVERHEAD = 32; // see private RamUsageEstimator.shallowSizeOfInstance(HashMap.Node.class)

    @Nullable
    static BatchIterator<Row> tryOptimizeSingleStringKey(IndexShard indexShard,
                                                         DocTableInfo table,
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
        keyRef = (Reference) DocReferences.inverseSourceLookup(keyRef);
        MappedFieldType keyFieldType = fieldTypeLookup.get(keyRef.storageIdent());
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
        var searcher = sharedShardContext.acquireSearcher("group-by-ordinals:" + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);

        final QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext();

        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx(collectTask.txnCtx());
        docCtx.add(collectPhase.toCollect().stream()::iterator);

        InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations(collectTask.txnCtx());
        ctxForAggregations.add(groupProjection.values());
        final List<CollectExpression<Row, ?>> aggExpressions = ctxForAggregations.expressions();

        List<AggregationContext> aggregations = ctxForAggregations.aggregations();
        List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();

        RamAccounting ramAccounting = collectTask.getRamAccounting();

        CollectorContext collectorContext = new CollectorContext(sharedShardContext.readerId(), table.droppedColumns(), table.lookupNameBySourceKey());
        InputRow inputRow = new InputRow(docCtx.topLevelInputs());

        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            indexShard.mapperService(),
            indexShard.shardId().getIndexName(),
            queryShardContext,
            table,
            sharedShardContext.indexService().cache()
        );

        return getIterator(
            bigArrays,
            searcher.item(),
            keyRef.storageIdent(),
            aggregations,
            expressions,
            aggExpressions,
            ramAccounting,
            collectTask.memoryManager(),
            collectTask.minNodeVersion(),
            inputRow,
            queryContext.query(),
            collectorContext,
            groupProjection.mode());
    }

    static BatchIterator<Row> getIterator(BigArrays bigArrays,
                                          IndexSearcher indexSearcher,
                                          String keyColumnName,
                                          List<AggregationContext> aggregations,
                                          List<? extends LuceneCollectorExpression<?>> expressions,
                                          List<CollectExpression<Row, ?>> aggExpressions,
                                          RamAccounting ramAccounting,
                                          MemoryManager memoryManager,
                                          Version minNodeVersion,
                                          InputRow inputRow,
                                          Query query,
                                          CollectorContext collectorContext,
                                          AggregateMode aggregateMode) {
        for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
            expressions.get(i).startCollect(collectorContext);
        }

        AtomicReference<Throwable> killed = new AtomicReference<>();
        return CollectingBatchIterator.newInstance(
            () -> killed.set(BatchIterator.CLOSED),
            killed::set,
            () -> {
                try {
                    return CompletableFuture.completedFuture(
                        getRows(
                            applyAggregatesGroupedByKey(
                                bigArrays,
                                indexSearcher,
                                keyColumnName,
                                aggregations,
                                expressions,
                                aggExpressions,
                                ramAccounting,
                                memoryManager,
                                minNodeVersion,
                                inputRow,
                                query,
                                killed
                            ),
                            ramAccounting,
                            aggregations,
                            aggregateMode
                        )
                    );
                } catch (Throwable t) {
                    return CompletableFuture.failedFuture(t);
                }
            },
            true
        );

    }

    private static Iterable<Row> getRows(Map<BytesRef, Object[]> groupedStates,
                                         RamAccounting ramAccounting,
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
                                                                       IndexSearcher indexSearcher,
                                                                       String keyColumnName,
                                                                       List<AggregationContext> aggregations,
                                                                       List<? extends LuceneCollectorExpression<?>> expressions,
                                                                       List<CollectExpression<Row, ?>> aggExpressions,
                                                                       RamAccounting ramAccounting,
                                                                       MemoryManager memoryManager,
                                                                       Version minNodeVersion,
                                                                       InputRow inputRow,
                                                                       Query query,
                                                                       AtomicReference<Throwable> killed) throws IOException {
        final HashMap<BytesRef, Object[]> statesByKey = new HashMap<>();
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        Object[] nullStates = null;

        for (LeafReaderContext leaf: leaves) {
            raiseIfClosedOrKilled(killed);
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            var readerContext = new ReaderContext(leaf);
            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).setNextReader(readerContext);
            }
            SortedSetDocValues values = DocValues.getSortedSet(leaf.reader(), keyColumnName);
            try (ObjectArray<Object[]> statesByOrd = bigArrays.newObjectArray(values.getValueCount())) {
                DocIdSetIterator docs = scorer.iterator();
                Bits liveDocs = leaf.reader().getLiveDocs();
                for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                    raiseIfClosedOrKilled(killed);
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
                            statesByOrd.set(ord, initStates(aggregations, ramAccounting, memoryManager, minNodeVersion));
                        } else {
                            aggregateValues(aggregations, ramAccounting, memoryManager, states);
                        }
                        if (values.docValueCount() > 1) {
                            throw new ArrayViaDocValuesUnsupportedException(keyColumnName);
                        }
                    } else {
                        if (nullStates == null) {
                            nullStates = initStates(aggregations, ramAccounting, memoryManager, minNodeVersion);
                        } else {
                            aggregateValues(aggregations, ramAccounting, memoryManager, nullStates);
                        }
                    }
                }
                for (long ord = 0; ord < statesByOrd.size(); ord++) {
                    raiseIfClosedOrKilled(killed);
                    Object[] states = statesByOrd.get(ord);
                    if (states == null) {
                        continue;
                    }
                    BytesRef sharedKey = values.lookupOrd(ord);
                    Object[] prevStates = statesByKey.get(sharedKey);
                    if (prevStates == null) {
                        ramAccounting.addBytes(BYTES_REF_SHALLOW_SIZE + sharedKey.length + HASH_MAP_ENTRY_OVERHEAD);
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

    static boolean hasHighCardinalityRatio(Supplier<Engine.Searcher> acquireSearcher, String fieldName) {
        // acquire separate searcher:
        // Can't use sharedShardContexts() yet, if we bail out the "getOrCreateContext" causes issues later on in the fallback logic
        try (var searcher = acquireSearcher.get()) {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
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

    private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
        return liveDocs != null && !liveDocs.get(doc);
    }

    private static void aggregateValues(List<AggregationContext> aggregations,
                                        RamAccounting ramAccounting,
                                        MemoryManager memoryManager,
                                        Object[] states) {
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);

            if (InputCondition.matches(aggregation.filter())) {
                //noinspection unchecked
                states[i] = aggregation.function().iterate(
                    ramAccounting,
                    memoryManager,
                    states[i],
                    aggregation.inputs());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static Object[] initStates(List<AggregationContext> aggregations,
                                       RamAccounting ramAccounting,
                                       MemoryManager memoryManager,
                                       Version minNodeVersion) {
        Object[] states = new Object[aggregations.size()];
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);
            AggregationFunction function = aggregation.function();

            var newState = function.newState(ramAccounting, Version.CURRENT, minNodeVersion, memoryManager);
            if (InputCondition.matches(aggregation.filter())) {
                //noinspection unchecked
                states[i] = function.iterate(
                    ramAccounting,
                    memoryManager,
                    newState,
                    aggregation.inputs());
            } else {
                states[i] = newState;
            }
        }
        return states;
    }


    @Nullable
    private static Reference getKeyRef(List<Symbol> toCollect, Symbol key) {
        if (key instanceof InputColumn inputColumn) {
            Symbol keyRef = toCollect.get(inputColumn.index());
            if (keyRef instanceof Reference ref) {
                return ref;
            }
        }
        return null;
    }

    private static GroupProjection getSingleStringKeyGroupProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        Projection shardProjection = shardProjections.iterator().next();
        if (!(shardProjection instanceof GroupProjection groupProjection)) {
            return null;
        }
        if (groupProjection.keys().size() != 1 || groupProjection.keys().get(0).valueType() != DataTypes.STRING) {
            return null;
        }
        return groupProjection;
    }

    private static void raiseIfClosedOrKilled(AtomicReference<Throwable> killed) {
        Throwable killedException = killed.get();
        if (killedException != null) {
            Exceptions.rethrowUnchecked(killedException);
        }
    }
}
