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

import com.sun.management.ThreadMXBean;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
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
import io.crate.execution.engine.aggregation.GroupByMaps;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.BytesRefColumnReference;
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
import io.crate.types.StringType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.execution.dsl.projection.Projections.shardProjections;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.formatSource;
import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.getCollectorContext;

final class GroupByOptimizedIterator {

    private static final Logger LOGGER = LogManager.getLogger(GroupByOptimizedIterator.class);


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
    private static final long HASH_MAP_ENTRY_OVERHEAD = 32; // see RamUsageEstimator.shallowSizeOfInstance(HashMap.Node.class)

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
            LOGGER.info("========== NOT using GroupByOptimizedIterator cause of group projection not found ==========");
            return null;
        }
        assert groupProjection.keys().size() == 1 : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";
        Reference keyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (keyRef == null) {
            LOGGER.info("========== NOT using GroupByOptimizedIterator cause of non-reference as group key ==========");
            return null; // group by on non-reference
        }
        MappedFieldType keyFieldType = fieldTypeLookup.get(keyRef.column().fqn());
        if (keyFieldType == null || !keyFieldType.hasDocValues()) {
            LOGGER.info("========== NOT using GroupByOptimizedIterator cause of missing DocValues ==========");
            return null;
        }
        if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE)
            || Symbols.containsColumn(collectPhase.where(), DocSysColumns.SCORE)) {
            // We could optimize this, but since it's assumed to be an uncommon case we fallback to generic group-by
            // to keep the optimized implementation a bit simpler
            LOGGER.info("========== NOT using GroupByOptimizedIterator cause of _score is selected ==========");
            return null;
        }
        /*
        if (hasHighCardinalityRatio(() -> indexShard.acquireSearcher("group-by-cardinality-check"), keyFieldType.name())) {
            LOGGER.info("========== NOT using GroupByOptimizedIterator cause of high cardinality ==========");
            return null;
        }
         */
        LOGGER.info("========== Using GroupByOptimizedIterator ==========");

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.acquireSearcher(formatSource(collectPhase));
        try {
            collectTask.addSearcher(sharedShardContext.readerId(), searcher);

            InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx(collectTask.txnCtx());
            docCtx.add(collectPhase.toCollect().stream()::iterator);
            //IndexOrdinalsFieldData keyIndexFieldData = queryShardContext.getForField(keyFieldType);

            InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations(collectTask.txnCtx());
            ctxForAggregations.add(groupProjection.values());

            List<AggregationContext> aggregations = ctxForAggregations.aggregations();
            List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();
            BytesRefColumnReference keyExpression = getKeyExpression(keyRef.column().fqn(), expressions);
            assert keyExpression != null : "Could not find the required key expression for '" + keyRef + "'";

            RamAccountingContext ramAccounting = collectTask.queryPhaseRamAccountingContext();

            QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext();
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
                                    keyExpression,
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

    private static Iterable<Row> getRows(Map<String, Object[]> groupedStates,
                                         RamAccountingContext ramAccounting,
                                         List<AggregationContext> aggregations,
                                         AggregateMode mode) {
        return () -> groupedStates.entrySet().stream()
            .map(new Function<Map.Entry<String, Object[]>, Row>() {

                final Object[] cells = new Object[1 + aggregations.size()];
                final RowN row = new RowN(cells);

                @Override
                public Row apply(Map.Entry<String, Object[]> entry) {
                    cells[0] = entry.getKey();
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

    private static Map<String, Object[]> applyAggregatesGroupedByKey(BigArrays bigArrays,
                                                                       Engine.Searcher searcher,
                                                                       BytesRefColumnReference keyExpression,
                                                                       InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations,
                                                                       List<AggregationContext> aggregations,
                                                                       List<? extends LuceneCollectorExpression<?>> expressions,
                                                                       RamAccountingContext ramAccounting,
                                                                       InputRow inputRow,
                                                                       LuceneQueryBuilder.Context queryContext) throws IOException {
        long usedMemoryStart = memoryUsage();
        long accountedMemoryStart = ramAccounting.totalBytes();

        final Map<String, Object[]> statesByKey = new HashMap<>();
        IndexSearcher indexSearcher = searcher.searcher();
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(queryContext.query()), ScoreMode.COMPLETE, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        final List<CollectExpression<Row, ?>> aggExpressions = ctxForAggregations.expressions();
        Object[] nullStates = null;

        BiConsumer<Map<String, Object[]>, String> accountForNewEntry = GroupByMaps.accountForNewEntry(
            ramAccounting,
            StringSizeEstimator.INSTANCE,
            StringType.INSTANCE);
        List<SizeEstimator<Object>> estimators = new ArrayList<>(aggregations.size());
        for (AggregationContext ctx : aggregations) {
            estimators.add(SizeEstimatorFactory.create(ctx.function().partialType()));
        }
        Consumer<Object[]> accountForNewStates = v -> {
            long size = 0;
            for (int i = 0; i < v.length; i++) {
                size += estimators.get(i).estimateSize(v[i]);
            }
            ramAccounting.addBytes(size);
        };

        logMemUsage("prepared", usedMemoryStart, accountedMemoryStart, ramAccounting);

        for (LeafReaderContext leaf: leaves) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).setNextReader(leaf);
            }

            SortedSetDocValues values = keyExpression.getOrdinalsValues();

            logMemUsage("ordinals loaded", usedMemoryStart, accountedMemoryStart, ramAccounting);
            try (ObjectArray<Object[]> statesByOrd = bigArrays.newObjectArray(values.getValueCount())) {
                DocIdSetIterator docs = scorer.iterator();
                Bits liveDocs = leaf.reader().getLiveDocs();
                for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                    if (docDeleted(liveDocs, doc)) {
                        continue;
                    }
                    onDoc(doc, expressions, ramAccounting);
                    /*
                    for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                        expressions.get(i).setNextDocId(doc);
                    }
                     */
                    for (int i = 0, expressionsSize = aggExpressions.size(); i < expressionsSize; i++) {
                        aggExpressions.get(i).setNextRow(inputRow);
                    }
                    logMemUsage("expressions", usedMemoryStart, accountedMemoryStart, ramAccounting);
                    if (values.advanceExact(doc)) {
                        long ord = values.nextOrd();
                        Object[] states = statesByOrd.get(ord);
                        if (states == null) {
                            statesByOrd.set(ord, initStates(aggregations, ramAccounting));
                        } else {
                            aggregateValues(aggregations, ramAccounting, states);
                        }
                        logMemUsage("states iterated", usedMemoryStart, accountedMemoryStart, ramAccounting);
                        if (values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                            throw new GroupByOnArrayUnsupportedException(keyExpression.getColumnName());
                        }
                    } else {
                        if (nullStates == null) {
                            nullStates = initStates(aggregations, ramAccounting);
                        } else {
                            aggregateValues(aggregations, ramAccounting, nullStates);
                        }
                        logMemUsage("NULL states iterated", usedMemoryStart, accountedMemoryStart, ramAccounting);
                    }
                }
                for (long ord = 0; ord < statesByOrd.size(); ord++) {
                    Object[] states = statesByOrd.get(ord);
                    if (states == null) {
                        continue;
                    }
                    String sharedKey = BytesRefs.toString(values.lookupOrd(ord));
                    Object[] prevStates = statesByKey.get(sharedKey);
                    if (prevStates == null) {
                        accountForNewEntry.accept(statesByKey, sharedKey);
                        accountForNewStates.accept(states);
                        //ramAccounting.addBytes(StringSizeEstimator.estimate(sharedKey) + HASH_MAP_ENTRY_OVERHEAD);
                        //ramAccounting.addBytes(RamUsageEstimator.shallowSizeOf(states));
                        statesByKey.put(sharedKey, states);
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

                logMemUsage("states stored", usedMemoryStart, accountedMemoryStart, ramAccounting);
            }
        }
        if (nullStates != null) {
            accountForNewEntry.accept(statesByKey, null);
            accountForNewStates.accept(nullStates);
            statesByKey.put(null, nullStates);
            logMemUsage("NULL states stored", usedMemoryStart, accountedMemoryStart, ramAccounting);
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
        ramAccounting.addBytes(RamUsageEstimator.shallowSizeOf(states));
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
            int idx = ((InputColumn) key).index();
            Symbol keyRef = toCollect.get(idx);
            if (keyRef instanceof Reference) {
                return (Reference) keyRef;
            }
        }
        return null;
    }

    @Nullable
    private static BytesRefColumnReference getKeyExpression(String columnName,
                                                            List<? extends LuceneCollectorExpression<?>> expressions) {
        for (LuceneCollectorExpression<?> expression : expressions) {
            if (expression instanceof BytesRefColumnReference) {
                BytesRefColumnReference bytesRefExpression = (BytesRefColumnReference) expression;
                if (bytesRefExpression.getColumnName().equals(columnName)) {
                    return bytesRefExpression;
                }
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

    private static void checkCircuitBreaker(RamAccountingContext ramAccountingContext) throws CircuitBreakingException {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new CircuitBreakingException(
                CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                                                                    ramAccountingContext.limit()));
        }
    }

    private static void onDoc(int doc,
                              List<? extends LuceneCollectorExpression<?>> expressions,
                              RamAccountingContext ramAccountingContext) throws IOException {
        checkCircuitBreaker(ramAccountingContext);
        for (LuceneCollectorExpression expression : expressions) {
            expression.setNextDocId(doc);
        }
    }

    private static long memoryUsage() {
        ThreadMXBean bean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
        return bean.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static void logMemUsage(String namespace,
                                    long usedMemoryStart,
                                    long accountedStart,
                                    RamAccountingContext ramAccountingContext) {
        LOGGER.info("[" + namespace + "] Current memory usage: " + (memoryUsage() - usedMemoryStart));
        LOGGER.info("[" + namespace + "] Accounted memory: " + (ramAccountingContext.totalBytes() - accountedStart));
    }
}
