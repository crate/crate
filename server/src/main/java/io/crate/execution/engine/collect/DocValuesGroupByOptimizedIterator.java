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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.memory.MemoryManager;
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

import io.crate.breaker.MultiSizeEstimator;
import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.Exceptions;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.GroupByMaps;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

final class DocValuesGroupByOptimizedIterator {

    @Nullable
    static BatchIterator<Row> tryOptimize(Functions functions,
                                          IndexShard indexShard,
                                          DocTableInfo table,
                                          LuceneQueryBuilder luceneQueryBuilder,
                                          FieldTypeLookup fieldTypeLookup,
                                          DocInputFactory docInputFactory,
                                          RoutedCollectPhase collectPhase,
                                          CollectTask collectTask) {
        if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE)
            || Symbols.containsColumn(collectPhase.where(), DocSysColumns.SCORE)) {
            return null;
        }

        Collection<? extends Projection> shardProjections = shardProjections(collectPhase.projections());
        GroupProjection groupProjection = getSinglePartialGroupProjection(shardProjections);
        if (groupProjection == null) {
            return null;
        }

        ArrayList<Reference> columnKeyRefs = new ArrayList<>(groupProjection.keys().size());
        for (var key : groupProjection.keys()) {
            var docKeyRef = getKeyRef(collectPhase.toCollect(), key);
            if (docKeyRef == null) {
                return null; // group by on non-reference
            }
            var columnKeyRef = (Reference) DocReferences.inverseSourceLookup(docKeyRef);
            var keyFieldType = fieldTypeLookup.get(columnKeyRef.column().fqn());
            if (keyFieldType == null || !keyFieldType.hasDocValues()) {
                return null;
            } else {
                columnKeyRefs.add(columnKeyRef);
            }
        }

        //noinspection rawtypes
        List<DocValueAggregator> aggregators = DocValuesAggregates.createAggregators(
            functions,
            groupProjection.values(),
            collectPhase.toCollect(),
            collectTask.txnCtx().sessionSettings().searchPath(),
            table
        );
        if (aggregators == null) {
            return null;
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = sharedShardContext.acquireSearcher("group-by-doc-value-aggregates: " + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext();

        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx
            = docInputFactory.getCtx(collectTask.txnCtx());
        List<LuceneCollectorExpression<?>> keyExpressions = new ArrayList<>();
        for (var keyRef : columnKeyRefs) {
            keyExpressions.add((LuceneCollectorExpression<?>) docCtx.add(keyRef));
        }
        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            indexShard.mapperService(),
            indexShard.shardId().getIndexName(),
            queryShardContext,
            table,
            sharedShardContext.indexService().cache()
        );

        if (columnKeyRefs.size() == 1) {
            return GroupByIterator.forSingleKey(
                aggregators,
                searcher.item(),
                columnKeyRefs.get(0),
                keyExpressions,
                collectTask.getRamAccounting(),
                collectTask.memoryManager(),
                collectTask.minNodeVersion(),
                queryContext.query(),
                new CollectorContext(sharedShardContext.readerId())
            );
        } else {
            return GroupByIterator.forManyKeys(
                aggregators,
                searcher.item(),
                columnKeyRefs,
                keyExpressions,
                collectTask.getRamAccounting(),
                collectTask.memoryManager(),
                collectTask.minNodeVersion(),
                queryContext.query(),
                new CollectorContext(sharedShardContext.readerId())
            );
        }
    }

    static class GroupByIterator {

        @VisibleForTesting
        static BatchIterator<Row> forSingleKey(List<DocValueAggregator> aggregators,
                                               IndexSearcher indexSearcher,
                                               Reference keyReference,
                                               List<? extends LuceneCollectorExpression<?>> keyExpressions,
                                               RamAccounting ramAccounting,
                                               MemoryManager memoryManager,
                                               Version minNodeVersion,
                                               Query query,
                                               CollectorContext collectorContext) {
            return GroupByIterator.getIterator(
                aggregators,
                indexSearcher,
                keyExpressions,
                ramAccounting,
                memoryManager,
                minNodeVersion,
                GroupByMaps.accountForNewEntry(
                    ramAccounting,
                    SizeEstimatorFactory.create(keyReference.valueType()),
                    null
                ),
                (expressions) -> expressions.get(0).value(),
                (key, cells) -> cells[0] = key,
                query,
                new CollectorContext(collectorContext.readerId())
            );
        }

        @VisibleForTesting
        static <K> BatchIterator<Row> forManyKeys(List<DocValueAggregator> aggregators,
                                                  IndexSearcher indexSearcher,
                                                  List<Reference> keyColumnRefs,
                                                  List<? extends LuceneCollectorExpression<?>> keyExpressions,
                                                  RamAccounting ramAccounting,
                                                  MemoryManager memoryManager,
                                                  Version minNodeVersion,
                                                  Query query,
                                                  CollectorContext collectorContext) {
            return GroupByIterator.getIterator(
                aggregators,
                indexSearcher,
                keyExpressions,
                ramAccounting,
                memoryManager,
                minNodeVersion,
                GroupByMaps.accountForNewEntry(
                    ramAccounting,
                    new MultiSizeEstimator(
                        Lists2.map(keyColumnRefs, Reference::valueType)
                    ),
                    null
                ),
                (expressions) -> {
                    ArrayList<Object> key = new ArrayList<>(keyColumnRefs.size());
                    for (int i = 0; i < expressions.size(); i++) {
                        key.add(expressions.get(i).value());
                    }
                    return key;
                },
                (List<Object> keys, Object[] cells) -> {
                    for (int i = 0; i < keys.size(); i++) {
                        cells[i] = keys.get(i);
                    }
                },
                query,
                new CollectorContext(collectorContext.readerId())
            );
        }

        @VisibleForTesting
        static <K> BatchIterator<Row> getIterator(List<DocValueAggregator> aggregators,
                                                  IndexSearcher indexSearcher,
                                                  List<? extends LuceneCollectorExpression<?>> keyExpressions,
                                                  RamAccounting ramAccounting,
                                                  MemoryManager memoryManager,
                                                  Version minNodeVersion,
                                                  BiConsumer<Map<K, Object[]>, K> accountForNewKeyEntry,
                                                  Function<List<? extends LuceneCollectorExpression<?>>, K> keyExtractor,
                                                  BiConsumer<K, Object[]> applyKeyToCells,
                                                  Query query,
                                                  CollectorContext collectorContext) {
            for (int i = 0; i < keyExpressions.size(); i++) {
                keyExpressions.get(i).startCollect(collectorContext);
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
                                    aggregators,
                                    indexSearcher,
                                    keyExpressions,
                                    accountForNewKeyEntry,
                                    keyExtractor,
                                    ramAccounting,
                                    memoryManager,
                                    minNodeVersion,
                                    query,
                                    killed
                                ),
                                keyExpressions.size(),
                                applyKeyToCells,
                                aggregators,
                                ramAccounting
                            )
                        );
                    } catch (Throwable t) {
                        return CompletableFuture.failedFuture(t);
                    }
                },
                true
            );
        }

        private static <K> Iterable<Row> getRows(Map<K, Object[]> groupedStates,
                                                 int numberOfKeys,
                                                 BiConsumer<K, Object[]> applyKeyToCells,
                                                 List<DocValueAggregator> aggregators,
                                                 RamAccounting ramAccounting) {
            return () -> {
                Object[] cells = new Object[numberOfKeys + aggregators.size()];
                RowN row = new RowN(cells);
                Function<Map.Entry<K, Object[]>, Row> mapper = entry -> {
                    K key = entry.getKey();
                    applyKeyToCells.accept(key, cells);

                    Object[] states = entry.getValue();
                    int c = numberOfKeys;
                    for (int i = 0; i < states.length; i++) {
                        //noinspection unchecked
                        cells[c] = aggregators.get(i).partialResult(ramAccounting, states[i]);
                        c++;
                    }
                    return row;
                };
                return groupedStates.entrySet().stream().map(mapper).iterator();
            };
        }

        private static <K> Map<K, Object[]> applyAggregatesGroupedByKey(
            List<DocValueAggregator> aggregators,
            IndexSearcher indexSearcher,
            List<? extends LuceneCollectorExpression<?>> keyExpressions,
            BiConsumer<Map<K, Object[]>, K> accountForNewKeyEntry,
            Function<List<? extends LuceneCollectorExpression<?>>, K> keyExtractor,
            RamAccounting ramAccounting,
            MemoryManager memoryManager,
            Version minNodeVersion,
            Query query,
            AtomicReference<Throwable> killed
        ) throws IOException {

            HashMap<K, Object[]> statesByKey = new HashMap<>();
            Weight weight = indexSearcher.createWeight(
                indexSearcher.rewrite(query),
                ScoreMode.COMPLETE_NO_SCORES,
                1f
            );
            List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
            for (var leaf : leaves) {
                raiseIfClosedOrKilled(killed);
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                for (int i = 0; i < keyExpressions.size(); i++) {
                    keyExpressions.get(i).setNextReader(new ReaderContext(leaf));
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    aggregators.get(i).loadDocValues(leaf.reader());
                }

                DocIdSetIterator docs = scorer.iterator();
                Bits liveDocs = leaf.reader().getLiveDocs();
                for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                    raiseIfClosedOrKilled(killed);
                    if (docDeleted(liveDocs, doc)) {
                        continue;
                    }

                    for (int i = 0; i < keyExpressions.size(); i++) {
                        keyExpressions.get(i).setNextDocId(doc);
                    }
                    K key = keyExtractor.apply(keyExpressions);

                    Object[] states = statesByKey.get(key);
                    if (states == null) {
                        states = new Object[aggregators.size()];
                        for (int i = 0; i < aggregators.size(); i++) {
                            var aggregator = aggregators.get(i);
                            states[i] = aggregator.initialState(ramAccounting, memoryManager, minNodeVersion);
                            //noinspection unchecked
                            aggregator.apply(ramAccounting, doc, states[i]);
                        }
                        accountForNewKeyEntry.accept(statesByKey, key);
                        statesByKey.put(key, states);
                    } else {
                        for (int i = 0; i < aggregators.size(); i++) {
                            //noinspection unchecked
                            aggregators.get(i).apply(ramAccounting, doc, states[i]);
                        }
                    }
                }
            }
            return statesByKey;
        }

        private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
            return liveDocs != null && !liveDocs.get(doc);
        }

        private static void raiseIfClosedOrKilled(AtomicReference<Throwable> killed) {
            Throwable killedException = killed.get();
            if (killedException != null) {
                Exceptions.rethrowUnchecked(killedException);
            }
        }
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

    private static GroupProjection getSinglePartialGroupProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        Projection shardProjection = shardProjections.iterator().next();
        if (!(shardProjection instanceof GroupProjection) ||
            ((GroupProjection) shardProjection).mode() == AggregateMode.ITER_FINAL) {
            return null;
        }
        return (GroupProjection) shardProjection;
    }
}
