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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.jspecify.annotations.Nullable;

import io.crate.collections.TwoLevelHashMap;
import io.crate.common.TriConsumer;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.common.concurrent.Killable;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.GroupByMaps;
import io.crate.execution.engine.aggregation.ResizeAwareMap;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.execution.support.ThreadPools;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.memory.MemoryManager;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.DataType;

final class DocValuesGroupByOptimizedIterator {

    /**
     * DocValueAggregator/LuceneCollectorExpression instances carry per-leaf mutable state
     * (e.g. the current segment's docValues), so each concurrently processed leaf needs its
     * own instances. This bundle is what {@link Supplier#get()} builds fresh, once per leaf.
     */
    @SuppressWarnings("rawtypes")
    record LeafState(
        List<DocValueAggregator> aggregators,
        List<LuceneCollectorExpression<?>> keyExpressions,
        MemoryManager memoryManager,
        CollectorContext collectorContext
    ) {
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    static BatchIterator<Row> tryOptimize(Functions functions,
                                          LuceneReferenceResolver referenceResolver,
                                          IndexShard indexShard,
                                          DocTableInfo table,
                                          List<String> partitionValues,
                                          LuceneQueryBuilder luceneQueryBuilder,
                                          DocInputFactory docInputFactory,
                                          RoutedCollectPhase collectPhase,
                                          CollectTask collectTask,
                                          ThreadPoolExecutor executor) {
        if (Symbols.hasColumn(collectPhase.toCollect(), SysColumns.SCORE)
            || collectPhase.where().hasColumn(SysColumns.SCORE)) {
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
            if (!columnKeyRef.hasDocValues()) {
                return null;
            } else {
                columnKeyRefs.add(columnKeyRef);
            }
        }

        Version shardCreatedVersion = indexShard.getVersionCreated();

        Supplier<List<DocValueAggregator>> aggregatorsFactory = () -> DocValuesAggregates.createAggregators(
            functions,
            referenceResolver,
            groupProjection.values(),
            collectPhase.toCollect(),
            table,
            shardCreatedVersion
        );
        List<DocValueAggregator> aggregators = aggregatorsFactory.get();
        if (aggregators == null) {
            return null;
        }
        // Safe to cast: createAggregators above already resolved each aggregation to an
        // AggregationFunction (it throws otherwise), so re-resolving here cannot fail.
        List<AggregationFunction> reducers = new ArrayList<>(groupProjection.values().size());
        for (var aggregation : groupProjection.values()) {
            reducers.add((AggregationFunction) functions.getQualified(aggregation));
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = sharedShardContext.acquireSearcher("group-by-doc-value-aggregates: " + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        IndexService indexService = sharedShardContext.indexService();

        Supplier<List<LuceneCollectorExpression<?>>> keyExpressionsFactory = () -> {
            InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = docInputFactory.getCtx(collectTask.txnCtx());
            List<LuceneCollectorExpression<?>> exprs = new ArrayList<>(columnKeyRefs.size());
            for (var keyRef : columnKeyRefs) {
                exprs.add((LuceneCollectorExpression<?>) ctx.add(keyRef));
            }
            return exprs;
        };
        Supplier<RamAccounting> ramAccountingFactory = collectTask::getRamAccounting;
        Supplier<LeafState> leafStateFactory = () -> new LeafState(
            aggregatorsFactory.get(),
            keyExpressionsFactory.get(),
            collectTask.memoryManager(),
            new CollectorContext(sharedShardContext.readerId(), () -> StoredRowLookup.create(shardCreatedVersion, table, partitionValues))
        );

        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            partitionValues,
            indexService.indexAnalyzers(),
            table,
            shardCreatedVersion,
            indexService.cache(),
            collectTask.killToken()::raiseIfKilled
        );

        if (columnKeyRefs.size() == 1) {
            return GroupByIterator.forSingleKey(
                leafStateFactory,
                ramAccountingFactory,
                reducers,
                searcher.item(),
                columnKeyRefs.get(0),
                collectTask.minNodeVersion(),
                queryContext.query(),
                executor
            );
        } else {
            return GroupByIterator.forManyKeys(
                leafStateFactory,
                ramAccountingFactory,
                reducers,
                searcher.item(),
                columnKeyRefs,
                collectTask.minNodeVersion(),
                queryContext.query(),
                executor
            );
        }
    }

    static final class GroupByIterator {

        private GroupByIterator() {}

        @SuppressWarnings("rawtypes")
        @VisibleForTesting
        static BatchIterator<Row> forSingleKey(Supplier<LeafState> leafStateFactory,
                                               Supplier<RamAccounting> ramAccountingFactory,
                                               List<AggregationFunction> reducers,
                                               IndexSearcher indexSearcher,
                                               Reference keyReference,
                                               Version minNodeVersion,
                                               Query query,
                                               ThreadPoolExecutor executor) {
            //noinspection unchecked
            DataType<Object> valueType = (DataType<Object>) keyReference.valueType();
            return GroupByIterator.getIterator(
                leafStateFactory,
                ramAccountingFactory,
                reducers,
                indexSearcher,
                1,
                minNodeVersion,
                ra -> GroupByMaps.accountForNewEntry(ra, valueType),
                (expressions) -> expressions.get(0).value(),
                (key, cells) -> cells[0] = key,
                query,
                executor
            );
        }

        @SuppressWarnings("rawtypes")
        @VisibleForTesting
        static BatchIterator<Row> forManyKeys(Supplier<LeafState> leafStateFactory,
                                              Supplier<RamAccounting> ramAccountingFactory,
                                              List<AggregationFunction> reducers,
                                              IndexSearcher indexSearcher,
                                              List<Reference> keyColumnRefs,
                                              Version minNodeVersion,
                                              Query query,
                                              ThreadPoolExecutor executor) {
            return GroupByIterator.getIterator(
                leafStateFactory,
                ramAccountingFactory,
                reducers,
                indexSearcher,
                keyColumnRefs.size(),
                minNodeVersion,
                ra -> GroupByMaps.accountForNewEntry(ra, Lists.map(keyColumnRefs, Reference::valueType)),
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
                executor
            );
        }

        @SuppressWarnings("rawtypes")
        @VisibleForTesting
        static <K> BatchIterator<Row> getIterator(Supplier<LeafState> leafStateFactory,
                                                  Supplier<RamAccounting> ramAccountingFactory,
                                                  List<AggregationFunction> reducers,
                                                  IndexSearcher indexSearcher,
                                                  int numberOfKeys,
                                                  Version minNodeVersion,
                                                  Function<RamAccounting, TriConsumer<ResizeAwareMap<K, Object[]>, K, Object[]>> accountForNewKeyEntryFactory,
                                                  Function<List<? extends LuceneCollectorExpression<?>>, K> keyExtractor,
                                                  BiConsumer<K, Object[]> applyKeyToCells,
                                                  Query query,
                                                  ThreadPoolExecutor executor) {
            Killable.Token killToken = new Killable.Token();
            int numberOfAggregates = reducers.size();
            return CollectingBatchIterator.newInstance(
                killToken,
                () -> getRows(
                    collectGroupedByKey(
                        leafStateFactory,
                        ramAccountingFactory,
                        reducers,
                        indexSearcher,
                        numberOfAggregates,
                        minNodeVersion,
                        accountForNewKeyEntryFactory,
                        keyExtractor,
                        query,
                        killToken,
                        executor
                    ),
                    numberOfKeys,
                    applyKeyToCells,
                    numberOfAggregates
                ),
                true
            );
        }

        private static <K> Iterable<Row> getRows(TwoLevelHashMap<K, Object[]> groupedPartialsByKey,
                                                  int numberOfKeys,
                                                  BiConsumer<K, Object[]> applyKeyToCells,
                                                  int numberOfAggregates) {
            return () -> {
                Object[] cells = new Object[numberOfKeys + numberOfAggregates];
                RowN row = new RowN(cells);
                Function<Map.Entry<K, Object[]>, Row> mapper = entry -> {
                    K key = entry.getKey();
                    applyKeyToCells.accept(key, cells);
                    Object[] partials = entry.getValue();
                    System.arraycopy(partials, 0, cells, numberOfKeys, partials.length);
                    return row;
                };
                return StreamSupport.stream(groupedPartialsByKey.spliterator(), false).map(mapper).iterator();
            };
        }

        /**
         * Runs one task per leaf (segment) on {@code executor} -- a pool dedicated to this kind
         * of fan-out, distinct from the SEARCH pool this whole batch-iterator pipeline already
         * runs on, so blocking here on the joined result can't self-deadlock that pool.
         */
        @SuppressWarnings("rawtypes")
        private static <K> TwoLevelHashMap<K, Object[]> collectGroupedByKey(
            Supplier<LeafState> leafStateFactory,
            Supplier<RamAccounting> ramAccountingFactory,
            List<AggregationFunction> reducers,
            IndexSearcher indexSearcher,
            int numberOfAggregates,
            Version minNodeVersion,
            Function<RamAccounting, TriConsumer<ResizeAwareMap<K, Object[]>, K, Object[]>> accountForNewKeyEntryFactory,
            Function<List<? extends LuceneCollectorExpression<?>>, K> keyExtractor,
            Query query,
            Killable.Token killToken,
            ThreadPoolExecutor executor
        ) throws IOException {
            Weight weight = indexSearcher.createWeight(
                indexSearcher.rewrite(query),
                ScoreMode.COMPLETE_NO_SCORES,
                1f
            );
            List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
            List<Supplier<TwoLevelHashMap<K, Object[]>>> tasks = new ArrayList<>(leaves.size());
            for (var leaf : leaves) {
                tasks.add(() -> {
                    try {
                        return processLeaf(
                            leafStateFactory.get(),
                            ramAccountingFactory.get(),
                            weight,
                            leaf,
                            numberOfAggregates,
                            minNodeVersion,
                            accountForNewKeyEntryFactory,
                            keyExtractor,
                            killToken
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            List<TwoLevelHashMap<K, Object[]>> leafResults;
            try {
                leafResults = ThreadPools.runWithAvailableThreads(
                    executor,
                    ThreadPools.numIdleThreads(executor, Runtime.getRuntime().availableProcessors()),
                    tasks
                ).join();
            } catch (CompletionException e) {
                RuntimeException re = Exceptions.toRuntimeException(e);
                if (re instanceof UncheckedIOException uio) {
                    throw uio.getCause();
                }
                throw re;
            }
            return mergeLeafResults(leafResults, reducers, ramAccountingFactory, executor);
        }

        @SuppressWarnings("rawtypes")
        private static <K> TwoLevelHashMap<K, Object[]> processLeaf(
            LeafState leafState,
            RamAccounting ramAccounting,
            Weight weight,
            LeafReaderContext leaf,
            int numberOfAggregates,
            Version minNodeVersion,
            Function<RamAccounting, TriConsumer<ResizeAwareMap<K, Object[]>, K, Object[]>> accountForNewKeyEntryFactory,
            Function<List<? extends LuceneCollectorExpression<?>>, K> keyExtractor,
            Killable.Token killToken
        ) throws IOException {
            killToken.raiseIfKilled();

            List<DocValueAggregator> aggregators = leafState.aggregators();
            List<LuceneCollectorExpression<?>> keyExpressions = leafState.keyExpressions();
            MemoryManager memoryManager = leafState.memoryManager();
            CollectorContext collectorContext = leafState.collectorContext();
            for (int i = 0; i < keyExpressions.size(); i++) {
                keyExpressions.get(i).startCollect(collectorContext);
            }

            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                return new TwoLevelHashMap<>();
            }

            TriConsumer<ResizeAwareMap<K, Object[]>, K, Object[]> accountForNewKeyEntry =
                accountForNewKeyEntryFactory.apply(ramAccounting);
            // Accumulate directly into the bucketed map (routing each key via its own bucket's
            // ResizeAwareMap wrapper) instead of building a flat HashMap and copying it into a
            // TwoLevelHashMap afterwards -- that copy would double the insert cost per leaf.
            TwoLevelHashMap<K, Object[]> statesByKey = new TwoLevelHashMap<>();
            @SuppressWarnings("unchecked")
            ResizeAwareMap<K, Object[]>[] bucketWrappers = new ResizeAwareMap[statesByKey.numBuckets()];
            for (int i = 0; i < bucketWrappers.length; i++) {
                bucketWrappers[i] = GroupByMaps.wrapperForJDKMap(statesByKey.bucket(i));
            }

            for (int i = 0; i < keyExpressions.size(); i++) {
                keyExpressions.get(i).setNextReader(new ReaderContext(leaf));
            }
            for (int i = 0; i < aggregators.size(); i++) {
                aggregators.get(i).loadDocValues(leaf);
            }

            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                killToken.raiseIfKilled();
                if (docDeleted(liveDocs, doc)) {
                    continue;
                }

                for (int i = 0; i < keyExpressions.size(); i++) {
                    keyExpressions.get(i).setNextDocId(doc);
                }
                K key = keyExtractor.apply(keyExpressions);
                ResizeAwareMap<K, Object[]> bucketMap = bucketWrappers[statesByKey.bucketIndex(key)];

                Object[] states = bucketMap.get(key);
                if (states == null) {
                    states = new Object[aggregators.size()];
                    for (int i = 0; i < aggregators.size(); i++) {
                        var aggregator = aggregators.get(i);

                        Object state = aggregator.initialState(ramAccounting, memoryManager, minNodeVersion);

                        //noinspection unchecked
                        state = aggregator.apply(ramAccounting, doc, state);
                        states[i] = state;
                    }
                    accountForNewKeyEntry.accept(bucketMap, key, states);
                    bucketMap.put(key, states);
                } else {
                    for (int i = 0; i < aggregators.size(); i++) {
                        //noinspection unchecked
                        states[i] = aggregators.get(i).apply(ramAccounting, doc, states[i]);
                    }
                }
            }

            // Overwrite each entry's states array in place with its partial result -- same keys,
            // same buckets, so this needs no re-insertion/rehashing, just a value-array mutation.
            for (var entry : statesByKey) {
                Object[] rawStates = entry.getValue();
                for (int i = 0; i < numberOfAggregates; i++) {
                    //noinspection unchecked
                    rawStates[i] = aggregators.get(i).partialResult(ramAccounting, rawStates[i]);
                }
            }
            return statesByKey;
        }

        /**
         * Bucket index depends only on the key's hash, so bucket N of every leaf result and of
         * {@code result} all hold the exact same set of possible keys -- merging bucket N is
         * independent of every other bucket and can run on its own thread.
         */
        @SuppressWarnings("rawtypes")
        private static <K> TwoLevelHashMap<K, Object[]> mergeLeafResults(List<TwoLevelHashMap<K, Object[]>> leafResults,
                                                                          List<AggregationFunction> reducers,
                                                                          Supplier<RamAccounting> ramAccountingFactory,
                                                                          ThreadPoolExecutor executor) {
            if (leafResults.size() <= 1) {
                return leafResults.isEmpty() ? new TwoLevelHashMap<>() : leafResults.get(0);
            }
            TwoLevelHashMap<K, Object[]> result = new TwoLevelHashMap<>();
            int numBuckets = result.numBuckets();
            List<Supplier<Void>> tasks = new ArrayList<>(numBuckets);
            for (int b = 0; b < numBuckets; b++) {
                int bucketIdx = b;
                tasks.add(() -> {
                    mergeBucket(bucketIdx, result, leafResults, reducers, ramAccountingFactory.get());
                    return null;
                });
            }
            try {
                ThreadPools.runWithAvailableThreads(
                    executor,
                    ThreadPools.numIdleThreads(executor, Runtime.getRuntime().availableProcessors()),
                    tasks
                ).join();
            } catch (CompletionException e) {
                throw Exceptions.toRuntimeException(e);
            }
            return result;
        }

        @SuppressWarnings("rawtypes")
        private static <K> void mergeBucket(int bucketIdx,
                                            TwoLevelHashMap<K, Object[]> result,
                                            List<TwoLevelHashMap<K, Object[]>> leafResults,
                                            List<AggregationFunction> reducers,
                                            RamAccounting ramAccounting) {
            Map<K, Object[]> target = result.bucket(bucketIdx);
            for (var leafResult : leafResults) {
                Map<K, Object[]> source = leafResult.bucket(bucketIdx);
                for (var entry : source.entrySet()) {
                    target.merge(entry.getKey(), entry.getValue(), (a, b) -> {
                        Object[] out = new Object[reducers.size()];
                        for (int j = 0; j < reducers.size(); j++) {
                            //noinspection unchecked
                            out[j] = reducers.get(j).reduce(ramAccounting, a[j], b[j]);
                        }
                        return out;
                    });
                }
            }
        }

        private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
            return liveDocs != null && !liveDocs.get(doc);
        }
    }

    @Nullable
    private static Reference getKeyRef(List<Symbol> toCollect, Symbol key) {
        if (key instanceof InputColumn inputCol) {
            Symbol keyRef = toCollect.get(inputCol.index());
            if (keyRef instanceof Reference ref) {
                return ref;
            }
        }
        return null;
    }

    private static GroupProjection getSinglePartialGroupProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        Projection shardProjection = shardProjections.iterator().next();
        if (!(shardProjection instanceof GroupProjection groupProjection) ||
            groupProjection.mode() == AggregateMode.ITER_FINAL) {
            return null;
        }
        return groupProjection;
    }
}
