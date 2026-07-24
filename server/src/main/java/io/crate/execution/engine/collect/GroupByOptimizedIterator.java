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

import static io.crate.execution.engine.collect.LuceneShardCollectorProvider.formatSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.jspecify.annotations.Nullable;

import com.carrotsearch.hppc.ObjectLongHashMap;

import io.crate.common.MutableLong;
import io.crate.common.concurrent.Killable;
import io.crate.common.concurrent.Killable.Token;
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
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.memory.MemoryManager;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.DataTypes;
import io.netty.util.collection.LongObjectHashMap;

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



    /// Returns a batchIterator that uses termFrequences for queries like:
    ///
    ///     SELECT strKey, count(*) GROUP BY strKey`
    ///
    /// Only works if the key is not nullable and if there is no WHERE clause
    /// and no aggregate filter.
    /// Returns null for other queries.
    @Nullable
    static BatchIterator<Row> tryUseTermFrequencies(IndexShard indexShard,
                                                    RoutedCollectPhase collectPhase,
                                                    CollectTask collectTask) {
        GroupProjection groupProjection = getSingleStringKeyGroupProjection(collectPhase.projections());
        if (groupProjection == null) {
            return null;
        }
        assert groupProjection.keys().size() == 1
            : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";

        Reference docKeyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (docKeyRef == null) {
            return null; // group by on non-reference
        }

        List<Aggregation> values = groupProjection.values();
        if (values.size() != 1 || !values.getFirst().signature().equals(CountAggregation.COUNT_STAR_SIGNATURE)) {
            return null;
        }

        Symbol aggregateFilter = values.getFirst().filter();
        if (!aggregateFilter.equals(Literal.BOOLEAN_TRUE)) {
            return null;
        }

        Symbol where = collectPhase.where();
        if (!where.equals(Literal.BOOLEAN_TRUE)) {
            return null;
        }

        final Reference keyRef = DocReferences.docRefToRegularRef(docKeyRef);

        if (!hasTerms(() -> indexShard.acquireSearcher("terms-check"), keyRef.storageIdent())) {
            return null;
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcherRef = sharedShardContext.acquireSearcher("group-by-ordinals:" + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcherRef);
        IndexSearcher searcher = searcherRef.item();

        Token killToken = new Killable.Token();
        AggregateMode mode = groupProjection.mode();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> countsToRows(getCountsByKey(collectTask.getRamAccounting(), keyRef, searcher, killToken), mode),
            true
        );
    }

    private static Iterable<Row> countsToRows(ObjectLongHashMap<BytesRef> countsByKey, AggregateMode mode) {
        final Object[] cells = new Object[2];
        final Row row = new RowN(cells);
        LongFunction<Object> wrapCount = switch (mode) {
            case ITER_PARTIAL -> MutableLong::new;
            case ITER_FINAL -> x -> x;
            case PARTIAL_FINAL -> throw new UnsupportedOperationException(
                "Shard level projection cannot start at PARTIAL");
        };
        return () -> StreamSupport.stream(countsByKey.spliterator(), false)
            .map(cursor -> {
                BytesRef key = cursor.key;
                long count = cursor.value;
                // See GroupProjection.outputs(): keys always come first, aggregations second
                cells[0] = key == null ? null : key.utf8ToString();
                cells[1] = wrapCount.apply(count);
                return row;
            })
            .iterator();
    }

    private static ObjectLongHashMap<BytesRef> getCountsByKey(RamAccounting ramAccounting,
                                                              Reference keyRef,
                                                              IndexSearcher searcher,
                                                              Token killToken) throws IOException {
        ObjectLongHashMap<BytesRef> countsByKey = new ObjectLongHashMap<>();
        String keyStorageIdent = keyRef.storageIdent();
        PostingsEnum postings = null;
        for (var leaf : searcher.getLeafContexts()) {
            LeafReader reader = leaf.reader();
            Terms terms = reader.terms(keyStorageIdent);
            if (terms == null) {
                continue;
            }
            TermsEnum termsEnum = terms.iterator();
            Bits liveDocs = reader.getLiveDocs();
            while (true) {
                BytesRef sharedKey = termsEnum.next();
                if (sharedKey == null) {
                    break;
                }
                int numDocs;
                if (liveDocs == null) {
                    numDocs = termsEnum.docFreq();
                } else {
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    numDocs = countFromPostings(postings, liveDocs);
                }
                if (countsByKey.containsKey(sharedKey)) {
                    countsByKey.addTo(sharedKey, numDocs);
                } else {
                    BytesRef key = BytesRef.deepCopyOf(sharedKey);
                    ramAccounting.addBytes(BYTES_REF_SHALLOW_SIZE + sharedKey.length + HASH_MAP_ENTRY_OVERHEAD);
                    countsByKey.put(key, numDocs);
                }
                killToken.raiseIfKilled();;
            }
        }
        if (keyRef.isNullable()) {
            Query existsQuery = keyRef.hasDocValues() || keyRef.indexType() == IndexType.FULLTEXT
                ? new FieldExistsQuery(keyStorageIdent)
                : new ConstantScoreQuery(new TermQuery(new Term(SysColumns.FieldNames.NAME, keyStorageIdent)));
            Query notNull = Queries.not(existsQuery);

            TotalHitCountCollectorManager topHitCounts = new TotalHitCountCollectorManager(searcher.getSlices());
            Integer count = searcher.search(notNull, topHitCounts);
            if (count > 0) {
                ramAccounting.addBytes(HASH_MAP_ENTRY_OVERHEAD);
                countsByKey.put(null, count);
            }
        }
        return countsByKey;
    }

    private static int countFromPostings(PostingsEnum postings, Bits liveDocs) throws IOException {
        int numDocs = 0;
        int doc;
        while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs.get(doc)) {
                numDocs++;
            }
        }
        return numDocs;
    }

    @Nullable
    static BatchIterator<Row> tryOptimizeSingleStringKey(Functions functions,
                                                         LuceneReferenceResolver referenceResolver,
                                                         IndexShard indexShard,
                                                         DocTableInfo table,
                                                         List<String> partitionValues,
                                                         LuceneQueryBuilder luceneQueryBuilder,
                                                         BigArrays bigArrays,
                                                         NodeContext nodeCtx,
                                                         DocInputFactory docInputFactory,
                                                         RoutedCollectPhase collectPhase,
                                                         CollectTask collectTask) {
        GroupProjection groupProjection = getSingleStringKeyGroupProjection(collectPhase.projections());
        if (groupProjection == null) {
            return null;
        }
        assert groupProjection.keys().size() == 1 : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";
        Reference keyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (keyRef == null) {
            return null; // group by on non-reference
        }
        keyRef = DocReferences.docRefToRegularRef(keyRef);
        if (!keyRef.hasDocValues()) {
            return null;
        }
        if (Symbols.hasColumn(collectPhase.toCollect(), SysColumns.SCORE)
            || collectPhase.where().hasColumn(SysColumns.SCORE)) {
            // We could optimize this, but since it's assumed to be an uncommon case we fallback to generic group-by
            // to keep the optimized implementation a bit simpler
            return null;
        }
        if (hasHighCardinalityRatio(() -> indexShard.acquireSearcher("group-by-cardinality-check"), keyRef.storageIdent())) {
            return null;
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = sharedShardContext.acquireSearcher("group-by-ordinals:" + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);

        IndexService indexService = sharedShardContext.indexService();
        Version shardCreatedVersion = indexShard.getVersionCreated();
        RamAccounting ramAccounting = collectTask.getRamAccounting();

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

        // Aggregations are executed on shard level, so ITER_FINAL would require finishing the aggregation
        // here, which DocValueAggregator can't do (it always yields a partial result). If the mode isn't
        // ITER_FINAL, try to combine the 2-phase ordinal key optimization with doc-value based aggregation,
        // same as DocValuesGroupByOptimizedIterator does for the generic single/many key case.
        if (groupProjection.mode() != AggregateMode.ITER_FINAL) {
            List<DocValueAggregator> docValueAggregators = DocValuesAggregates.createAggregators(
                functions,
                referenceResolver,
                groupProjection.values(),
                collectPhase.toCollect(),
                table,
                shardCreatedVersion
            );
            if (docValueAggregators != null) {
                return getIteratorWithDocValueAggregators(
                    searcher.item(),
                    keyRef.storageIdent(),
                    docValueAggregators,
                    ramAccounting,
                    collectTask.memoryManager(),
                    collectTask.minNodeVersion(),
                    queryContext.query()
                );
            }
        }

        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx(collectTask.txnCtx());
        docCtx.add(collectPhase.toCollect().stream()::iterator);

        InputFactory inputFactory = new InputFactory(nodeCtx);
        InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations(collectTask.txnCtx());
        ctxForAggregations.add(groupProjection.values());
        final List<CollectExpression<Row, ?>> aggExpressions = ctxForAggregations.expressions();

        List<AggregationContext> aggregations = ctxForAggregations.aggregations();
        List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();

        CollectorContext collectorContext
            = new CollectorContext(sharedShardContext.readerId(), () -> StoredRowLookup.create(shardCreatedVersion, table, partitionValues));
        InputRow inputRow = new InputRow(docCtx.topLevelInputs());

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

    /// Combines the 2-phase ordinal-value lookup (see {@link #applyAggregatesGroupedByKey}) with the
    /// doc-value-aggregators optimization (see {@link DocValuesGroupByOptimizedIterator}): the group key is
    /// still resolved from Lucene ordinals in 2 phases (cheap long-keyed grouping first, resolving to the
    /// actual term value only once per distinct term per segment), but the aggregation values are computed
    /// directly from doc values via {@link DocValueAggregator}.
    @SuppressWarnings("rawtypes")
    private static BatchIterator<Row> getIteratorWithDocValueAggregators(IndexSearcher indexSearcher,
                                                                         String keyColumnName,
                                                                         List<DocValueAggregator> aggregators,
                                                                         RamAccounting ramAccounting,
                                                                         MemoryManager memoryManager,
                                                                         Version minNodeVersion,
                                                                         Query query) {
        Killable.Token killToken = new Token();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> getRowsFromDocValueAggregatorStates(
                applyDocValueAggregatorsGroupedByKey(
                    indexSearcher,
                    keyColumnName,
                    aggregators,
                    ramAccounting,
                    memoryManager,
                    minNodeVersion,
                    query,
                    killToken
                ),
                aggregators,
                ramAccounting
            ),
            true
        );
    }

    @SuppressWarnings("rawtypes")
    private static Iterable<Row> getRowsFromDocValueAggregatorStates(Map<BytesRef, Object[]> groupedStates,
                                                                     List<DocValueAggregator> aggregators,
                                                                     RamAccounting ramAccounting) {
        return () -> groupedStates.entrySet().stream()
            .map(new Function<Map.Entry<BytesRef, Object[]>, Row>() {

                final Object[] cells = new Object[1 + aggregators.size()];
                final RowN row = new RowN(cells);

                @SuppressWarnings("unchecked")
                @Override
                public Row apply(Map.Entry<BytesRef, Object[]> entry) {
                    cells[0] = BytesRefs.toString(entry.getKey());
                    Object[] states = entry.getValue();
                    for (int i = 0, c = 1; i < states.length; i++, c++) {
                        cells[c] = aggregators.get(i).partialResult(ramAccounting, states[i]);
                    }
                    return row;
                }
            })
            .iterator();
    }

    /// Groups docs matching `query` by the ordinal of `keyColumnName`'s `SortedSetDocValues` (phase 1) and
    /// aggregates them using `aggregators`. The ordinal is only resolved to the actual term value (phase 2)
    /// the first time it is encountered within a segment; the resulting state is then cached (by ordinal) for
    /// the remainder of that segment and shared with the global, cross-segment map keyed by the resolved term,
    /// so that further docs matching the same term - whether in this segment or another one - keep mutating the
    /// same aggregation state directly, without a separate merge/reduce step.
    @SuppressWarnings("rawtypes")
    private static Map<BytesRef, Object[]> applyDocValueAggregatorsGroupedByKey(IndexSearcher indexSearcher,
                                                                                String keyColumnName,
                                                                                List<DocValueAggregator> aggregators,
                                                                                RamAccounting ramAccounting,
                                                                                MemoryManager memoryManager,
                                                                                Version minNodeVersion,
                                                                                Query query,
                                                                                Token killToken) throws IOException {
        final HashMap<BytesRef, Object[]> statesByKey = new HashMap<>();
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        Object[] nullStates = null;

        LongObjectHashMap<Object[]> stateByOrdInLeaf = new LongObjectHashMap<>();
        for (LeafReaderContext leaf : leaves) {
            killToken.raiseIfKilled();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0, aggregatorsSize = aggregators.size(); i < aggregatorsSize; i++) {
                aggregators.get(i).loadDocValues(leaf);
            }
            SortedSetDocValues values = DocValues.getSortedSet(leaf.reader(), keyColumnName);
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            stateByOrdInLeaf.clear();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                killToken.raiseIfKilled();
                if (docDeleted(liveDocs, doc)) {
                    continue;
                }
                if (values.advanceExact(doc)) {
                    long ord = values.nextOrd();
                    Object[] states = stateByOrdInLeaf.get(ord);
                    if (states == null) {
                        BytesRef sharedKey = values.lookupOrd(ord);
                        BytesRef key = BytesRef.deepCopyOf(sharedKey);
                        states = statesByKey.get(key);
                        if (states == null) {
                            states = initDocValueAggregatorStates(aggregators, ramAccounting, memoryManager, minNodeVersion, doc);
                            ramAccounting.addBytes(BYTES_REF_SHALLOW_SIZE + key.length + HASH_MAP_ENTRY_OVERHEAD);
                            statesByKey.put(key, states);
                        } else {
                            applyDocValueAggregators(aggregators, ramAccounting, doc, states);
                        }
                        stateByOrdInLeaf.put(ord, states);
                    } else {
                        applyDocValueAggregators(aggregators, ramAccounting, doc, states);
                    }
                    if (values.docValueCount() > 1) {
                        throw new ArrayViaDocValuesUnsupportedException(keyColumnName);
                    }
                } else {
                    if (nullStates == null) {
                        nullStates = initDocValueAggregatorStates(aggregators, ramAccounting, memoryManager, minNodeVersion, doc);
                    } else {
                        applyDocValueAggregators(aggregators, ramAccounting, doc, nullStates);
                    }
                }
            }
        }
        if (nullStates != null) {
            statesByKey.put(null, nullStates);
        }
        return statesByKey;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object[] initDocValueAggregatorStates(List<DocValueAggregator> aggregators,
                                                         RamAccounting ramAccounting,
                                                         MemoryManager memoryManager,
                                                         Version minNodeVersion,
                                                         int doc) throws IOException {
        Object[] states = new Object[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            var aggregator = aggregators.get(i);
            Object state = aggregator.initialState(ramAccounting, memoryManager, minNodeVersion);
            state = aggregator.apply(ramAccounting, doc, state);
            states[i] = state;
        }
        return states;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void applyDocValueAggregators(List<DocValueAggregator> aggregators,
                                                 RamAccounting ramAccounting,
                                                 int doc,
                                                 Object[] states) throws IOException {
        for (int i = 0; i < aggregators.size(); i++) {
            states[i] = aggregators.get(i).apply(ramAccounting, doc, states[i]);
        }
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

        Killable.Token killToken = new Token();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> getRows(
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
                        killToken
                    ),
                ramAccounting,
                aggregations,
                aggregateMode
            ),
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
                                                                       Token killToken) throws IOException {
        final HashMap<BytesRef, Object[]> statesByKey = new HashMap<>();
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        Object[] nullStates = null;

        LongObjectHashMap<Object[]> statesByOrd = new LongObjectHashMap<>();
        for (LeafReaderContext leaf: leaves) {
            killToken.raiseIfKilled();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            var readerContext = new ReaderContext(leaf);
            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).setNextReader(readerContext);
            }
            SortedSetDocValues values = DocValues.getSortedSet(leaf.reader(), keyColumnName);
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                killToken.raiseIfKilled();
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
                        statesByOrd.put(ord, initStates(aggregations, ramAccounting, memoryManager, minNodeVersion));
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
            for (var entry : statesByOrd.entries()) {
                killToken.raiseIfKilled();
                long ord = entry.key();
                Object[] states = entry.value();
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
            statesByOrd.clear();
        }
        if (nullStates != null) {
            statesByKey.put(null, nullStates);
        }
        return statesByKey;
    }

    static boolean hasTerms(Supplier<Engine.Searcher> acquireSearcher, String fieldName) {
        try (var searcher = acquireSearcher.get()) {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                Terms terms = leaf.reader().terms(fieldName);
                if (terms == null) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
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

    @SuppressWarnings("unchecked")
    private static void aggregateValues(List<AggregationContext> aggregations,
                                        RamAccounting ramAccounting,
                                        MemoryManager memoryManager,
                                        Object[] states) {
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);

            if (InputCondition.matches(aggregation.filter())) {
                states[i] = aggregation.function().iterate(
                    ramAccounting,
                    memoryManager,
                    states[i],
                    aggregation.inputs());
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object[] initStates(List<AggregationContext> aggregations,
                                       RamAccounting ramAccounting,
                                       MemoryManager memoryManager,
                                       Version minNodeVersion) {
        Object[] states = new Object[aggregations.size()];
        for (int i = 0; i < aggregations.size(); i++) {
            AggregationContext aggregation = aggregations.get(i);
            AggregationFunction function = aggregation.function();

            var newState = function.newState(ramAccounting, minNodeVersion, memoryManager);
            if (InputCondition.matches(aggregation.filter())) {
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

    private static GroupProjection getSingleStringKeyGroupProjection(Collection<? extends Projection> projections) {
        GroupProjection groupProjection = null;
        int shardProjections = 0;
        for (var projection : projections) {
            if (projection.requiredGranularity() == RowGranularity.SHARD) {
                shardProjections++;
                if (projection instanceof GroupProjection gProjection) {
                    groupProjection = gProjection;
                }
            }
        }
        if (shardProjections != 1 || groupProjection == null) {
            return null;
        }
        if (groupProjection.keys().size() != 1 || groupProjection.keys().get(0).valueType().id() != DataTypes.STRING.id()) {
            return null;
        }
        return groupProjection;
    }
}
