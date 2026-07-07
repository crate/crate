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
import java.util.HashSet;
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
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
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
     * <p>
     * Q: select count(*) from (select distinct x from t) t
     * <p>
     * cardinality-ratio | mean difference
     * ------------------+-----------------
     *              0.90 |      -5.65%
     *              0.75 |      -5.06%
     *              0.50 |      +1.51%
     *              0.25 |     +38.79%
     * <p>
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

    /// Returns a BatchIterator that reads the term dictionary for keys-only group-by queries like:
    ///
    ///     `SELECT strKey GROUP BY strKey`
    ///
    /// Every term is checked for its live docs, to make sure it doesn't belong to a deleted document.
    ///
    ///  Only works if the group-by has no aggregates and there is no WHERE clause.
    /// Returns null for other queries.
    @Nullable
    static BatchIterator<Row> tryUseTermsForDistinctKeys(IndexShard indexShard,
                                                         RoutedCollectPhase collectPhase,
                                                         CollectTask collectTask) {
        GroupProjection groupProjection = getSingleStringKeyGroupProjection(collectPhase.projections());
        if (groupProjection == null) {
            return null;
        }
        assert groupProjection.keys().size() == 1
            : "Must have 1 key if getSingleStringKeyGroupProjection returned a projection";

        if (!groupProjection.values().isEmpty()) {
            return null; // keys-only; group-by with aggregates not handled
        }

        Reference docKeyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().get(0));
        if (docKeyRef == null) {
            return null; // group by on non-reference
        }

        Symbol where = collectPhase.where();
        if (!where.equals(Literal.BOOLEAN_TRUE)) {
            // The terms dictionary cannot enumerate terms with a filter.
            return null;
        }

        final Reference keyRef = DocReferences.docRefToRegularRef(docKeyRef);
        if (!hasTerms(() -> indexShard.acquireSearcher("terms-check"), keyRef.storageIdent())) {
            return null;
        }

        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(indexShard.shardId());
        var searcherRef = sharedShardContext.acquireSearcher("group-by-distinct-keys:" + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcherRef);
        IndexSearcher searcher = searcherRef.item();

        Token killToken = new Killable.Token();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> keysToRows(getDistinctKeys(collectTask.getRamAccounting(), keyRef, searcher, killToken)),
            true
        );
    }

    private static Iterable<Row> keysToRows(Collection<BytesRef> keys) {
        final Object[] cells = new Object[1];
        final Row row = new RowN(cells);
        return () -> keys.stream()
            .map(key -> {
                cells[0] = key == null ? null : key.utf8ToString();
                return row;
            })
            .iterator();
    }

    /// Collects the distinct live values of `keyRef` by iterating the term dictionary of each segment
    /// instead of scanning documents. A NULL group is added when the (nullable) column has null docs,
    /// matching the semantics of a keys-only GROUP BY.
    static Collection<BytesRef> getDistinctKeys(RamAccounting ramAccounting,
                                                Reference keyRef,
                                                IndexSearcher searcher,
                                                Token killToken) throws IOException {
        HashSet<BytesRef> keys = new HashSet<>();
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
            BytesRef sharedKey;
            while ((sharedKey = termsEnum.next()) != null) {
                killToken.raiseIfKilled();
                if (keys.contains(sharedKey)) {
                    continue;
                }
                if (liveDocs != null) {
                    // Terms of deleted documents stay in the dictionary until segments merge,
                    // hence we need to check for terms without live docs.
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    if (!hasLiveDoc(postings, liveDocs)) {
                        continue;
                    }
                }
                ramAccounting.addBytes(BYTES_REF_SHALLOW_SIZE + sharedKey.length + HASH_MAP_ENTRY_OVERHEAD);
                keys.add(BytesRef.deepCopyOf(sharedKey));
            }
        }
        if (keyRef.isNullable() && countNullValues(keyRef, searcher) > 0) {
            ramAccounting.addBytes(HASH_MAP_ENTRY_OVERHEAD);
            keys.add(null);
        }
        return keys;
    }

    private static boolean hasLiveDoc(PostingsEnum postings, Bits liveDocs) throws IOException {
        int doc;
        while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs.get(doc)) {
                return true;
            }
        }
        return false;
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
            int count = countNullValues(keyRef, searcher);
            if (count > 0) {
                ramAccounting.addBytes(HASH_MAP_ENTRY_OVERHEAD);
                countsByKey.put(null, count);
            }
        }
        return countsByKey;
    }

    private static int countNullValues(Reference keyRef, IndexSearcher searcher) throws IOException {
        String keyStorageIdent = keyRef.storageIdent();
        Query existsQuery = keyRef.hasDocValues() || keyRef.indexType() == IndexType.FULLTEXT
            ? new FieldExistsQuery(keyStorageIdent)
            : new ConstantScoreQuery(new TermQuery(new Term(SysColumns.FieldNames.NAME, keyStorageIdent)));
        Query notNull = Queries.not(existsQuery);

        TotalHitCountCollectorManager topHitCounts = new TotalHitCountCollectorManager(searcher.getSlices());
        return searcher.search(notNull, topHitCounts);
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
    static BatchIterator<Row> tryOptimizeSingleStringKey(IndexShard indexShard,
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

        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx = docInputFactory.getCtx(collectTask.txnCtx());
        docCtx.add(collectPhase.toCollect().stream()::iterator);

        InputFactory inputFactory = new InputFactory(nodeCtx);
        InputFactory.Context<CollectExpression<Row, ?>> ctxForAggregations = inputFactory.ctxForAggregations(collectTask.txnCtx());
        ctxForAggregations.add(groupProjection.values());
        final List<CollectExpression<Row, ?>> aggExpressions = ctxForAggregations.expressions();

        List<AggregationContext> aggregations = ctxForAggregations.aggregations();
        List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();

        RamAccounting ramAccounting = collectTask.getRamAccounting();

        Version shardCreatedVersion = indexShard.getVersionCreated();
        CollectorContext collectorContext
            = new CollectorContext(sharedShardContext.readerId(), () -> StoredRowLookup.create(shardCreatedVersion, table, partitionValues));
        InputRow inputRow = new InputRow(docCtx.topLevelInputs());

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
