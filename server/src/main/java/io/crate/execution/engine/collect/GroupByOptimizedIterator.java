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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.jspecify.annotations.Nullable;

import io.crate.common.MutableLong;
import io.crate.common.TriConsumer;
import io.crate.common.collections.Lists;
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
import io.crate.execution.engine.aggregation.GroupByMaps;
import io.crate.execution.engine.aggregation.ResizeAwareMap;
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
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.DataTypes;
import io.netty.util.collection.LongObjectHashMap;

final class GroupByOptimizedIterator {

    /// Stands in for the Lucene ordinal of a key column in docs that have no value for it.
    /// Lucene ordinals are always `>= 0`, so this cannot collide with a real one.
    private static final long NULL_ORD = -1L;

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

    /// Returns a BatchIterator that reads the term dictionary of a single string key column instead
    /// of scanning documents, for group-by queries without a WHERE clause:
    ///
    ///     `SELECT strKey GROUP BY strKey`            (keys only)
    ///     `SELECT strKey, count(*) GROUP BY strKey`  (with an unfiltered count(*))
    ///
    /// Terms of deleted documents linger in the dictionary until segments merge, so each term is
    /// checked against live docs. A NULL group is added for nullable columns that have null docs.
    /// Returns null for queries that don't match this shape.
    @Nullable
    static BatchIterator<Row> tryUseTermDictionary(IndexShard indexShard,
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
        boolean keysOnly = values.isEmpty();
        if (!keysOnly) {
            // The only aggregate we can serve straight from the term dictionary is an unfiltered count(*).
            if (values.size() != 1 || !values.getFirst().signature().equals(CountAggregation.COUNT_STAR_SIGNATURE)) {
                return null;
            }
            Symbol aggregateFilter = values.getFirst().filter();
            if (!aggregateFilter.equals(Literal.BOOLEAN_TRUE)) {
                return null;
            }
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
        RamAccounting ramAccounting = collectTask.getRamAccounting();
        if (keysOnly) {
            return CollectingBatchIterator.newInstance(
                killToken,
                () -> keysToRows(getCountsByKey(ramAccounting, keyRef, searcher, killToken)),
                true
            );
        }
        AggregateMode mode = groupProjection.mode();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> countsToRows(getCountsByKey(ramAccounting, keyRef, searcher, killToken), mode),
            true
        );
    }

    private static Iterable<Row> keysToRows(Map<String, Long> countsByKey) {
        final Object[] cells = new Object[1];
        final Row row = new RowN(cells);
        return () -> countsByKey.keySet().stream()
            .map(key -> {
                cells[0] = key;
                return row;
            })
            .iterator();
    }

    /// Returns a batchIterator that reads the BKD points index instead of scanning documents, for:
    ///
    ///     `SELECT numKey FROM t GROUP BY numKey`
    ///
    /// Only works for a keys-only GROUP BY on a single numeric column and without a WHERE clause.
    /// Returns null for other queries.
    @Nullable
    static BatchIterator<Row> tryUseLooseIndexScan(IndexShard indexShard,
                                                   RoutedCollectPhase collectPhase,
                                                   CollectTask collectTask) {
        GroupProjection groupProjection = singleKeyGroupProjection(collectPhase.projections());
        if (groupProjection == null) {
            return null;
        }
        if (!groupProjection.values().isEmpty()) {
            // There are three cases:
            //   1. count(*),                            e.g. SELECT x, count(*) FROM tbl GROUP BY x
            //   2. aggregates over the key column,       e.g. SELECT x, avg(x) FROM tbl GROUP BY x
            //   3. aggregates over a different column,   e.g. SELECT x, avg(y) FROM tbl GROUP BY x
            //
            // (1) and (2) can be implemented with just the PointTree, but currently aren't.
            //
            // (3) requires doc values, and they cannot be driven from PointValues, because
            // PointValues is ordered by value: advanceExact requires a doc id >= the current one
            // (see DocValuesIterator), while a value-ordered traversal hands out doc ids in
            // arbitrary order.
            return null;
        }
        if (!collectPhase.where().equals(Literal.BOOLEAN_TRUE)) {
            // In theory a WHERE clause that uses only this key column could be pushed down to the
            // PointTree, but that's currently not implemented.
            // Any other WHERE clause, using other columns, cannot work at all.
            return null;
        }

        Reference docKeyRef = getKeyRef(collectPhase.toCollect(), groupProjection.keys().getFirst());
        if (docKeyRef == null) {
            return null; // group by on a non-reference
        }
        final Reference keyRef = DocReferences.docRefToRegularRef(docKeyRef);
        if (!LooseIndexScan.supportsType(keyRef.valueType())) {
            return null; // not a type that CrateDB indexes as a single point dimension
        }
        if (keyRef.indexType() == IndexType.NONE) {
            return null; // INDEX OFF, no points written
        }

        int bytesPerValue = pointWidth(
            () -> indexShard.acquireSearcher("loose-index-scan:" + formatSource(collectPhase)), keyRef.storageIdent());
        if (bytesPerValue < 0) {
            return null;
        }

        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcherRef = sharedShardContext.acquireSearcher("loose-index-scan:" + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcherRef);

        return LooseIndexScan.iterator(
            searcherRef.item(),
            keyRef,
            bytesPerValue,
            collectTask.getRamAccounting(),
            collectTask.killToken()
        );
    }

    /**
     * The point width of `fieldName`, or -1 if the loose index scan cannot be used because the
     * column has no points at all, or is indexed with more than one dimension.
     */
    static int pointWidth(Supplier<Engine.Searcher> acquireSearcher, String fieldName) {
        // Acquires a separate searcher for the same reason as hasHighCardinalityRatio: going through
        // sharedShardContexts() and then bailing out causes issues in the fallback logic later on.
        try (var searcher = acquireSearcher.get()) {
            // Enough to inspect only the first segment that contains the field since Lucene makes sure all segments
            // use the same dimension.
            // A segment can lack the field entirely if the column was added after it was written, or if every
            // doc in it has a NULL value.
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(fieldName);
                if (fieldInfo == null) {
                    continue;
                }
                assert fieldInfo.getPointIndexDimensionCount() > 0
                    : "pointWidth() called on a field that has no points (not indexed)";
                return fieldInfo.getPointIndexDimensionCount() == 1
                    ? fieldInfo.getPointNumBytes()
                    : -1;
            }
            return -1;
        }
    }

    private static Iterable<Row> countsToRows(Map<String, Long> countsByKey, AggregateMode mode) {
        final Object[] cells = new Object[2];
        final Row row = new RowN(cells);
        LongFunction<Object> wrapCount = switch (mode) {
            case ITER_PARTIAL -> MutableLong::new;
            case ITER_FINAL -> x -> x;
            case PARTIAL_FINAL -> throw new UnsupportedOperationException(
                "Shard level projection cannot start at PARTIAL");
        };
        return () -> countsByKey.entrySet().stream()
            .map(entry -> {
                // See GroupProjection.outputs(): keys always come first, aggregations second
                cells[0] = entry.getKey();
                cells[1] = wrapCount.apply(entry.getValue());
                return row;
            })
            .iterator();
    }

    private static Map<String, Long> getCountsByKey(RamAccounting ramAccounting,
                                                    Reference keyRef,
                                                    IndexSearcher searcher,
                                                    Token killToken) throws IOException {
        // Using `String` keys instead of `BytesRef`
        // BytesRef would allow to avoid utf8->utf16 conversations for values that already exist in the map
        // but for values that do not exist in the map, BytesRef needs deepCopy + utf8->utf16.
        //
        // Benchmarks showed that this has significant cost (~35%).
        // Given that high cardinality cases are the more expensive ones, we optimize for them instead of the low cardinality cases.
        ResizeAwareMap<String, Long> countsByKey = GroupByMaps.wrapperForJDKMap(new HashMap<>());
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

                if (numDocs != 0) {
                    String keyStr = sharedKey.utf8ToString();
                    countsByKey.compute(keyStr, (k, count) -> {
                        if (count == null) {
                            ramAccounting.addBytes(
                                RamUsageEstimator.sizeOf(keyStr)
                                    + HASH_MAP_ENTRY_OVERHEAD
                                    + countsByKey.expectedCapacityIncreaseBytes());
                            return (long) numDocs;
                        }
                        return count + numDocs;
                    });
                }

                killToken.raiseIfKilled();
            }
        }

        if (keyRef.isNullable()) {
            int count = countNullValues(keyRef, searcher);
            if (count > 0) {
                ramAccounting.addBytes(HASH_MAP_ENTRY_OVERHEAD);
                countsByKey.put(null, Long.valueOf(count));
            }
        }

        return countsByKey;
    }

    public static int countNullValues(Reference keyRef, IndexSearcher searcher) throws IOException {
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

    /**
     * Returns a BatchIterator that groups by the Lucene ordinals of the group keys instead of
     * by their values, for group-by queries where every key is a string column with doc values:
     * <code>SELECT tag, country, count(*) FROM tbl GROUP BY tag, country</code>
     * <p>
     * The aggregation values are computed straight from doc values if every aggregation has a
     * {@link DocValueAggregator}, and via the generic aggregation machinery otherwise. Both flavours
     * share the per-segment ordinal structure documented on {@link #applyDocValueAggregatorsGroupedByKeys}.
     */
    @SuppressWarnings("rawtypes")
    @Nullable
    static BatchIterator<Row> tryOptimizeStringKeys(Functions functions,
                                                    LuceneReferenceResolver referenceResolver,
                                                    IndexShard indexShard,
                                                    DocTableInfo table,
                                                    List<String> partitionValues,
                                                    LuceneQueryBuilder luceneQueryBuilder,
                                                    NodeContext nodeCtx,
                                                    DocInputFactory docInputFactory,
                                                    RoutedCollectPhase collectPhase,
                                                    CollectTask collectTask) {
        GroupProjection groupProjection = shardGroupProjection(collectPhase.projections());
        if (groupProjection == null) {
            return null;
        }
        List<Reference> keyRefs = new ArrayList<>(groupProjection.keys().size());
        for (Symbol key : groupProjection.keys()) {
            Reference docKeyRef = getKeyRef(collectPhase.toCollect(), key);
            if (docKeyRef == null) {
                return null; // group by on non-reference
            }
            Reference keyRef = DocReferences.docRefToRegularRef(docKeyRef);
            if (keyRef.valueType().id() != DataTypes.STRING.id() || !keyRef.hasDocValues()) {
                return null;
            }
            keyRefs.add(keyRef);
        }
        if (Symbols.hasColumn(collectPhase.toCollect(), SysColumns.SCORE)
            || collectPhase.where().hasColumn(SysColumns.SCORE)) {
            // We could optimize this, but since it's assumed to be an uncommon case we fallback to generic group-by
            // to keep the optimized implementation a bit simpler
            return null;
        }
        List<String> keyColumns = Lists.map(keyRefs, Reference::storageIdent);
        if (hasHighCardinalityRatio(() -> indexShard.acquireSearcher("group-by-cardinality-check"), keyColumns)) {
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

        TriConsumer<ResizeAwareMap<List<Object>, Object[]>, List<Object>, Object[]> accountForNewEntry =
            GroupByMaps.accountForNewEntry(ramAccounting, Lists.map(keyRefs, Reference::valueType));

        // Combine the 2-phase ordinal key optimization with doc-value based aggregation, same as
        // DocValuesGroupByOptimizedIterator does for the generic single/many key case. A DocValueAggregator
        // always yields a partial result; for ITER_FINAL the partial results are finished here via
        // AggregateMode#finishCollect (terminatePartial), same as the generic group-by path in getRows().
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
                keyColumns,
                docValueAggregators,
                aggregationFunctions(functions, groupProjection.values()),
                groupProjection.mode(),
                ramAccounting,
                accountForNewEntry,
                collectTask.memoryManager(),
                collectTask.minNodeVersion(),
                queryContext.query()
            );
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
            searcher.item(),
            keyColumns,
            aggregations,
            expressions,
            aggExpressions,
            ramAccounting,
            accountForNewEntry,
            collectTask.memoryManager(),
            collectTask.minNodeVersion(),
            inputRow,
            queryContext.query(),
            collectorContext,
            groupProjection.mode());
    }

    /// Combines the 2-phase ordinal-value lookup (see {@link #applyDocValueAggregatorsGroupedByKeys})
    /// with the doc-value-aggregators optimization (see {@link DocValuesGroupByOptimizedIterator}): the
    /// group keys are still resolved from Lucene ordinals in 2 phases (cheap long-keyed grouping first,
    /// resolving to the actual term values only once per distinct group per segment), but the aggregation
    /// values are computed directly from doc values via {@link DocValueAggregator}.
    @SuppressWarnings("rawtypes")
    private static BatchIterator<Row> getIteratorWithDocValueAggregators(
            IndexSearcher indexSearcher,
            List<String> keyColumns,
            List<DocValueAggregator> aggregators,
            List<AggregationFunction> aggregationFunctions,
            AggregateMode mode,
            RamAccounting ramAccounting,
            TriConsumer<ResizeAwareMap<List<Object>, Object[]>, List<Object>, Object[]> accountForNewEntry,
            MemoryManager memoryManager,
            Version minNodeVersion,
            Query query) {

        Killable.Token killToken = new Token();
        return CollectingBatchIterator.newInstance(
            killToken,
            () -> getRowsFromDocValueAggregatorStates(
                applyDocValueAggregatorsGroupedByKeys(
                    indexSearcher,
                    keyColumns,
                    aggregators,
                    ramAccounting,
                    accountForNewEntry,
                    memoryManager,
                    minNodeVersion,
                    query,
                    killToken
                ),
                keyColumns.size(),
                aggregators,
                aggregationFunctions,
                mode,
                ramAccounting
            ),
            true
        );
    }

    @SuppressWarnings("rawtypes")
    private static List<AggregationFunction> aggregationFunctions(Functions functions, List<Aggregation> aggregations) {
        List<AggregationFunction> aggregationFunctions = new ArrayList<>(aggregations.size());
        for (int i = 0; i < aggregations.size(); i++) {
            aggregationFunctions.add((AggregationFunction<?, ?>) functions.getQualified(aggregations.get(i)));
        }
        return aggregationFunctions;
    }

    @SuppressWarnings("rawtypes")
    private static Iterable<Row> getRowsFromDocValueAggregatorStates(Map<List<Object>, Object[]> groupedStates,
                                                                     int numKeys,
                                                                     List<DocValueAggregator> aggregators,
                                                                     List<AggregationFunction> aggregationFunctions,
                                                                     AggregateMode mode,
                                                                     RamAccounting ramAccounting) {
        return () -> groupedStates.entrySet().stream()
            .map(new Function<Map.Entry<List<Object>, Object[]>, Row>() {

                final Object[] cells = new Object[numKeys + aggregators.size()];
                final RowN row = new RowN(cells);

                @SuppressWarnings("unchecked")
                @Override
                public Row apply(Map.Entry<List<Object>, Object[]> entry) {
                    // See GroupProjection.outputs(): keys always come first, aggregations second
                    List<Object> key = entry.getKey();
                    for (int i = 0; i < numKeys; i++) {
                        cells[i] = key.get(i);
                    }
                    Object[] states = entry.getValue();
                    for (int i = 0, c = numKeys; i < states.length; i++, c++) {
                        // A DocValueAggregator always yields a partial result; finishCollect returns it
                        // as-is for ITER_PARTIAL and finishes it (terminatePartial) for ITER_FINAL.
                        Object partial = aggregators.get(i).partialResult(ramAccounting, states[i]);
                        cells[c] = mode.finishCollect(ramAccounting, aggregationFunctions.get(i), partial);
                    }
                    return row;
                }
            })
            .iterator();
    }

    /**
     * Groups the docs matching {@code query} by the Lucene ordinals of the {@code keyColumns}
     * (phase 1) and aggregates them using {@code aggregators}.
     * <p>
     * The returned map is keyed by the resolved key values - one element per key column in the list,
     * in the same order as the key cols, e.g. {@code ["foo", "Austria"]} - and spans all segments.
     * <p>
     * Within a segment the same groups are additionally held in a structure nested one level per key
     * column, where each level is keyed by the ordinal of its key column in that segment:
     * {@code ord(key0) -> ord(key1) -> ... -> states}. The per-segment key is therefore the whole
     * tuple of ordinals, spread over the nested levels, therfore, routing a doc to its group
     * uses longs and performs one lookup per key column until you "descend" to the `states`.
     * <p>
     * The ordinals of a group are resolved to their term values (phase 2, see {@link #resolveKey})
     * only the first time the group is reached within a segment. Ordinals are segment-local, therefore
     * the returned map is keyed by the values and not by the ordinals.
     * <p>
     * The states array ({@code Object[]}) is stored both in the nested structure ({@code statesByOrdsInLeaf}
     * and in the returned map, so that further docs of the same group - whether in this segment or another one -
     * can keep mutating the same aggregation state directly, without a separate merge/reduce step.
     */
    @SuppressWarnings("rawtypes")
    private static Map<List<Object>, Object[]> applyDocValueAggregatorsGroupedByKeys(
            IndexSearcher indexSearcher,
            List<String> keyColumns,
            List<DocValueAggregator> aggregators,
            RamAccounting ramAccounting,
            TriConsumer<ResizeAwareMap<List<Object>, Object[]>, List<Object>, Object[]> accountForNewEntry,
            MemoryManager memoryManager,
            Version minNodeVersion,
            Query query,
            Token killToken) throws IOException {
        final ResizeAwareMap<List<Object>, Object[]> statesByKey = GroupByMaps.wrapperForJDKMap(new HashMap<>());
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        final int numKeys = keyColumns.size();
        final long[] ords = new long[numKeys];
        final SortedSetDocValues[] keyValues = new SortedSetDocValues[numKeys];

        for (LeafReaderContext leaf : leaves) {
            killToken.raiseIfKilled();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0, aggregatorsSize = aggregators.size(); i < aggregatorsSize; i++) {
                aggregators.get(i).loadDocValues(leaf);
            }
            for (int i = 0; i < numKeys; i++) {
                keyValues[i] = DocValues.getSortedSet(leaf.reader(), keyColumns.get(i));
            }
            LongObjectHashMap<Object> statesByOrdsInLeaf = new LongObjectHashMap<>();
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                killToken.raiseIfKilled();
                if (docDeleted(liveDocs, doc)) {
                    continue;
                }
                readOrds(keyColumns, keyValues, doc, ords);
                LongObjectHashMap<Object> statesByLastOrd = getNested(statesByOrdsInLeaf, ords);
                long lastOrd = ords[numKeys - 1];
                Object[] states = (Object[]) statesByLastOrd.get(lastOrd);
                if (states == null) {
                    List<Object> key = resolveKey(keyValues, ords);
                    states = statesByKey.get(key);
                    if (states == null) {
                        states = initDocValueAggregatorStates(aggregators, ramAccounting, memoryManager, minNodeVersion, doc);
                        accountForNewEntry.accept(statesByKey, key, states);
                        statesByKey.put(key, states);
                    } else {
                        applyDocValueAggregators(aggregators, ramAccounting, doc, states);
                    }
                    statesByLastOrd.put(lastOrd, states);
                } else {
                    applyDocValueAggregators(aggregators, ramAccounting, doc, states);
                }
            }
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

    static BatchIterator<Row> getIterator(IndexSearcher indexSearcher,
                                          List<String> keyColumns,
                                          List<AggregationContext> aggregations,
                                          List<? extends LuceneCollectorExpression<?>> expressions,
                                          List<CollectExpression<Row, ?>> aggExpressions,
                                          RamAccounting ramAccounting,
                                          TriConsumer<ResizeAwareMap<List<Object>, Object[]>, List<Object>, Object[]> accountForNewEntry,
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
                    applyAggregatesGroupedByKeys(
                        indexSearcher,
                        keyColumns,
                        aggregations,
                        expressions,
                        aggExpressions,
                        ramAccounting,
                        accountForNewEntry,
                        memoryManager,
                        minNodeVersion,
                        inputRow,
                        query,
                        killToken
                    ),
                keyColumns.size(),
                ramAccounting,
                aggregations,
                aggregateMode
            ),
            true
        );
    }

    private static Iterable<Row> getRows(Map<List<Object>, Object[]> groupedStates,
                                         int numKeys,
                                         RamAccounting ramAccounting,
                                         List<AggregationContext> aggregations,
                                         AggregateMode mode) {
        return () -> groupedStates.entrySet().stream()
            .map(new Function<Map.Entry<List<Object>, Object[]>, Row>() {

                final Object[] cells = new Object[numKeys + aggregations.size()];
                final RowN row = new RowN(cells);

                @Override
                public Row apply(Map.Entry<List<Object>, Object[]> entry) {
                    // See GroupProjection.outputs(): keys always come first, aggregations second
                    List<Object> key = entry.getKey();
                    for (int i = 0; i < numKeys; i++) {
                        cells[i] = key.get(i);
                    }
                    Object[] states = entry.getValue();
                    for (int i = 0, c = numKeys; i < states.length; i++, c++) {
                        //noinspection unchecked
                        cells[c] = mode.finishCollect(ramAccounting, aggregations.get(i).function(), states[i]);
                    }
                    return row;
                }
            })
            .iterator();
    }

    private static Map<List<Object>, Object[]> applyAggregatesGroupedByKeys(
            IndexSearcher indexSearcher,
            List<String> keyColumns,
            List<AggregationContext> aggregations,
            List<? extends LuceneCollectorExpression<?>> expressions,
            List<CollectExpression<Row, ?>> aggExpressions,
            RamAccounting ramAccounting,
            TriConsumer<ResizeAwareMap<List<Object>, Object[]>, List<Object>, Object[]> accountForNewEntry,
            MemoryManager memoryManager,
            Version minNodeVersion,
            InputRow inputRow,
            Query query,
            Token killToken) throws IOException {
        final ResizeAwareMap<List<Object>, Object[]> statesByKey = GroupByMaps.wrapperForJDKMap(new HashMap<>());
        final Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        final int numKeys = keyColumns.size();
        final long[] ords = new long[numKeys];
        final SortedSetDocValues[] keyValues = new SortedSetDocValues[numKeys];

        for (LeafReaderContext leaf : leaves) {
            killToken.raiseIfKilled();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            var readerContext = new ReaderContext(leaf);
            for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
                expressions.get(i).setNextReader(readerContext);
            }
            for (int i = 0; i < numKeys; i++) {
                keyValues[i] = DocValues.getSortedSet(leaf.reader(), keyColumns.get(i));
            }
            LongObjectHashMap<Object> statesByOrdsInLeaf = new LongObjectHashMap<>();
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
                readOrds(keyColumns, keyValues, doc, ords);
                LongObjectHashMap<Object> statesByLastOrd = getNested(statesByOrdsInLeaf, ords);
                long lastOrd = ords[numKeys - 1];
                Object[] states = (Object[]) statesByLastOrd.get(lastOrd);
                if (states == null) {
                    List<Object> key = resolveKey(keyValues, ords);
                    states = statesByKey.get(key);
                    if (states == null) {
                        // initStates already iterates over the current row
                        states = initStates(aggregations, ramAccounting, memoryManager, minNodeVersion);
                        accountForNewEntry.accept(statesByKey, key, states);
                        statesByKey.put(key, states);
                    } else {
                        aggregateValues(aggregations, ramAccounting, memoryManager, states);
                    }
                    statesByLastOrd.put(lastOrd, states);
                } else {
                    aggregateValues(aggregations, ramAccounting, memoryManager, states);
                }
            }
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

    static boolean hasHighCardinalityRatio(Supplier<Engine.Searcher> acquireSearcher, List<String> fieldNames) {
        // acquire separate searcher:
        // Can't use sharedShardContexts() yet, if we bail out the "getOrCreateContext" causes issues later on in the fallback logic
        try (var searcher = acquireSearcher.get()) {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                double distinctCombinations = 1.0;
                for (String fieldName : fieldNames) {
                    Terms terms = leaf.reader().terms(fieldName);
                    if (terms == null) {
                        return true;
                    }
                    long numTerms = terms.size();
                    if (numTerms < 0) {
                        return true; // the codec doesn't store the term count, can't estimate
                    }
                    distinctCombinations *= numTerms;
                }
                double cardinalityRatio = distinctCombinations / leaf.reader().numDocs();
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

    /**
     * The single shard level GroupProjection, with one or more grouping keys of any type,
     * or null if the projections don't have that shape.
     */
    @Nullable
    private static GroupProjection shardGroupProjection(Collection<? extends Projection> projections) {
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
        if (shardProjections != 1 || groupProjection == null || groupProjection.keys().isEmpty()) {
            return null;
        }
        return groupProjection;
    }

    /**
     * The single shard level GroupProjection with exactly one grouping key of any type, or null if
     * the projections don't have that shape.
     */
    @Nullable
    private static GroupProjection singleKeyGroupProjection(Collection<? extends Projection> projections) {
        GroupProjection groupProjection = shardGroupProjection(projections);
        if (groupProjection == null || groupProjection.keys().size() != 1) {
            return null;
        }
        return groupProjection;
    }

    @Nullable
    private static GroupProjection getSingleStringKeyGroupProjection(Collection<? extends Projection> projections) {
        GroupProjection groupProjection = singleKeyGroupProjection(projections);
        if (groupProjection == null
            || groupProjection.keys().getFirst().valueType().id() != DataTypes.STRING.id()) {
            return null;
        }
        return groupProjection;
    }

    private static void readOrds(List<String> keyColumns,
                                 SortedSetDocValues[] keyValues,
                                 int doc,
                                 long[] ords) throws IOException {
        for (int i = 0; i < keyValues.length; i++) {
            SortedSetDocValues values = keyValues[i];
            if (values.advanceExact(doc)) {
                ords[i] = values.nextOrd();
                if (values.docValueCount() > 1) {
                    throw new ArrayViaDocValuesUnsupportedException(keyColumns.get(i));
                }
            } else {
                ords[i] = NULL_ORD;
            }
        }
    }

    /**
     * Traverses the per-segment structure along `ords` down to the level that holds the aggregation
     * states, creating the intermediate levels on the way.
     */
    @SuppressWarnings("unchecked")
    private static LongObjectHashMap<Object> getNested(LongObjectHashMap<Object> root, long[] ords) {
        LongObjectHashMap<Object> level = root;
        for (int i = 0; i < ords.length - 1; i++) {
            LongObjectHashMap<Object> next = (LongObjectHashMap<Object>) level.get(ords[i]);
            if (next == null) {
                next = new LongObjectHashMap<>();
                level.put(ords[i], next);
            }
            level = next;
        }
        return level;
    }

    /**
     * Phase 2 of the ordinal lookup: resolves the ordinals of a group to its key values.
     */
    private static List<Object> resolveKey(SortedSetDocValues[] keyValues, long[] ords) throws IOException {
        ArrayList<Object> key = new ArrayList<>(ords.length);
        for (int i = 0; i < ords.length; i++) {
            key.add(ords[i] == NULL_ORD ? null : keyValues[i].lookupOrd(ords[i]).utf8ToString());
        }
        return key;
    }
}
