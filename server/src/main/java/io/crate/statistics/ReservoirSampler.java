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

package io.crate.statistics;

import static io.crate.data.breaker.BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES;
import static io.crate.statistics.TableStatsService.STATS_SERVICE_THROTTLING_SETTING;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

import io.crate.common.collections.Lists;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.engine.fetch.FetchId;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.metadata.DocReferences;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;
import io.netty.util.collection.IntObjectHashMap;

public final class ReservoirSampler {

    private static final Logger LOGGER = LogManager.getLogger(ReservoirSampler.class);

    private final ClusterService clusterService;
    private final CircuitBreakerService circuitBreakerService;
    private final Schemas schemas;
    private final IndicesService indicesService;

    private final RateLimiter rateLimiter;

    @Inject
    public ReservoirSampler(ClusterService clusterService,
                            CircuitBreakerService circuitBreakerService,
                            NodeContext nodeContext,
                            IndicesService indicesService,
                            Settings settings) {
        this(clusterService,
             circuitBreakerService,
             nodeContext,
             indicesService,
             new RateLimiter.SimpleRateLimiter(STATS_SERVICE_THROTTLING_SETTING.get(settings).getMbFrac())
        );
    }

    @VisibleForTesting
    ReservoirSampler(ClusterService clusterService,
                     CircuitBreakerService circuitBreakerService,
                     NodeContext nodeContext,
                     IndicesService indicesService,
                     RateLimiter rateLimiter) {
        this.clusterService = clusterService;
        this.circuitBreakerService = circuitBreakerService;
        this.schemas = nodeContext.schemas();
        this.indicesService = indicesService;
        this.rateLimiter = rateLimiter;

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_THROTTLING_SETTING, this::updateReadLimit);
    }

    private void updateReadLimit(ByteSizeValue newReadLimit) {
        rateLimiter.setMBPerSec(newReadLimit.getMbFrac()); // mbPerSec is volatile in SimpleRateLimiter, one volatile write
    }

    Samples getSamples(RelationName relationName, List<Reference> columns) {
        TableInfo table;
        try {
            table = schemas.getTableInfo(relationName);
        } catch (RelationUnknown e) {
            return Samples.EMPTY;
        }
        if (!(table instanceof DocTableInfo docTable)) {
            return Samples.EMPTY;
        }
        Random random = Randomness.get();
        Metadata metadata = clusterService.state().metadata();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
        RamAccounting ramAccounting = new BlockBasedRamAccounting(
            b -> breaker.addEstimateBytesAndMaybeBreak(b, "Reservoir-sampling"),
            MAX_BLOCK_SIZE_IN_BYTES);

        try (SketchRamAccounting rla = new SketchRamAccounting(ramAccounting, rateLimiter)) {
            return getSamples(
                columns,
                docTable,
                rla,
                random,
                metadata
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Samples getSamples(List<Reference> columns,
                               DocTableInfo docTable,
                               SketchRamAccounting ramAccounting,
                               Random random,
                               Metadata metadata) throws IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Start collecting samples for table: {}({}),",
                docTable.ident().fqn(),
                columns.stream().map(r -> r.column().fqn()).collect(Collectors.joining(", ")));
        }

        Reservoir fetchIdSamples = new Reservoir(random);
        long totalNumDocs = 0;
        long totalSizeInBytes = 0;

        ArrayList<ColumnExpression<?>> columnExpressions = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columnExpressions.add(new ColumnExpression<>(ramAccounting, columns.get(i)));
        }
        IntObjectHashMap<ShardExpressions> searchersToRelease = new IntObjectHashMap<>();

        try {

            for (String index : docTable.concreteOpenIndices(metadata)) {
                var indexMetadata = metadata.index(index);
                if (indexMetadata == null) {
                    continue;
                }
                var indexService = indicesService.indexService(indexMetadata.getIndex());
                if (indexService == null) {
                    continue;
                }

                List<? extends LuceneCollectorExpression<?>> expressions
                    = getCollectorExpressions(indexService, docTable, columns);

                for (IndexShard indexShard : indexService) {
                    ShardRouting routingEntry = indexShard.routingEntry();
                    if (!routingEntry.primary() || !routingEntry.active()) {
                        continue;
                    }
                    try {
                        Engine.Searcher searcher = indexShard.acquireSearcher("update-table-statistics");
                        searchersToRelease.put(indexShard.shardId().id(), new ShardExpressions(searcher, expressions));
                        totalNumDocs += searcher.getIndexReader().numDocs();
                        totalSizeInBytes += indexShard.storeStats().getSizeInBytes();
                        // We do the sampling in 2 phases. First we get the docIds;
                        // then we retrieve the column values for the sampled docIds.
                        // we do this in 2 phases because the reservoir sampling might override previously seen
                        // items and we want to avoid unnecessary disk-lookup
                        sampleDocIds(fetchIdSamples, searchersToRelease.size() - 1, searcher);
                    } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                    }
                }
            }

            var sampler = new ColumnSampler(columnExpressions, searchersToRelease::get);
            sampler.iterate(fetchIdSamples.samples());

            List<ColumnSketchBuilder<?>> statsBuilders = new ArrayList<>(columnExpressions.size());
            for (var collector : columnExpressions) {
                statsBuilders.add(collector.statsBuilder);
            }
            return new Samples(statsBuilders, totalNumDocs, totalSizeInBytes);
        } finally {
            for (var cursor : searchersToRelease.entries()) {
                cursor.value().searcher.close();
            }
        }
    }

    @VisibleForTesting
    static void sampleDocIds(Reservoir reservoir, int readerIdx, IndexSearcher searcher) {
        var collector = new ReservoirCollector(reservoir, readerIdx);
        try {
            searcher.search(new MatchAllDocsQuery(), collector);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<? extends LuceneCollectorExpression<?>> getCollectorExpressions(
        IndexService indexService,
        DocTableInfo docTable,
        List<Reference> columns
    ) {
        String indexName = indexService.index().getName();
        LuceneReferenceResolver referenceResolver = new LuceneReferenceResolver(
            indexName,
            docTable.partitionedByColumns()
        );
        List<? extends LuceneCollectorExpression<?>> expressions = Lists.map(
            columns,
            x -> referenceResolver.getImplementation(DocReferences.toDocLookup(x))
        );

        CollectorContext collectorContext
            = new CollectorContext(() -> StoredRowLookup.create(docTable, indexName));
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }

        return expressions;
    }

    private static class ColumnExpression<T> {

        final ColumnSketchBuilder<T> statsBuilder;
        final SketchRamAccounting ramAccounting;
        final DataType<T> dataType;
        private final Reference column;
        LuceneCollectorExpression<?> expression;

        private ColumnExpression(SketchRamAccounting ramAccounting, Reference column) {
            this.dataType = (DataType<T>) column.valueType();
            this.column = column;
            this.statsBuilder = dataType.columnStatsSupport().sketchBuilder();
            this.ramAccounting = ramAccounting;
        }

        void setShardExpression(LuceneCollectorExpression<?> collector) {
            this.expression = collector;
        }

        void collect(int docId) {
            expression.setNextDocId(docId);
            Object rawValue = expression.value();
            try {
                T value = dataType.sanitizeValue(rawValue);
                ramAccounting.addBytes(dataType.valueBytes(value));
                statsBuilder.add(value);
            } catch (ClassCastException ex) {
                System.out.println(String.format(
                    Locale.ENGLISH,
                    "Value %s doesn't match type for column %s",
                    rawValue,
                    column
                ));
                throw ex;
            }
        }

        @Override
        public String toString() {
            return "ColumnCollector{" + column.column() + '}';
        }
    }

    private record ShardExpressions(Engine.Searcher searcher,
                                    List<? extends LuceneCollectorExpression<?>> expressions) {

        void setShardExpressions(List<ColumnExpression<?>> columnExpressions) {
            assert columnExpressions.size() == expressions.size();
            for (int i = 0; i < columnExpressions.size(); i++) {
                columnExpressions.get(i).setShardExpression(expressions.get(i));
            }
        }
    }

    private static class ColumnSampler {

        final List<ColumnExpression<?>> columnExpressions;
        final IntFunction<ShardExpressions> getShardExpressions;

        private ColumnSampler(List<ColumnExpression<?>> columnExpressions,
                              IntFunction<ShardExpressions> getShardExpressions) {
            this.columnExpressions = columnExpressions;
            this.getShardExpressions = getShardExpressions;
        }

        public final void iterate(LongArrayList ids) throws IOException {
            int currentShardId = -1;
            ShardExpressions currentShardExpressions = null;
            int currentReaderCeiling = -1;
            int currentReaderDocBase = -1;
            long rowsCollected = 0;
            for (LongCursor cursor : ids) {
                long fetchId = cursor.value;
                int shardId = FetchId.decodeReaderId(fetchId);
                if (shardId != currentShardId) {
                    currentShardId = shardId;
                    currentShardExpressions = getShardExpressions.apply(shardId);
                    currentShardExpressions.setShardExpressions(columnExpressions);
                    currentReaderCeiling = -1;
                }
                int docId = FetchId.decodeDocId(fetchId);
                if (docId >= currentReaderCeiling) {
                    assert currentShardExpressions != null;
                    Searcher searcher = currentShardExpressions.searcher;
                    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
                    int leafIdx = ReaderUtil.subIndex(docId, leaves);
                    LeafReaderContext leafContext = leaves.get(leafIdx);
                    ReaderContext readerContext = new ReaderContext(leafContext);
                    for (var columnExpression : columnExpressions) {
                        columnExpression.expression.setNextReader(readerContext);
                    }
                    currentReaderDocBase = leafContext.docBase;
                    currentReaderCeiling = currentReaderDocBase + leafContext.reader().maxDoc();
                }

                int doc = docId - currentReaderDocBase;
                try {
                    for (var collector : columnExpressions) {
                        collector.collect(doc);
                    }
                    rowsCollected++;
                } catch (CircuitBreakingException ex) {
                    LOGGER.info(
                        "Stopped gathering samples for `ANALYZE` operation because circuit breaker triggered. "
                            + "Generating statistics with {} instead of {} records",
                        rowsCollected,
                        ids.size()
                    );
                    break;
                }
            }
        }
    }

    private static class ReservoirCollector implements Collector {

        private final Reservoir reservoir;
        private final int readerIdx;

        ReservoirCollector(Reservoir reservoir, int readerIdx) {
            this.reservoir = reservoir;
            this.readerIdx = readerIdx;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new ReservoirLeafCollector(reservoir, readerIdx, context);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static class ReservoirLeafCollector implements LeafCollector {

        private final Reservoir reservoir;
        private final int readerIdx;
        private final LeafReaderContext context;

        ReservoirLeafCollector(Reservoir reservoir, int readerIdx, LeafReaderContext context) {
            this.reservoir = reservoir;
            this.readerIdx = readerIdx;
            this.context = context;
        }

        @Override
        public void setScorer(Scorable scorer) {
        }

        @Override
        public void collect(int doc) {
            var shouldContinue = reservoir.update(FetchId.encode(readerIdx, doc + context.docBase));
            if (shouldContinue == false) {
                throw new CollectionTerminatedException();
            }
        }
    }

}
