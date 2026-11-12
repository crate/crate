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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.UnscoredLeafCollector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import io.crate.common.annotations.VisibleForTesting;
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
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableInfo;
import io.crate.types.DataType;

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

    Samples getSamples(RelationName relationName,
                       int tableOid,
                       List<Reference> columns,
                       Version minNodeVersion) {
        TableInfo table;
        try {
            table = tableOid == Metadata.OID_UNASSIGNED ?
                schemas.getTableInfo(relationName) : schemas.getRelationInfo(tableOid);
        } catch (RelationUnknown e) {
            return Samples.EMPTY;
        }
        if (!(table instanceof DocTableInfo docTable)) {
            return Samples.EMPTY;
        }
        Random random = Randomness.get();
        Metadata metadata = clusterService.state().metadata();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.QUERY);
        RamAccounting ramAccounting = new BlockBasedRamAccounting(
            b -> breaker.addEstimateBytesAndMaybeBreak(b, "Reservoir-sampling"),
            MAX_BLOCK_SIZE_IN_BYTES);

        try (SketchRamAccounting rla = new SketchRamAccounting(ramAccounting, rateLimiter)) {
            return getSamples(
                columns,
                minNodeVersion,
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
                               Version minNodeVersion,
                               DocTableInfo docTable,
                               SketchRamAccounting ramAccounting,
                               Random random,
                               Metadata metadata) throws IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Start collecting samples for table: {}({}),",
                docTable.ident().fqn(),
                columns.stream().map(r -> r.column().fqn()).collect(Collectors.joining(", ")));
        }

        long totalNumDocs = 0;
        long totalSizeInBytes = 0;

        List<ColumnCollector<?>> columnCollectors = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnCollectors.add(new ColumnCollector<>(ramAccounting, columns.get(i).valueType(), minNodeVersion));
        }

        List<ShardExpressions> searchersToRelease = new ArrayList<>();

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

                for (IndexShard indexShard : indexService) {
                    ShardRouting routingEntry = indexShard.routingEntry();
                    if (!routingEntry.primary() || !routingEntry.active()) {
                        continue;
                    }
                    try {
                        Engine.Searcher searcher = indexShard.acquireSearcher("update-table-statistics");
                        List<? extends LuceneCollectorExpression<?>> expressions
                            = getCollectorExpressions(indexMetadata.partitionValues(), docTable, indexShard.getVersionCreated(), columns);
                        searchersToRelease.add(new ShardExpressions(indexShard, indexMetadata.partitionValues(), searcher, docTable, expressions));
                        totalNumDocs += searcher.getIndexReader().numDocs();
                        totalSizeInBytes += indexShard.storeStats().sizeInBytes();
                    } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                    }
                }
            }

            // We do the sampling in 2 phases. First we get the docIds;
            // then we retrieve the column values for the sampled docIds.
            long[] fetchIds = sampleLiveDocIds(
                Lists.map(searchersToRelease, ShardExpressions::searcher),
                Reservoir.NUM_SAMPLES,
                random
            );

            var sampler = new ColumnSampler(columnCollectors, searchersToRelease::get);
            sampler.iterate(fetchIds, fetchIds.length);

            List<ColumnSketchBuilder<?>> statsBuilders = new ArrayList<>();
            for (var collector : columnCollectors) {
                statsBuilders.add(collector.statsBuilder);
            }

            return new Samples(statsBuilders, totalNumDocs, totalSizeInBytes);
        } finally {
            for (var shard : searchersToRelease) {
                shard.searcher.close();
            }
        }
    }

    @VisibleForTesting
    static void sampleDocIds(Reservoir reservoir, int readerIdx, IndexSearcher searcher) {
        try {
            searcher.search(MatchAllDocsQuery.INSTANCE, new ReservoirCollectorManager(reservoir, readerIdx));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// Samples `min(numSamples, live docs)` distinct live docs, each subset equally likely.
    ///
    /// @return the [FetchId]s of the sampled docs in increasing order; the reader id is the index into `searchers`
    @VisibleForTesting
    static long[] sampleLiveDocIds(List<? extends IndexSearcher> searchers, int numSamples, Random random) {
        long totalNumDocs = 0;
        for (IndexSearcher searcher : searchers) {
            totalNumDocs += searcher.getIndexReader().numDocs();
        }
        long[] ids = new long[(int) Math.min(numSamples, totalNumDocs)];
        sampleSortedRanks(ids, totalNumDocs, random);
        ranksToFetchIds(ids, searchers);
        return ids;
    }

    /// Replaces each rank (index of a doc among the live docs of all `searchers`, in reader and leaf order)
    /// with the [FetchId] of that doc. `ids` must be sorted.
    private static void ranksToFetchIds(long[] ids, List<? extends IndexSearcher> searchers) {
        int i = 0;
        long leafRankBase = 0;
        for (int readerIdx = 0; readerIdx < searchers.size() && i < ids.length; readerIdx++) {
            for (LeafReaderContext leaf : searchers.get(readerIdx).getIndexReader().leaves()) {
                LeafReader reader = leaf.reader();
                long leafRankEnd = leafRankBase + reader.numDocs();
                Bits liveDocs = reader.getLiveDocs();
                int docId = -1;
                long docRank = leafRankBase - 1;
                for (; i < ids.length && ids[i] < leafRankEnd; i++) {
                    if (liveDocs == null) {
                        docId = (int) (ids[i] - leafRankBase);
                    } else {
                        while (docRank < ids[i]) {
                            docId++;
                            if (liveDocs.get(docId)) {
                                docRank++;
                            }
                        }
                    }
                    ids[i] = FetchId.encode(readerIdx, leaf.docBase + docId);
                }
                leafRankBase = leafRankEnd;
            }
        }
        assert i == ids.length : "ranks must be below the sum of leaf numDocs";
    }

    /// Fills `ranks` with distinct values from `[0, populationSize)` in increasing order, each subset equally likely.
    ///
    /// Vitter's Algorithm D, from "An Efficient Algorithm for Sequential Random Sampling", ACM TOMS 13(1), 1987.
    /// It takes O(ranks.length) expected time. Like in the paper, it switches to Algorithm A once
    /// the remaining sample is more than 1/13 of the remaining population.
    private static void sampleSortedRanks(long[] ranks, long populationSize, Random random) {
        int n = ranks.length;
        if (n == 0) {
            return;
        }
        long N = populationSize;
        long current = 0;
        int out = 0;
        double nInv = 1.0 / n;
        double vPrime = Math.exp(Math.log(openUniform(random)) * nInv);
        long qu1 = N - n + 1;
        long threshold = 13L * n;
        while (n > 1 && threshold < N) {
            double nMin1Inv = 1.0 / (n - 1);
            double realN = N;
            long s;
            while (true) {
                // D2: generate S from the continuous approximation
                double x;
                while (true) {
                    x = realN * (1.0 - vPrime);
                    s = (long) x;
                    if (s < qu1) {
                        break;
                    }
                    vPrime = Math.exp(Math.log(openUniform(random)) * nInv);
                }
                // D3: accept S with the cheap test
                double u = openUniform(random);
                double y1 = Math.exp(Math.log(u * realN / qu1) * nMin1Inv);
                vPrime = y1 * (1.0 - x / realN) * ((double) qu1 / (qu1 - s));
                if (vPrime <= 1.0) {
                    break;
                }
                // D4: accept S with the exact test
                double y2 = 1.0;
                double top = realN - 1.0;
                double bottom;
                long limit;
                if (n - 1 > s) {
                    bottom = realN - n;
                    limit = N - s;
                } else {
                    bottom = realN - s - 1.0;
                    limit = qu1;
                }
                for (long t = N - 1; t >= limit; t--) {
                    y2 = y2 * top / bottom;
                    top -= 1.0;
                    bottom -= 1.0;
                }
                if (realN / (realN - x) >= y1 * Math.exp(Math.log(y2) * nMin1Inv)) {
                    vPrime = Math.exp(Math.log(openUniform(random)) * nMin1Inv);
                    break;
                }
                vPrime = Math.exp(Math.log(openUniform(random)) * nInv);
            }
            // D5: skip S ranks and select the next one
            current += s;
            ranks[out++] = current++;
            N -= s + 1;
            n--;
            nInv = nMin1Inv;
            qu1 -= s;
            threshold -= 13;
        }
        if (n > 1) {
            // Algorithm A
            long top = N - n;
            while (n > 1) {
                double v = openUniform(random);
                long s = 0;
                double quot = (double) top / N;
                while (quot > v) {
                    s++;
                    top--;
                    quot = quot * top / (N - s);
                }
                current += s;
                ranks[out++] = current++;
                N -= s + 1;
                n--;
            }
            vPrime = openUniform(random);
        }
        ranks[out] = current + Math.min((long) (N * vPrime), N - 1);
    }

    /// @return a uniform value in the open interval (0, 1)
    private static double openUniform(Random random) {
        return ((random.nextLong() >>> 11) + 0.5) * 0x1.0p-53;
    }

    private static List<? extends LuceneCollectorExpression<?>> getCollectorExpressions(
        List<String> partitionValues,
        DocTableInfo docTable,
        Version shardCreatedVersion,
        List<Reference> columns
    ) {
        LuceneReferenceResolver referenceResolver = new LuceneReferenceResolver(
            partitionValues,
            docTable.partitionedByColumns(),
            docTable.primaryKey(),
            shardCreatedVersion,
            docTable.isParentReferenceIgnored()
        );

        return Lists.map(
            columns,
            x -> referenceResolver.getImplementation(DocReferences.toDocLookup(x))
        );
    }

    private static class ColumnCollector<T> {

        LuceneCollectorExpression<?> expression;
        final ColumnSketchBuilder<T> statsBuilder;
        final SketchRamAccounting ramAccounting;
        final DataType<T> dataType;

        private ColumnCollector(SketchRamAccounting ramAccounting, DataType<T> dataType, Version minNodeVersion) {
            this.statsBuilder = dataType.columnStatsSupport().sketchBuilder(minNodeVersion);
            this.dataType = dataType;
            this.ramAccounting = ramAccounting;
        }

        void setShard(LuceneCollectorExpression<?> collector) {
            this.expression = collector;
        }

        void collect(int docId) {
            expression.setNextDocId(docId);
            T value = dataType.sanitizeValue(expression.value());
            ramAccounting.addBytes(dataType.valueBytes(value));
            statsBuilder.add(value);
        }
    }

    private record ShardExpressions(IndexShard indexShard,
                                    List<String> partitionValues,
                                    Engine.Searcher searcher,
                                    DocTableInfo tableInfo,
                                    List<? extends LuceneCollectorExpression<?>> expressions) {

        void updateColumnCollectors(List<ColumnCollector<?>> collectors) {
            assert collectors.size() == expressions.size();
            CollectorContext context = new CollectorContext(() -> StoredRowLookup.create(
                indexShard.getVersionCreated(),
                tableInfo,
                partitionValues));
            for (int i = 0; i < collectors.size(); i++) {
                LuceneCollectorExpression<?> expression = expressions.get(i);
                expression.startCollect(context);
                collectors.get(i).setShard(expression);
            }
        }
    }

    private static class ColumnSampler {

        final List<ColumnCollector<?>> collectors;
        final IntFunction<ShardExpressions> shardSupplier;

        ShardExpressions currentShard;
        ReaderContext currentLeafContext;
        int rowsCollected;
        long idCount;

        private ColumnSampler(
            List<ColumnCollector<?>> collectors,
            IntFunction<ShardExpressions> shardSupplier
        ) {
            this.collectors = collectors;
            this.shardSupplier = shardSupplier;
        }

        public final void iterate(long[] ids, int numIds) throws IOException {

            this.idCount = numIds;

            int currentShardId = -1;
            IndexReaderContext currentShardContext = null;
            int currentReaderCeiling = -1;
            int currentReaderDocBase = -1;

            for (int i = 0; i < numIds; i++) {
                long id = ids[i];
                int shardId = FetchId.decodeReaderId(id);
                if (shardId != currentShardId) {
                    currentShardId = shardId;
                    currentShardContext = nextShard(shardId);
                    currentReaderCeiling = -1;
                }
                int docId = FetchId.decodeDocId(id);
                if (docId >= currentReaderCeiling) {
                    assert currentShardContext != null;
                    int contextOrd = ReaderUtil.subIndex(docId, currentShardContext.leaves());
                    var leafContext = nextReaderContext(contextOrd);
                    currentReaderDocBase = leafContext.docBase;
                    currentReaderCeiling = currentReaderDocBase + leafContext.reader().maxDoc();
                }

                if (collect(docId - currentReaderDocBase) == false) {
                    break;
                }
            }
        }

        protected IndexReaderContext nextShard(int shardId) {
            currentShard = shardSupplier.apply(shardId);
            currentShard.updateColumnCollectors(collectors);
            return currentShard.searcher.getTopReaderContext();
        }

        protected LeafReaderContext nextReaderContext(int contextOrd) throws IOException {
            var ctx = currentShard.searcher.getLeafContexts().get(contextOrd);
            currentLeafContext = new ReaderContext(ctx);
            for (var collector : collectors) {
                collector.expression.setNextReader(currentLeafContext);
            }
            return ctx;
        }

        protected boolean collect(int doc) {
            try {
                for (var collector : collectors) {
                    collector.collect(doc);
                }
                rowsCollected++;
                return true;
            } catch (CircuitBreakingException e) {
                LOGGER.info(
                    "Stopped gathering samples for `ANALYZE` operation because circuit breaker triggered. "
                        + "Generating statistics with {} instead of {} records",
                    rowsCollected,
                    idCount
                );
                return false;
            }
        }
    }

    private record ReservoirCollectorManager(Reservoir reservoir,
                                             int readerIdx) implements CollectorManager<Collector, Reservoir> {

        @Override
        public Collector newCollector() throws IOException {
            return new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) {
                    return (UnscoredLeafCollector) doc -> {
                        var shouldContinue = reservoir.update(FetchId.encode(readerIdx, doc + context.docBase));
                        if (shouldContinue == false) {
                            throw new CollectionTerminatedException();
                        }
                    };
                }

                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }
            };
        }

        @Override
        public Reservoir reduce(Collection<Collector> collectors) {
            return reservoir;
        }
    }

}
