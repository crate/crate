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

import static io.crate.statistics.TableStatsService.STATS_SERVICE_THROTTLING_SETTING;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.execution.engine.fetch.FetchId;
import io.crate.execution.engine.fetch.FetchIdIterator;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;

public final class ReservoirSampler {

    private static final Logger LOGGER = LogManager.getLogger(ReservoirSampler.class);

    private final ClusterService clusterService;
    private final NodeContext nodeCtx;
    private final Schemas schemas;
    private final CircuitBreakerService circuitBreakerService;
    private final IndicesService indicesService;

    private final RateLimiter rateLimiter;

    @Inject
    public ReservoirSampler(ClusterService clusterService,
                            NodeContext nodeCtx,
                            Schemas schemas,
                            CircuitBreakerService circuitBreakerService,
                            IndicesService indicesService,
                            Settings settings) {
        this(clusterService,
             nodeCtx,
             schemas,
             circuitBreakerService,
             indicesService,
             new RateLimiter.SimpleRateLimiter(STATS_SERVICE_THROTTLING_SETTING.get(settings).getMbFrac())
        );
    }

    @VisibleForTesting
    ReservoirSampler(ClusterService clusterService,
                            NodeContext nodeCtx,
                            Schemas schemas,
                            CircuitBreakerService circuitBreakerService,
                            IndicesService indicesService,
                            RateLimiter rateLimiter) {
        this.clusterService = clusterService;
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.circuitBreakerService = circuitBreakerService;
        this.indicesService = indicesService;
        this.rateLimiter = rateLimiter;

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_THROTTLING_SETTING, this::updateReadLimit);
    }

    private void updateReadLimit(ByteSizeValue newReadLimit) {
        rateLimiter.setMBPerSec(newReadLimit.getMbFrac()); // mbPerSec is volatile in SimpleRateLimiter, one volatile write
    }

    Samples getSamples(RelationName relationName, List<Reference> columns, int maxSamples) {
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
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        try {
            return getSamples(
                columns,
                maxSamples,
                docTable,
                random,
                metadata,
                coordinatorTxnCtx
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Samples getSamples(List<Reference> columns,
                               int maxSamples,
                               DocTableInfo docTable,
                               Random random,
                               Metadata metadata,
                               CoordinatorTxnCtx coordinatorTxnCtx) throws IOException {

        Reservoir fetchIdSamples = new Reservoir(maxSamples, random);
        long totalNumDocs = 0;
        long totalSizeInBytes = 0;

        List<ColumnCollector<?>> columnCollectors = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnCollectors.add(new ColumnCollector<>(new ColumnSketchBuilder<>(columns.get(i).valueType())));
        }

        List<ShardExpressions> searchersToRelease = new ArrayList<>();

        try {

            for (String index : docTable.concreteOpenIndices()) {
                var indexMetadata = metadata.index(index);
                if (indexMetadata == null) {
                    continue;
                }
                var indexService = indicesService.indexService(indexMetadata.getIndex());
                if (indexService == null) {
                    continue;
                }

                ExpressionsAndInputs expressionsAndInputs
                    = getExpressionsAndInputs(indexService, docTable, coordinatorTxnCtx, columns);

                for (IndexShard indexShard : indexService) {
                    if (!indexShard.routingEntry().primary()) {
                        continue;
                    }
                    try {
                        Engine.Searcher searcher = indexShard.acquireSearcher("update-table-statistics");
                        searchersToRelease.add(new ShardExpressions(searcher, expressionsAndInputs));
                        totalNumDocs += searcher.getIndexReader().numDocs();
                        totalSizeInBytes += indexShard.storeStats().getSizeInBytes();

                        try {
                            // We do the sampling in 2 phases. First we get the docIds;
                            // then we retrieve the column values for the sampled docIds.
                            // we do this in 2 phases because the reservoir sampling might override previously seen
                            // items and we want to avoid unnecessary disk-lookup
                            var collector = new ReservoirCollector(fetchIdSamples, searchersToRelease.size() - 1);
                            searcher.search(new MatchAllDocsQuery(), collector);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                    }
                }
            }

            var sampler = new ColumnSampler(columnCollectors, searchersToRelease::get);
            sampler.iterate(fetchIdSamples.samples());

            List<ColumnSketch<?>> statsBuilders = new ArrayList<>();
            for (var collector : columnCollectors) {
                statsBuilders.add(collector.statsBuilder.toSketch());
            }

            return new Samples(statsBuilders, totalNumDocs, totalSizeInBytes);
        } finally {
            for (var shard : searchersToRelease) {
                shard.searcher.close();
            }
        }
    }

    private ExpressionsAndInputs getExpressionsAndInputs(IndexService indexService,
                                                         DocTableInfo docTable,
                                                         CoordinatorTxnCtx coordinatorTxnCtx,
                                                         List<Reference> columns) {
        var mapperService = indexService.mapperService();
        FieldTypeLookup fieldTypeLookup = mapperService::fieldType;
        var ctx = new DocInputFactory(
            nodeCtx,
            new LuceneReferenceResolver(
                indexService.index().getName(),
                fieldTypeLookup,
                docTable.partitionedByColumns()
            )
        ).getCtx(coordinatorTxnCtx);
        ctx.add(Lists.map(columns, DocReferences::toSourceLookup));
        List<Input<?>> inputs = ctx.topLevelInputs();
        List<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();

        CollectorContext collectorContext = new CollectorContext(docTable.droppedColumns(), docTable.lookupNameBySourceKey());
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }

        return new ExpressionsAndInputs(expressions, inputs);
    }

    private static class ColumnCollector<T> {
        LuceneCollectorExpression<?> collector;
        Input<?> input;
        final ColumnSketchBuilder<T> statsBuilder;

        private ColumnCollector(ColumnSketchBuilder<T> statsBuilder) {
            this.statsBuilder = statsBuilder;
        }

        void setShard(LuceneCollectorExpression<?> collector, Input<?> input) {
            this.collector = collector;
            this.input = input;
        }

        @SuppressWarnings("unchecked")
        void collect(int docId) {
            collector.setNextDocId(docId);
            statsBuilder.add((T) input.value());
        }
    }

    private record ExpressionsAndInputs(
        List<? extends LuceneCollectorExpression<?>> expressions,
        List<Input<?>> inputs
    ) {}

    private record ShardExpressions(Engine.Searcher searcher,
                                    ExpressionsAndInputs expressionsAndInputs) {

        void updateColumnCollectors(List<ColumnCollector<?>> collectors) {
            assert collectors.size() == expressionsAndInputs.expressions.size();
            assert collectors.size() == expressionsAndInputs.inputs.size();
            for (int i = 0; i < collectors.size(); i++) {
                collectors.get(i).setShard(expressionsAndInputs.expressions.get(i), expressionsAndInputs.inputs.get(i));
            }
        }

    }

    private static class ColumnSampler extends FetchIdIterator {

        final List<ColumnCollector<?>> expressions;
        final IntFunction<ShardExpressions> shardSupplier;

        ShardExpressions currentShard;
        ReaderContext currentLeafContext;
        int rowsCollected;

        private ColumnSampler(
            List<ColumnCollector<?>> expressions,
            IntFunction<ShardExpressions> shardSupplier
        ) {
            this.expressions = expressions;
            this.shardSupplier = shardSupplier;
        }

        @Override
        protected IndexReaderContext nextShard(int shardId) {
            currentShard = shardSupplier.apply(shardId);
            currentShard.updateColumnCollectors(expressions);
            return currentShard.searcher.getTopReaderContext();
        }

        @Override
        protected LeafReaderContext nextReaderContext(int contextOrd) throws IOException {
            var ctx = currentShard.searcher.getLeafContexts().get(contextOrd);
            currentLeafContext = new ReaderContext(ctx);
            for (var expression : expressions) {
                expression.collector.setNextReader(currentLeafContext);
            }
            return ctx;
        }

        @Override
        protected boolean collect(int doc) {
            try {
                for (var expression : expressions) {
                    expression.collect(doc);
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
