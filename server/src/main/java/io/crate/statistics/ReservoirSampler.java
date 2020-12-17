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

package io.crate.statistics;

import io.crate.Streamer;
import io.crate.breaker.BlockBasedRamAccounting;
import io.crate.breaker.RamAccounting;
import io.crate.breaker.RowCellsAccountingWithEstimators;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.RowN;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.execution.engine.fetch.FetchId;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataTypes;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static io.crate.breaker.BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES;

public final class ReservoirSampler {

    private final ClusterService clusterService;
    private final NodeContext nodeCtx;
    private final Schemas schemas;
    private CircuitBreakerService circuitBreakerService;
    private final IndicesService indicesService;

    @Inject
    public ReservoirSampler(ClusterService clusterService,
                            NodeContext nodeCtx,
                            Schemas schemas,
                            CircuitBreakerService circuitBreakerService,
                            IndicesService indicesService) {
        this.clusterService = clusterService;
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.circuitBreakerService = circuitBreakerService;
        this.indicesService = indicesService;
    }

    public Samples getSamples(RelationName relationName, List<Reference> columns, int maxSamples) {
        TableInfo table;
        try {
            table = schemas.getTableInfo(relationName);
        } catch (RelationUnknown e) {
            return Samples.EMPTY;
        }
        if (!(table instanceof DocTableInfo)) {
            return Samples.EMPTY;
        }
        DocTableInfo docTable = (DocTableInfo) table;
        Random random = Randomness.get();
        Metadata metadata = clusterService.state().metadata();
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        List<Streamer> streamers = Arrays.asList(Symbols.streamerArray(columns));
        List<Engine.Searcher> searchersToRelease = new ArrayList<>();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
        RamAccounting ramAccounting = new BlockBasedRamAccounting(
            b -> breaker.addEstimateBytesAndMaybeBreak(b, "Reservoir-sampling"),
            MAX_BLOCK_SIZE_IN_BYTES);
        try {
            return getSamples(
                columns,
                maxSamples,
                docTable,
                random,
                metadata,
                coordinatorTxnCtx,
                streamers,
                searchersToRelease,
                ramAccounting
            );
        } finally {
            ramAccounting.close();
            for (Engine.Searcher searcher : searchersToRelease) {
                searcher.close();
            }
        }
    }

    private Samples getSamples(List<Reference> columns,
                               int maxSamples,
                               DocTableInfo docTable,
                               Random random,
                               Metadata metadata,
                               CoordinatorTxnCtx coordinatorTxnCtx,
                               List<Streamer> streamers,
                               List<Engine.Searcher> searchersToRelease,
                               RamAccounting ramAccounting) {
        ramAccounting.addBytes(DataTypes.LONG.fixedSize() * maxSamples);
        Reservoir<Long> fetchIdSamples = new Reservoir<>(maxSamples, random);
        ArrayList<DocIdToRow> docIdToRowsFunctionPerReader = new ArrayList<>();
        long totalNumDocs = 0;
        long totalSizeInBytes = 0;
        for (String index : docTable.concreteOpenIndices()) {
            var indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                continue;
            }
            var indexService = indicesService.indexService(indexMetadata.getIndex());
            if (indexService == null) {
                continue;
            }
            var mapperService = indexService.mapperService();
            FieldTypeLookup fieldTypeLookup = mapperService::fullName;
            var ctx = new DocInputFactory(
                nodeCtx,
                new LuceneReferenceResolver(
                    indexService.index().getName(),
                    fieldTypeLookup,
                    docTable.partitionedByColumns()
                )
            ).getCtx(coordinatorTxnCtx);
            ctx.add(columns);
            List<Input<?>> inputs = ctx.topLevelInputs();
            List<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
            CollectorContext collectorContext = new CollectorContext();
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.startCollect(collectorContext);
            }
            for (IndexShard indexShard : indexService) {
                if (!indexShard.routingEntry().primary()) {
                    continue;
                }
                try {
                    Engine.Searcher searcher = indexShard.acquireSearcher("update-table-statistics");
                    searchersToRelease.add(searcher);
                    totalNumDocs += searcher.getIndexReader().numDocs();
                    totalSizeInBytes += indexShard.storeStats().getSizeInBytes();
                    DocIdToRow docIdToRow = new DocIdToRow(searcher, inputs, expressions);
                    docIdToRowsFunctionPerReader.add(docIdToRow);
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
        var rowAccounting = new RowCellsAccountingWithEstimators(Symbols.typeView(columns), ramAccounting, 0);
        return new Samples(
            Lists2.map(fetchIdSamples.samples(), fetchId -> {
                int readerId = FetchId.decodeReaderId(fetchId);
                DocIdToRow docIdToRow = docIdToRowsFunctionPerReader.get(readerId);
                Object[] row = docIdToRow.apply(FetchId.decodeDocId(fetchId));
                rowAccounting.accountForAndMaybeBreak(row);
                return new RowN(row);
            }),
            streamers,
            totalNumDocs,
            totalSizeInBytes
        );
    }

    static class DocIdToRow implements Function<Integer, Object[]> {

        private final Engine.Searcher searcher;
        private final List<Input<?>> inputs;
        private final List<? extends LuceneCollectorExpression<?>> expressions;

        DocIdToRow(Engine.Searcher searcher,
                   List<Input<?>> inputs,
                   List<? extends LuceneCollectorExpression<?>> expressions) {
            this.searcher = searcher;
            this.inputs = inputs;
            this.expressions = expressions;
        }

        @Override
        public Object[] apply(Integer docId) {
            List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
            int readerIndex = ReaderUtil.subIndex(docId, leaves);
            LeafReaderContext leafContext = leaves.get(readerIndex);
            int subDoc = docId - leafContext.docBase;
            var readerContext = new ReaderContext(leafContext);
            for (LuceneCollectorExpression<?> expression : expressions) {
                try {
                    expression.setNextReader(readerContext);
                    expression.setNextDocId(subDoc);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            Object[] cells = new Object[inputs.size()];
            for (int i = 0; i < cells.length; i++) {
                cells[i] = inputs.get(i).value();
            }
            return cells;
        }
    }

    private static class ReservoirCollector implements Collector {

        private final Reservoir<Long> reservoir;
        private int readerIdx;

        ReservoirCollector(Reservoir<Long> reservoir, int readerIdx) {
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

        private final Reservoir<Long> reservoir;
        private final int readerIdx;
        private final LeafReaderContext context;

        ReservoirLeafCollector(Reservoir<Long> reservoir, int readerIdx, LeafReaderContext context) {
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
