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
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public final class ReservoirSampler {

    private final ClusterService clusterService;
    private final Functions functions;
    private final Schemas schemas;
    private final IndicesService indicesService;

    @Inject
    public ReservoirSampler(ClusterService clusterService,
                            Functions functions,
                            Schemas schemas,
                            IndicesService indicesService) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.schemas = schemas;
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
        MetaData metaData = clusterService.state().metaData();
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        List<Streamer> streamers = Arrays.asList(Symbols.streamerArray(columns));
        Samples samples = Samples.EMPTY;
        for (String index : docTable.concreteOpenIndices()) {
            var indexMetaData = metaData.index(index);
            if (indexMetaData == null) {
                continue;
            }
            var indexService = indicesService.indexService(indexMetaData.getIndex());
            if (indexService == null) {
                continue;
            }
            FieldTypeLookup fieldTypeLookup = indexService.mapperService()::fullName;
            var ctx = new DocInputFactory(
                functions, fieldTypeLookup, new LuceneReferenceResolver(fieldTypeLookup)).getCtx(coordinatorTxnCtx);
            ctx.add(columns);
            List<Input<?>> inputs = ctx.topLevelInputs();
            List<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
            CollectorContext collectorContext = new CollectorContext(indexService.newQueryShardContext()::getForField);
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.startCollect(collectorContext);
            }
            // TODO: We could also go through all shards first and sample _fetchIds,
            //  and then do the conversion to values afterwards
            // would reduce the amount of value lookup's and make the merge unnecessary
            for (IndexShard indexShard : indexService) {
                if (!indexShard.routingEntry().primary()) {
                    continue;
                }
                try (Engine.Searcher searcher = indexShard.acquireSearcher("update-table-statistics")) {
                    DocIdToRow docIdToRow = new DocIdToRow(searcher, inputs, expressions);
                    try {
                        // We do the sampling in 2 phases. First we get the docIds;
                        // then we retrieve the column values for the sampled docIds.
                        // we do this in 2 phases because the reservoir sampling might override previously seen
                        // items and we want to avoid unnecessary disk-lookup
                        ReservoirCollector results = new ReservoirCollector(maxSamples, random);
                        searcher.searcher().search(new MatchAllDocsQuery(), results);
                        samples = samples.merge(
                            maxSamples,
                            new Samples(
                                Lists2.map(results.items.samples(), docIdToRow),
                                streamers,
                                searcher.reader().numDocs(),
                                indexShard.storeStats().getSizeInBytes()
                            ),
                            random
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                } catch (IllegalIndexShardStateException | AlreadyClosedException ignored) {
                }
            }
        }
        return samples;
    }


    static class DocIdToRow implements Function<Integer, Row> {

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
        public Row apply(Integer docId) {
            List<LeafReaderContext> leaves = searcher.reader().leaves();
            int readerIndex = ReaderUtil.subIndex(docId, leaves);
            LeafReaderContext leafContext = leaves.get(readerIndex);
            int subDoc = docId - leafContext.docBase;
            for (LuceneCollectorExpression<?> expression : expressions) {
                try {
                    expression.setNextReader(leafContext);
                    expression.setNextDocId(subDoc);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            Object[] cells = new Object[inputs.size()];
            for (int i = 0; i < cells.length; i++) {
                cells[i] = inputs.get(i).value();
            }
            // no shared rows because we know these are going to be stored.
            return new RowN(cells);
        }
    }

    private static class ReservoirCollector implements Collector {

        private final Reservoir<Integer> items;

        ReservoirCollector(int maxSamples, Random random) {
            items = new Reservoir<>(maxSamples, random);
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new ReservoirLeafCollector(items, context);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static class ReservoirLeafCollector implements LeafCollector {

        private final Reservoir<Integer> reservoir;
        private final LeafReaderContext context;

        ReservoirLeafCollector(Reservoir<Integer> reservoir, LeafReaderContext context) {
            this.reservoir = reservoir;
            this.context = context;
        }

        @Override
        public void setScorer(Scorable scorer) {
        }

        @Override
        public void collect(int doc) {
            reservoir.update(doc + context.docBase);
        }
    }
}
