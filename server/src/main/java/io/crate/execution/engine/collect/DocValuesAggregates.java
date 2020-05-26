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

package io.crate.execution.engine.collect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.aggregation.impl.AverageAggregation;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;

public class DocValuesAggregates {

    @Nullable
    public static BatchIterator<Row> tryOptimize(IndexShard indexShard,
                                                 DocTableInfo table,
                                                 LuceneQueryBuilder luceneQueryBuilder,
                                                 FieldTypeLookup fieldTypeLookup,
                                                 DocInputFactory docInputFactory,
                                                 RoutedCollectPhase phase,
                                                 CollectTask collectTask) {
        var shardProjections = Projections.shardProjections(phase.projections());
        AggregationProjection aggregateProjection = aggregateProjection(shardProjections);
        if (aggregateProjection == null) {
            return null;
        }
        var aggregators = createAggregators(aggregateProjection, fieldTypeLookup, phase.toCollect());
        if (aggregators == null) {
            return null;
        }
        ShardId shardId = indexShard.shardId();
        SharedShardContext shardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Searcher searcher = shardContext.acquireSearcher(LuceneShardCollectorProvider.formatSource(phase));
        try {
            QueryShardContext queryShardContext = shardContext.indexService().newQueryShardContext();
            collectTask.addSearcher(shardContext.readerId(), searcher);
            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                phase.where(),
                collectTask.txnCtx(),
                indexShard.mapperService(),
                indexShard.shardId().getIndexName(),
                queryShardContext,
                table,
                shardContext.indexService().cache()
            );

            AtomicReference<Throwable> killed = new AtomicReference<>();
            return CollectingBatchIterator.newInstance(
                () -> killed.set(BatchIterator.CLOSED),
                killed::set,
                () -> {
                    try {
                        return CompletableFuture.completedFuture(getRow(
                            searcher,
                            queryContext.query(),
                            aggregators
                        ));
                    } catch (Throwable t) {
                        return CompletableFuture.failedFuture(t);
                    }
                },
                true
            );
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }

    @Nullable
    private static List<Aggregator> createAggregators(AggregationProjection aggregateProjection,
                                                      FieldTypeLookup fieldTypeLookup,
                                                      List<Symbol> toCollect) {
        List<Aggregation> aggregations = aggregateProjection.aggregations();
        ArrayList<Aggregator> aggregator = new ArrayList<>(aggregations.size());
        for (int i = 0; i < aggregations.size(); i++) {
            Aggregation aggregation = aggregations.get(i);

            if (aggregation.inputs().size() != 1) {
                return null;
            }
            Symbol input = aggregation.inputs().get(0);
            if (!(input instanceof InputColumn)) {
                return null;
            }
            Symbol collectSymbol = toCollect.get(((InputColumn) input).index());
            if (!(collectSymbol instanceof Reference)) {
                return null;
            }

            // TODO: We could also add an adapter for the default `AggregationFunction` implementations,
            // in case any aggregations here don't fit the requirements for the optimized versions


            // TODO: What about FILTER clauses?
            //
            Reference ref = (Reference) collectSymbol;
            String fqn = ref.column().fqn();
            MappedFieldType fieldType = fieldTypeLookup.get(fqn);
            if (fieldType == null || !fieldType.hasDocValues()) {
                return null;
            }
            switch (aggregation.functionIdent().name()) {
                case "sum": {
                    switch (ref.valueType().id()) {
                        case ShortType.ID:
                        case IntegerType.ID:
                        case LongType.ID:
                            aggregator.add(new SumLong(fqn));
                            continue;

                        case FloatType.ID:
                        case DoubleType.ID:
                            aggregator.add(new SumDouble(fqn));
                            continue;

                        default:
                            return null;
                    }
                }

                case "mean":
                case "avg": {
                    switch (ref.valueType().id()) {
                        case ShortType.ID:
                        case IntegerType.ID:
                        case LongType.ID:
                            aggregator.add(new AvgLong(fqn));
                            continue;

                        case FloatType.ID:
                        case DoubleType.ID:
                            aggregator.add(new AvgDouble(fqn));
                            continue;

                        default:
                            return null;
                    }
                }

                default:
                    return null;
            }
        }
        return aggregator;
    }

    // TODO: integrate this into aggregation functions - maybe as a `createDocValuesAggregator()`
    // Or create a separate registry for optimized aggregation functions?
    abstract static class Aggregator {

        public abstract void loadDocValues(LeafReader reader) throws IOException;

        public abstract void apply(int doc) throws IOException;

        // Aggregations are executed on shard level,
        // that means there is always a final reduce step necessary
        // → never return final value, but always partial result
        public abstract Object partialResult();
    }

    static class SumLong extends Aggregator {

        private final String columnName;
        private SortedNumericDocValues values;
        private long sum = 0;
        private boolean hadValue = false;

        SumLong(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                sum += values.nextValue();
                hadValue = true;
            }
        }

        @Override
        public Object partialResult() {
            return hadValue ? sum : null;
        }
    }

    static class SumDouble extends Aggregator {

        private final String columnName;
        private SortedNumericDocValues values;
        private double sum = 0.0;
        private boolean hadValue = false;

        SumDouble(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                sum += NumericUtils.sortableLongToDouble(values.nextValue());
                hadValue = true;
            }
        }

        @Override
        public Object partialResult() {
            return hadValue ? sum : null;
        }
    }

    static class AvgLong extends Aggregator {

        private final String columnName;
        private SortedNumericDocValues values;

        private long sum = 0;
        private long count = 0;

        public AvgLong(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);

        }

        @Override
        public void apply(int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                count++;
                sum += values.nextValue();
            }
        }

        @Override
        public Object partialResult() {
            AverageAggregation.AverageState averageState = new AverageAggregation.AverageState();
            averageState.sum = sum;
            averageState.count = count;
            return averageState;
        }
    }


    static class AvgDouble extends Aggregator {

        private final String columnName;
        private SortedNumericDocValues values;

        private double sum = 0.0;
        private long count = 0;

        public AvgDouble(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                count++;
                sum += NumericUtils.sortableLongToDouble(values.nextValue());
            }
        }

        @Override
        public Object partialResult() {
            AverageAggregation.AverageState averageState = new AverageAggregation.AverageState();
            averageState.sum = sum;
            averageState.count = count;
            return averageState;
        }
    }


    private static Iterable<Row> getRow(Searcher searcher,
                                        Query query,
                                        List<Aggregator> aggregators) throws IOException {
        IndexSearcher indexSearcher = searcher.searcher();
        Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        for (var leaf : leaves) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            for (int i = 0; i < aggregators.size(); i++) {
                aggregators.get(i).loadDocValues(leaf.reader());
            }
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                if (liveDocs != null && !liveDocs.get(doc)) {
                    continue;
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    aggregators.get(i).apply(doc);
                }
            }
        }
        Object[] cells = new Object[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            cells[i] = aggregators.get(i).partialResult();
        }
        return List.of(new RowN(cells));
    }


    @Nullable
    private static AggregationProjection aggregateProjection(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        var projection = shardProjections.iterator().next();
        if (!(projection instanceof AggregationProjection)) {
            return null;
        }
        return (AggregationProjection) projection;
    }
}
