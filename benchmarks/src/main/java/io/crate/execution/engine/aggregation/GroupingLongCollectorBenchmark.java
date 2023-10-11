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

package io.crate.execution.engine.aggregation;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.netty.util.collection.LongObjectHashMap;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Measurement(iterations = 5)
@Fork(value = 2)
@Warmup(iterations = 2)
public class GroupingLongCollectorBenchmark {

    private GroupingCollector groupBySumCollector;
    private List<Row> rows;
    private long[] numbers;
    private IndexSearcher searcher;

    @Setup
    public void createGroupingCollector() throws Exception {
        IndexWriter iw = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        Functions functions = new ModulesBuilder()
            .add(new AggregationImplModule())
            .createInjector()
            .getInstance(Functions.class);
        SumAggregation<?> sumAgg = (SumAggregation<?>) functions.getQualified(
            Signature.aggregate(
                SumAggregation.NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            List.of(DataTypes.INTEGER),
            DataTypes.INTEGER
        );
        var memoryManager = new OnHeapMemoryManager(bytes -> {});
        groupBySumCollector = createGroupBySumCollector(sumAgg, memoryManager);

        int size = 20_000_000;
        rows = new ArrayList<>(size);
        numbers = new long[size];
        for (int i = 0; i < size; i++) {
            long value = (long) i % 200;
            rows.add(new Row1(value));
            numbers[i] = value;
            var doc = new Document();
            doc.add(new NumericDocValuesField("x", value));
            doc.add(new SortedNumericDocValuesField("y", value));
            iw.addDocument(doc);
        }
        iw.commit();
        iw.forceMerge(1, true);
        searcher = new IndexSearcher(DirectoryReader.open(iw));
    }

    private static GroupingCollector createGroupBySumCollector(AggregationFunction sumAgg, MemoryManager memoryManager) {
        RowCollectExpression keyInput = new RowCollectExpression(0);
        List<Input<?>> keyInputs = Arrays.<Input<?>>asList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        return GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAgg },
            new Input[][] { new Input[] { keyInput }},
            new Input[] { Literal.BOOLEAN_TRUE },
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            Version.CURRENT,
            keyInputs.get(0),
            DataTypes.LONG,
            Version.CURRENT
        );
    }

    @Benchmark
    public void measureGroupBySumLong(Blackhole blackhole) throws Exception {
        var rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(rowsIterator.collect(groupBySumCollector).get());
    }

    @Benchmark
    public LongObjectHashMap<Long> measureGroupingOnNumericDocValues() throws Exception {
        Weight weight = searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        LeafReaderContext leaf = searcher.getTopReaderContext().leaves().get(0);
        Scorer scorer = weight.scorer(leaf);
        NumericDocValues docValues = DocValues.getNumeric(leaf.reader(), "x");
        DocIdSetIterator docIt = scorer.iterator();
        LongObjectHashMap<Long> sumByKey = new LongObjectHashMap<>();
        for (int docId = docIt.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docIt.nextDoc()) {
            if (docValues.advanceExact(docId)) {
                long number = docValues.longValue();
                sumByKey.compute(number, (key, oldValue) -> {
                    if (oldValue == null) {
                        return number;
                    } else {
                        return oldValue + number;
                    }
                });
            }
        }
        return sumByKey;
    }

    @Benchmark
    public LongObjectHashMap<Long> measureGroupingOnSortedNumericDocValues() throws Exception {
        var weight = searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        var leaf = searcher.getTopReaderContext().leaves().get(0);
        var scorer = weight.scorer(leaf);
        var docValues = DocValues.getSortedNumeric(leaf.reader(), "y");
        var docIt = scorer.iterator();
        LongObjectHashMap<Long> sumByKey = new LongObjectHashMap<>();
        for (int docId = docIt.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docIt.nextDoc()) {
            if (docValues.advanceExact(docId)) {
                if (docValues.docValueCount() == 1) {
                    long number = docValues.nextValue();
                    sumByKey.compute(number, (key, oldValue) -> {
                        if (oldValue == null) {
                            return number;
                        } else {
                            return oldValue + number;
                        }
                    });
                }
            }
        }
        return sumByKey;
    }

    /**
     * To establish a base line on how fast it could go
     */
    @Benchmark
    public LongObjectHashMap<Long> measureGroupingOnLongArray() {
        LongObjectHashMap<Long> sumByKey = new LongObjectHashMap<>();
        for (long number : numbers) {
            sumByKey.compute(number, (key, oldValue) -> {
                if (oldValue == null) {
                    return number;
                } else {
                    return oldValue + number;
                }
            });
        }
        return sumByKey;
    }
}
