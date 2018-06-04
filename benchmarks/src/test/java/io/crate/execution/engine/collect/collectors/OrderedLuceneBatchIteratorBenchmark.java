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

package io.crate.execution.engine.collect.collectors;

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.OrderBy;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.shard.ShardId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class OrderedLuceneBatchIteratorBenchmark {

    private static final RowAccountingWithEstimators ROW_ACCOUNTING = new RowAccountingWithEstimators(Collections.singleton(LongType.INSTANCE),
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA))
    );

    private String columnName;
    private IndexSearcher indexSearcher;
    private boolean[] reverseFlags = new boolean[]{true};
    private Boolean[] nullsFirst = new Boolean[]{null};
    private Reference reference;
    private OrderBy orderBy;
    private CollectorContext collectorContext;
    private ShardId dummyShardId;

    @Setup
    public void createLuceneBatchIterator() throws Exception {
        IndexWriter iw = new IndexWriter(
            new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer())
        );
        dummyShardId = new ShardId("dummy", UUIDs.randomBase64UUID(), 1);
        columnName = "x";
        for (int i = 0; i < 10_000_000; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(columnName, i));
            iw.addDocument(doc);
        }
        iw.commit();
        iw.forceMerge(1, true);
        indexSearcher = new IndexSearcher(DirectoryReader.open(iw, true, true));
        collectorContext = new CollectorContext(
            mappedFieldType -> null,
            new CollectorFieldsVisitor(0)
        );
        reference = new Reference(
            new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "dummyTable"), columnName), RowGranularity.DOC, DataTypes.INTEGER);
        orderBy = new OrderBy(
            Collections.singletonList(reference),
            reverseFlags,
            nullsFirst
        );
    }

    @Benchmark
    public void measureLoadAndConsumeOrderedLuceneBatchIterator(Blackhole blackhole) {
        BatchIterator<Row> it = OrderedLuceneBatchIteratorFactory.newInstance(
            Collections.singletonList(createOrderedCollector(indexSearcher, columnName)),
            OrderingByPosition.rowOrdering(new int[]{0}, reverseFlags, nullsFirst),
            ROW_ACCOUNTING, MoreExecutors.directExecutor(),
            false
        );
        while (!it.allLoaded()) {
            it.loadNextBatch().toCompletableFuture().join();
        }
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    private LuceneOrderedDocCollector createOrderedCollector(IndexSearcher searcher,
                                                             String sortByColumnName) {
        List<LuceneCollectorExpression<?>> expressions = Collections.singletonList(
            new OrderByCollectorExpression(reference, orderBy, o -> o));
        return new LuceneOrderedDocCollector(
            dummyShardId,
            searcher,
            new MatchAllDocsQuery(),
            null,
            false,
            10_000_000,
            collectorContext,
            f -> null,
            new Sort(new SortedNumericSortField(sortByColumnName, SortField.Type.INT, reverseFlags[0])),
            expressions,
            expressions
        );
    }

}
