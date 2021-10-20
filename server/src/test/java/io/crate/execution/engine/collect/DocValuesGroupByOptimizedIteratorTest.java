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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.common.lucene.BytesRefs;
import org.junit.Before;
import org.junit.Test;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.BytesRefColumnReference;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LongColumnReference;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Reference.IndexType;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;

public class DocValuesGroupByOptimizedIteratorTest extends CrateDummyClusterServiceUnitTest {

    private Functions functions;
    private IndexSearcher indexSearcher;

    private List<Object[]> rows = List.of(
        new Object[]{"1", 1L, 1L},
        new Object[]{"0", 0L, 2L},
        new Object[]{"1", 1L, 3L},
        new Object[]{"0", 0L, 4L}
    );

    @Before
    public void setup() throws IOException {
        var nodeContext = createNodeContext();
        functions = nodeContext.functions();

        var indexWriter = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig());
        for (var row : rows) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("x", BytesRefs.toBytesRef(row[0])));
            doc.add(new NumericDocValuesField("y", (Long) row[1]));
            doc.add(new NumericDocValuesField("z", (Long) row[2]));
            indexWriter.addDocument(doc);
        }
        indexWriter.commit();
        indexSearcher = new IndexSearcher(DirectoryReader.open(indexWriter));
    }

    @Test
    public void test_group_by_doc_values_optimized_iterator_for_single_numeric_key() throws Exception {
        SumAggregation<?> sumAggregation = (SumAggregation<?>) functions.getQualified(
            Signature.aggregate(
                SumAggregation.NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            List.of(DataTypes.LONG),
            DataTypes.LONG
        );

        var sumDocValuesAggregator = sumAggregation.getDocValueAggregator(
            List.of(new Reference(
                new ReferenceIdent(RelationName.fromIndexName("test"), "z"),
                RowGranularity.DOC,
                DataTypes.LONG,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                0,
                null)
            ),
            mock(DocTableInfo.class),
            List.of()
        );
        var keyExpressions = List.of(new LongColumnReference("y"));
        var it = DocValuesGroupByOptimizedIterator.GroupByIterator.forSingleKey(
            List.of(sumDocValuesAggregator),
            indexSearcher,
            new Reference(
                new ReferenceIdent(RelationName.fromIndexName("test"), "y"),
                RowGranularity.DOC,
                DataTypes.LONG,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                0,
                null
            ),
            keyExpressions,
            RamAccounting.NO_ACCOUNTING,
            null,
            null,
            new MatchAllDocsQuery(),
            new CollectorContext()
        );

        var rowConsumer = new TestingRowConsumer();
        rowConsumer.accept(it, null);
        assertThat(
            rowConsumer.getResult(),
            containsInAnyOrder(new Object[]{0L, 6L}, new Object[]{1L, 4L}));
    }

    @Test
    public void test_group_by_doc_values_optimized_iterator_for_many_keys() throws Exception {
        SumAggregation<?> sumAggregation = (SumAggregation<?>) functions.getQualified(
            Signature.aggregate(
                SumAggregation.NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            List.of(DataTypes.LONG),
            DataTypes.LONG
        );

        var sumDocValuesAggregator = sumAggregation.getDocValueAggregator(
            List.of(new Reference(
                new ReferenceIdent(
                    RelationName.fromIndexName("test"),
                    "z"),
                RowGranularity.DOC,
                DataTypes.LONG,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                0,
                null)
            ),
            mock(DocTableInfo.class),
            List.of()
        );
        var keyExpressions = List.of(new BytesRefColumnReference("x"), new LongColumnReference("y"));
        var keyRefs = List.of(
            new Reference(
                new ReferenceIdent(RelationName.fromIndexName("test"), "x"),
                RowGranularity.DOC,
                DataTypes.STRING,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                1,
                null
            ),
            new Reference(
                new ReferenceIdent(RelationName.fromIndexName("test"), "y"),
                RowGranularity.DOC,
                DataTypes.LONG,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                2,
                null
            )
        );
        var it = DocValuesGroupByOptimizedIterator.GroupByIterator.forManyKeys(
            List.of(sumDocValuesAggregator),
            indexSearcher,
            keyRefs,
            keyExpressions,
            RamAccounting.NO_ACCOUNTING,
            null,
            null,
            new MatchAllDocsQuery(),
            new CollectorContext()
        );

        var rowConsumer = new TestingRowConsumer();
        rowConsumer.accept(it, null);

        assertThat(
            rowConsumer.getResult(),
            containsInAnyOrder(new Object[]{"0", 0L, 6L}, new Object[]{"1", 1L, 4L})
        );
    }

    @Test
    public void test_optimized_iterator_stop_processing_on_kill() throws Exception {
        Throwable expectedException = stopOnInterrupting(it -> it.kill(new InterruptedException("killed")));
        assertThat(expectedException, instanceOf(InterruptedException.class));
    }

    @Test
    public void test_optimized_iterator_stop_processing_on_close() throws Exception {
        Throwable expectedException = stopOnInterrupting(BatchIterator::close);
        assertThat(expectedException, instanceOf(IllegalStateException.class));
    }

    private Throwable stopOnInterrupting(Consumer<BatchIterator<Row>> interrupt) throws Exception {
        CountDownLatch waitForLoadNextBatch = new CountDownLatch(1);
        CountDownLatch pauseOnDocumentCollecting = new CountDownLatch(1);
        CountDownLatch batchLoadingCompleted = new CountDownLatch(1);

        BatchIterator<Row> it = createBatchIterator(() -> {
            waitForLoadNextBatch.countDown();
            try {
                pauseOnDocumentCollecting.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        AtomicReference<Throwable> exception = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                it.loadNextBatch().whenComplete((r, e) -> {
                    if (e != null) {
                        exception.set(e.getCause());
                    }
                    batchLoadingCompleted.countDown();
                });
            } catch (Exception e) {
                exception.set(e);
            }
        });
        t.start();
        waitForLoadNextBatch.await(5, TimeUnit.SECONDS);
        interrupt.accept(it);
        pauseOnDocumentCollecting.countDown();
        batchLoadingCompleted.await(5, TimeUnit.SECONDS);
        return exception.get();
    }

    private BatchIterator<Row> createBatchIterator(Runnable onNextReader) {
        return DocValuesGroupByOptimizedIterator.GroupByIterator.getIterator(
            List.of(),
            indexSearcher,
            List.of(new LuceneCollectorExpression<>() {

                @Override
                public void setNextReader(ReaderContext context) throws IOException {
                    onNextReader.run();
                }

                @Override
                public Object value() {
                    return null;
                }
            }),
            null,
            null,
            null,
            (states, key) -> {
            },
            (expressions) -> expressions.get(0).value(),
            (key, cells) -> cells[0] = key,
            new MatchAllDocsQuery(),
            new CollectorContext()
        );
    }
}
