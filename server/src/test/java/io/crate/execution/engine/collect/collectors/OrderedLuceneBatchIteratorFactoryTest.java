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

package io.crate.execution.engine.collect.collectors;

import static io.crate.testing.TestingHelpers.createReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.crate.analyze.OrderBy;
import io.crate.breaker.TypedRowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;
import io.crate.types.LongType;

public class OrderedLuceneBatchIteratorFactoryTest extends ESTestCase {

    private static final TypedRowAccounting ROW_ACCOUNTING = new TypedRowAccounting(
        List.of(LongType.INSTANCE),
        RamAccounting.NO_ACCOUNTING
    );

    private final String columnName = "x";
    private final Reference reference = createReference(columnName, DataTypes.LONG);
    private IndexSearcher searcher1;
    private IndexSearcher searcher2;
    private OrderBy orderBy;
    private List<Object[]> expectedResult;
    private final boolean[] reverseFlags = new boolean[]{true};
    private final boolean[] nullsFirst = new boolean[]{true};

    @Mock
    public RowAccounting<Row> rowAccounting;

    @Before
    public void prepareSearchers() throws Exception {
        MockitoAnnotations.openMocks(this);

        IndexWriter iw1 = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        IndexWriter iw2 = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));

        expectedResult = LongStream.range(0, 20)
            .mapToObj(i -> new Object[]{i})
            .collect(Collectors.toList());
        // expect descending order to differentiate between insert order
        expectedResult.sort(Comparator.comparingLong((Object[] o) -> ((long) o[0])).reversed());

        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(columnName, i));
            if (i % 2 == 0) {
                iw1.addDocument(doc);
            } else {
                iw2.addDocument(doc);
            }
        }
        iw1.commit();
        iw2.commit();

        searcher1 = new IndexSearcher(DirectoryReader.open(iw1));
        searcher2 = new IndexSearcher(DirectoryReader.open(iw2));
        orderBy = new OrderBy(
            Collections.singletonList(reference),
            reverseFlags,
            nullsFirst
        );
    }

    @Test
    public void testOrderedLuceneBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> {
                LuceneOrderedDocCollector collector1 = createOrderedCollector(searcher1, 1);
                LuceneOrderedDocCollector collector2 = createOrderedCollector(searcher2, 2);
                return OrderedLuceneBatchIteratorFactory.newInstance(
                    Arrays.asList(collector1, collector2),
                    OrderingByPosition.rowOrdering(List.of(DataTypes.LONG), new int[]{0}, reverseFlags, nullsFirst),
                    ROW_ACCOUNTING,
                    Runnable::run,
                    () -> 1,
                    true
                );
            }, ResultOrder.EXACT
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testSingleCollectorOrderedLuceneBatchIteratorTripsCircuitBreaker() throws Exception {
        CircuitBreakingException circuitBreakingException = new CircuitBreakingException("tripped circuit breaker");
        doThrow(circuitBreakingException)
            .when(rowAccounting).accountForAndMaybeBreak(any(Row.class));

        BatchIterator<Row> rowBatchIterator = OrderedLuceneBatchIteratorFactory.newInstance(
            List.of(createOrderedCollector(searcher1, 1)),
            OrderingByPosition.rowOrdering(List.of(DataTypes.INTEGER), new int[]{0}, reverseFlags, nullsFirst),
            rowAccounting,
            Runnable::run,
            () -> 2,
            true
        );

        consumeIteratorAndVerifyResultIsException(rowBatchIterator, circuitBreakingException);
    }

    @Test
    public void testOrderedLuceneBatchIteratorWithMultipleCollectorsTripsCircuitBreaker() throws Exception {
        CircuitBreakingException circuitBreakingException = new CircuitBreakingException("tripped circuit breaker");
        doThrow(circuitBreakingException)
            .when(rowAccounting).accountForAndMaybeBreak(any(Row.class));

        LuceneOrderedDocCollector collector1 = createOrderedCollector(searcher1, 1);
        LuceneOrderedDocCollector collector2 = createOrderedCollector(searcher2, 2);
        BatchIterator<Row> rowBatchIterator = OrderedLuceneBatchIteratorFactory.newInstance(
            Arrays.asList(collector1, collector2),
            OrderingByPosition.rowOrdering(List.of(DataTypes.INTEGER), new int[]{0}, reverseFlags, nullsFirst),
            rowAccounting,
            Runnable::run,
            () -> 1,
            true
        );

        consumeIteratorAndVerifyResultIsException(rowBatchIterator, circuitBreakingException);
    }

    @Test
    public void test_ensure_lucene_ordered_collector_propagates_kill() throws Exception {
        LuceneOrderedDocCollector luceneOrderedDocCollector = createOrderedCollector(searcher1, 1);
        AtomicReference<Thread> collectThread = new AtomicReference<>();
        CountDownLatch threadStarted = new CountDownLatch(1);

        // defers the real runnable until after the test case has asserted that the thread is active
        CountDownLatch triggerRunnable = new CountDownLatch(1);
        BatchIterator<Row> rowBatchIterator = OrderedLuceneBatchIteratorFactory.newInstance(
            Collections.singletonList(luceneOrderedDocCollector),
            OrderingByPosition.rowOrdering(List.of(DataTypes.INTEGER), new int[]{0}, reverseFlags, nullsFirst),
            rowAccounting,
            runnable -> {
                var t = new Thread(() -> {
                    threadStarted.countDown();
                    try {
                        triggerRunnable.await(1, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    runnable.run();
                });
                collectThread.set(t);
                t.start();
            },
            () -> 1,
            true
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(rowBatchIterator, null);

        try {
            threadStarted.await(1, TimeUnit.SECONDS);
            assertThat(collectThread.get().isAlive()).isTrue();
        } finally {
            triggerRunnable.countDown();
        }

        rowBatchIterator.kill(new InterruptedException("killed"));

        assertThatThrownBy(consumer::getResult)
            .satisfiesAnyOf(
                x -> assertThat(x)
                    .hasRootCauseInstanceOf(InterruptedException.class)
                    .hasRootCauseMessage("killed"),
                x -> assertThat(x)
                    .isExactlyInstanceOf(InterruptedException.class)
                    .hasMessage("killed")
            );
    }

    private void consumeIteratorAndVerifyResultIsException(BatchIterator<Row> rowBatchIterator, Exception exception)
        throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(rowBatchIterator, null);

        assertThatThrownBy(consumer::getResult)
            .isExactlyInstanceOf(exception.getClass())
            .hasMessage(exception.getMessage());
    }

    private LuceneOrderedDocCollector createOrderedCollector(IndexSearcher searcher, int shardId) {
        CollectorContext collectorContext = new CollectorContext(() -> null);
        List<LuceneCollectorExpression<?>> expressions = Collections.singletonList(
            new OrderByCollectorExpression(reference, orderBy, o -> o));
        return new LuceneOrderedDocCollector(
            new ShardId("dummy", UUIDs.randomBase64UUID(), shardId),
            searcher,
            new MatchAllDocsQuery(),
            null,
            false,
            5, // batchSize < 10 to have at least one searchMore call.
            RamAccounting.NO_ACCOUNTING,
            collectorContext,
            f -> null,
            new Sort(new SortedNumericSortField(columnName, SortField.Type.LONG, reverseFlags[0])),
            expressions,
            expressions
        );
    }
}
