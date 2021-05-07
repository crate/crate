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

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.AggregationContext;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.NodeContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.BatchIteratorTester;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GroupByOptimizedIteratorTest extends CrateDummyClusterServiceUnitTest {

    private IndexSearcher indexSearcher;
    private ArrayList<Object[]> expectedResult;
    private String columnName;
    private InputCollectExpression inExpr;
    private List<AggregationContext> aggregationContexts;

    @Before
    public void prepare() throws Exception {
        NodeContext nodeCtx = createNodeContext();
        IndexWriter iw = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        columnName = "x";
        expectedResult = new ArrayList<>(20);
        for (long i = 0; i < 20; i++) {
            Document doc = new Document();
            String val = "val_" + i;
            doc.add(new SortedSetDocValuesField(columnName, new BytesRef(val)));
            iw.addDocument(doc);
            expectedResult.add(new Object[] { val, 1L });
        }
        iw.commit();
        indexSearcher = new IndexSearcher(DirectoryReader.open(iw));

        inExpr = new InputCollectExpression(0);
        CountAggregation aggregation = (CountAggregation) nodeCtx.functions().getQualified(
            CountAggregation.COUNT_STAR_SIGNATURE,
            Collections.emptyList(),
            CountAggregation.COUNT_STAR_SIGNATURE.getReturnType().createType()
        );
        aggregationContexts = List.of(new AggregationContext(aggregation, () -> true, List.of()));
    }

    private BatchIterator<Row> createBatchIterator(Runnable onNextReader) {
        return GroupByOptimizedIterator.getIterator(
            BigArrays.NON_RECYCLING_INSTANCE,
            indexSearcher,
            columnName,
            aggregationContexts,
            List.of(new LuceneCollectorExpression<Object>() {

                @Override
                public void setNextReader(ReaderContext context) throws IOException {
                    onNextReader.run();
                }

                @Override
                public Object value() {
                    return null;
                }
            }),
            Collections.singletonList(inExpr),
            RamAccounting.NO_ACCOUNTING,
            new OnHeapMemoryManager(usedBytes -> {}),
            Version.CURRENT,
            new InputRow(Collections.singletonList(inExpr)),
            new MatchAllDocsQuery(),
            new CollectorContext(),
            AggregateMode.ITER_FINAL
        );
    }

    @Test
    public void testHighCardinalityRatioReturnsTrueForHighCardinality() throws Exception {
        IndexWriter iw = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        String columnName = "x";
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            BytesRef value = new BytesRef(Integer.toString(i));
            doc.add(new Field(columnName, value, KeywordFieldMapper.Defaults.FIELD_TYPE.clone()));
            iw.addDocument(doc);
        }
        iw.commit();

        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(iw));

        assertThat(
            GroupByOptimizedIterator.hasHighCardinalityRatio(
                () -> new Engine.Searcher(
                    "dummy",
                    indexSearcher.getIndexReader(),
                    indexSearcher.getQueryCache(),
                    indexSearcher.getQueryCachingPolicy(),
                    () -> {}
                ),
                "x"
            ),
            is(true)
        );
    }

    @Test
    public void testHighCardinalityRatioReturnsTrueForLowCardinality() throws Exception {
        IndexWriter iw = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        String columnName = "x";
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            BytesRef value = new BytesRef("1");
            doc.add(new Field(columnName, value, KeywordFieldMapper.Defaults.FIELD_TYPE.clone()));
            iw.addDocument(doc);
        }
        iw.commit();

        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(iw));

        assertThat(
            GroupByOptimizedIterator.hasHighCardinalityRatio(
                () -> new Engine.Searcher(
                    "dummy",
                    indexSearcher.getIndexReader(),
                    indexSearcher.getQueryCache(),
                    indexSearcher.getQueryCachingPolicy(),
                    () -> {}
                ),
                "x"
            ),
            is(false)
        );
    }

    @Test
    public void test_optimized_iterator_behaviour() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(() -> createBatchIterator(() -> {}));
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
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
}
