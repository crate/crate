/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.merge;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.core.collections.SingleRowBucket;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.projectors.RowReceiver;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class IteratorPageDownstreamTest extends CrateUnitTest {

    public static final PageConsumeListener PAGE_CONSUME_LISTENER = new PageConsumeListener() {
        @Override
        public void needMore() {

        }

        @Override
        public void finish() {

        }
    };

    @Mock
    PagingIterator<Row> mockedPagingIterator;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMergeOnPagingIteratorIsCalledAfterALLBucketsAreReady() throws Exception {
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                new CollectingRowReceiver(), mockedPagingIterator, Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        SettableFuture<Bucket> b2 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1, b2)), PAGE_CONSUME_LISTENER);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends NumberedIterable<Row>>>any());
        b1.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends NumberedIterable<Row>>>any());
        b2.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(1)).merge(Mockito.<Iterable<? extends NumberedIterable<Row>>>any());
    }

    @Test
    public void testFailingNextRowIsHandled() throws Exception {
        expectedException.expect(CircuitBreakingException.class);

        class FailingRowReceiver extends CollectingRowReceiver {
            @Override
            public boolean setNextRow(Row row) {
                throw new CircuitBreakingException("foo");
            }
        }

        CollectingRowReceiver rowReceiver = new FailingRowReceiver();
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                rowReceiver, PassThroughPagingIterator.<Row>oneShot(), Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        b1.set(new SingleRowBucket(new Row1(42)));
        rowReceiver.result();
    }

    @Test
    public void testBucketFailureIsPassedToDownstream() throws Exception {
        IllegalStateException dummy = new IllegalStateException("dummy");
        expectedException.expect(IllegalStateException.class);

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                rowReceiver, mockedPagingIterator, Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        b1.setException(dummy);
        rowReceiver.result();
    }

    @Test
    public void testMultipleFinishPropagatesOnlyOnceToDownstream() throws Exception {
        RowReceiver rowReceiver = mock(RowReceiver.class);
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                rowReceiver, mockedPagingIterator, Optional.<Executor>absent());

        downstream.finish();
        downstream.finish();

        verify(rowReceiver, times(1)).finish();
    }

    @Test
    public void testFinishDoesNotEmitRemainingRow() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withLimit(1);
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                rowReceiver,
                PassThroughPagingIterator.<Row>oneShot(),
                Optional.<Executor>absent()
        );

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket(
                new Object[][]{
                        new Object[]{"a"},
                        new Object[]{"b"},
                        new Object[]{"c"}
                }
        ));

        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        downstream.finish();
        assertThat(rowReceiver.result().size(), is(1));
    }

    @Test
    public void testRejectedExecutionDoesNotCauseBucketMergerToGetStuck() throws Exception {
         expectedException.expect(EsRejectedExecutionException.class);
         final CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

         IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                 rowReceiver,
                 PassThroughPagingIterator.<Row>oneShot(),
                 Optional.<Executor>of(new Executor() {
                     @Override
                     public void execute(@Nonnull Runnable command) {
                         throw new EsRejectedExecutionException("HAHA !");
                     }
                 }));

         SettableFuture<Bucket> b1 = SettableFuture.create();
         b1.set(Bucket.EMPTY);
         pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), new PageConsumeListener() {
             @Override
             public void needMore() {
                 rowReceiver.finish();
             }

             @Override
             public void finish() {
                 rowReceiver.finish();
             }
         });
         rowReceiver.result();
    }

    @Test
    public void testRepeat() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                rowReceiver,
                PassThroughPagingIterator.<Row>repeatable(),
                Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket(
                new Object[][] {
                        new Object[] {"a"},
                        new Object[] {"b"},
                        new Object[] {"c"}
                }
        ));
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        pageDownstream.finish();
        rowReceiver.repeatUpstream();
        pageDownstream.finish();
        assertThat(TestingHelpers.printedTable(rowReceiver.result()), is(
                "a\n" +
                "b\n" +
                "c\n" +
                "a\n" +
                "b\n" +
                "c\n" +
                "a\n" +
                "b\n" +
                "c\n" +
                "a\n" +
                "b\n" +
                "c\n"));
    }

    @Test
    public void testPauseAndResume() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(2);
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                rowReceiver,
                PassThroughPagingIterator.<Row>repeatable(),
                Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket(
                new Object[][] {
                        new Object[] {"a"},
                        new Object[] {"b"},
                        new Object[] {"c"}
                }
        ));
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        assertThat(rowReceiver.rows.size(), is(2));
        rowReceiver.resumeUpstream(false);
        assertThat(rowReceiver.rows.size(), is(3));

        pageDownstream.finish();
        rowReceiver.repeatUpstream();

        assertThat(TestingHelpers.printedTable(rowReceiver.result()), is(
                "a\n" +
                "b\n" +
                "c\n" +
                "a\n" +
                "b\n" +
                "c\n"));
    }
}