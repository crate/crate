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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.data.*;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class IteratorPageDownstreamTest extends CrateUnitTest {

    @Mock
    public PageConsumeListener pageConsumeListener;

    @Mock
    PagingIterator<Void, Row> mockedPagingIterator;

    @Test
    public void testMergeOnPagingIteratorIsCalledAfterALLBucketsAreReady() throws Exception {
        IteratorPageDownstream downstream = new IteratorPageDownstream(
            new CollectingRowReceiver(), mockedPagingIterator, Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        SettableFuture<Bucket> b2 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1, b2)), pageConsumeListener);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends KeyIterable<Void, Row>>>any());
        b1.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends KeyIterable<Void, Row>>>any());
        b2.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(1)).merge(Mockito.<Iterable<? extends KeyIterable<Void, Row>>>any());
    }

    @Test
    public void testFailingNextRowIsHandled() throws Exception {
        expectedException.expect(CircuitBreakingException.class);

        class FailingRowReceiver extends CollectingRowReceiver {
            @Override
            public Result setNextRow(Row row) {
                throw new CircuitBreakingException("foo");
            }
        }

        CollectingRowReceiver rowReceiver = new FailingRowReceiver();
        IteratorPageDownstream downstream = new IteratorPageDownstream(
            rowReceiver, PassThroughPagingIterator.<Void, Row>oneShot(), Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
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
        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
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

        verify(rowReceiver, times(1)).finish(any(RepeatHandle.class));
    }

    @Test
    public void testFinishDoesNotEmitRemainingRow() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withLimit(1);
        IteratorPageDownstream downstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>oneShot(),
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

        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
        downstream.finish();
        assertThat(rowReceiver.result().size(), is(1));
    }

    @Test
    public void testRejectedExecutionDoesNotCauseBucketMergerToGetStuck() throws Exception {
        expectedException.expect(EsRejectedExecutionException.class);
        final CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>oneShot(),
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
                rowReceiver.finish(RepeatHandle.UNSUPPORTED);
            }

            @Override
            public void finish() {
                rowReceiver.finish(RepeatHandle.UNSUPPORTED);
            }
        });
        rowReceiver.result();
    }

    @Test
    public void testRepeat() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>repeatable(),
            Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket(
            new Object[][]{
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"c"}
            }
        ));
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
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
    public void testFailOnRepeatResultsInFailure() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withFailureOnRepeat(); // this one fails on 1st repeat
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>repeatable(),
            Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket($$($("a"))));
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
        pageDownstream.finish();
        rowReceiver.repeatUpstream();

        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(2));
    }

    @Test
    public void testPauseAndResume() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(2);
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>repeatable(),
            Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        b1.set(new ArrayBucket(
            new Object[][]{
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"c"}
            }
        ));
        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1)), pageConsumeListener);
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

    @Test
    public void testPauseDuringConsumeRemaining() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(3);
        SortedPagingIterator<Void, Row> pagingIterator =
            new SortedPagingIterator<>(OrderingByPosition.rowOrdering(0, false, null), false);
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            pagingIterator,
            Optional.<Executor>absent());

        ListenableFuture<Bucket> b1 = Futures.<Bucket>immediateFuture((new ArrayBucket(
            new Object[][]{
                new Object[]{"a"},
            })));
        ListenableFuture<Bucket> b2 = Futures.<Bucket>immediateFuture((new ArrayBucket(
            new Object[][]{
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"b"},
            })));

        pageDownstream.nextPage(new BucketPage(ImmutableList.of(b1, b2)), pageConsumeListener);
        assertThat(rowReceiver.rows.size(), is(1));
        // pageDownstream calls needMore after first row - it is holding back the rows of the second bucket
        // because there could be a second page for the first bucket with more rows
        verify(pageConsumeListener, times(1)).needMore();
        pageDownstream.finish(); // causes pageDownstream to emit the rows it had been holding back.

        assertThat(rowReceiver.rows.size(), is(3));

        rowReceiver.resumeUpstream(false);
        assertThat(rowReceiver.rows.size(), is(4));
        verify(pageConsumeListener, times(1)).needMore();
        verify(pageConsumeListener, times(0)).finish();
    }

    @Test
    public void testKillCallsRowReceiver() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
            rowReceiver,
            PassThroughPagingIterator.<Void, Row>repeatable(),
            Optional.<Executor>absent());

        pageDownstream.kill(null);

        expectedException.expect(CancellationException.class);
        rowReceiver.result(TimeValue.timeValueSeconds(5));
    }
}
