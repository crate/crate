/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.execution.engine.distribution.merge.SortedPagingIterator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.testing.TestingRowConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DistResultRXTaskTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private DistResultRXTask getPageDownstreamContext(TestingRowConsumer batchConsumer,
                                                      PagingIterator<Integer, Row> pagingIterator,
                                                      int numBuckets) {
        return new DistResultRXTask(
            Loggers.getLogger(DistResultRXTask.class),
            "n1",
            1,
            "dummy",
            MoreExecutors.directExecutor(),
            batchConsumer,
            pagingIterator,
            new Streamer[1],
            RAM_ACCOUNTING_CONTEXT,
            BucketReceiverFactory.Type.MERGE_BUCKETS,
            numBuckets
        );
    }

    @Test
    public void testCantSetSameBucketTwiceWithoutReceivingFullPage() throws Throwable {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        PageBucketReceiver ctx = getPageDownstreamContext(batchConsumer, PassThroughPagingIterator.oneShot(), 3).getBucketReceiver((byte) 0);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        ctx.setBucket(1, bucket, false, pageResultListener);
        ctx.setBucket(1, bucket, false, pageResultListener);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Same bucket of a page set more than once. node=n1 method=setBucket phaseId=1 bucket=1");
        batchConsumer.getResult();
    }

    @Test
    public void testKillCallsDownstream() throws Throwable {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(batchConsumer, PassThroughPagingIterator.oneShot(), 3);

        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        ctx.completionFuture().whenComplete((r, t) -> {
            if (t != null) {
                assertTrue(throwable.compareAndSet(null, t));
            } else {
                fail("Expected exception");
            }
        });

        ctx.kill(new InterruptedException());
        assertThat(throwable.get(), instanceOf(InterruptedException.class));

        expectedException.expect(InterruptedException.class);
        batchConsumer.getResult();
    }

    @Test
    public void testPagingWithSortedPagingIterator() throws Throwable {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(
            batchConsumer,
            new SortedPagingIterator<>(Comparator.comparingInt(r -> (int)r.get(0)), false),
            2
        );

        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);

        Bucket b1 = new ArrayBucket(new Object[][]{
            new Object[]{1},
            new Object[]{1},
        });
        Bucket b11 = new ArrayBucket(new Object[][]{
            new Object[]{2},
            new Object[]{2},
        });
        bucketReceiver.setBucket(0, b1, false, new PageResultListener() {
            @Override
            public void needMore(boolean needMore) {
                if (needMore) {
                    bucketReceiver.setBucket(0, b11, true, mock(PageResultListener.class));
                }
            }
        });
        Bucket b2 = new ArrayBucket(new Object[][] {
            new Object[] { 4 }
        });
        bucketReceiver.setBucket(1, b2, true, mock(PageResultListener.class));


        List<Object[]> result = batchConsumer.getResult();
        assertThat(TestingHelpers.printedTable(new CollectionBucket(result)),
            is("1\n" +
               "1\n" +
               "2\n" +
               "2\n" +
               "4\n"));
    }

    @Test
    public void testListenersCalledWhenOtherUpstreamIsFailing() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(consumer, PassThroughPagingIterator.oneShot(), 2);

        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        PageResultListener listener = mock(PageResultListener.class);
        bucketReceiver.setBucket(0, Bucket.EMPTY, false, listener);
        bucketReceiver.failure(1, new Exception("dummy"));

        verify(listener, times(1)).needMore(false);
    }

    @Test
    public void testListenerCalledAfterOthersHasFailed() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(consumer, PassThroughPagingIterator.oneShot(), 2);

        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        bucketReceiver.failure(0, new Exception("dummy"));
        PageResultListener listener = mock(PageResultListener.class);
        bucketReceiver.setBucket(1, Bucket.EMPTY, true, listener);

        verify(listener, times(1)).needMore(false);
    }

    @Test
    public void testSetBucketOnAKilledCtxReleasesListener() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(consumer, PassThroughPagingIterator.oneShot(), 2);
        ctx.kill(new InterruptedException("killed"));

        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        CompletableFuture<Void> listenerReleased = new CompletableFuture<>();
        bucketReceiver.setBucket(0, Bucket.EMPTY, false, needMore -> listenerReleased.complete(null));

        // Must not timeout
        listenerReleased.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testNonSequentialBucketIds() throws Exception {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(
            batchConsumer,
            PassThroughPagingIterator.oneShot(),
            3
        );
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);

        final PageResultListener mockListener = mock(PageResultListener.class);

        Bucket b1 = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        bucketReceiver.setBucket(0, b1, true, mockListener);

        Bucket b2 = new CollectionBucket(Collections.singletonList(new Object[] { "bar" }));
        bucketReceiver.setBucket(3, b2, true, mockListener);

        Bucket b3 = new CollectionBucket(Collections.singletonList(new Object[] { "universe" }));
        CheckPageResultListener checkPageResultListener = new CheckPageResultListener();
        bucketReceiver.setBucket(42, b3, false, checkPageResultListener);
        assertThat(checkPageResultListener.needMoreResult, is(true));
        bucketReceiver.setBucket(42, b3, true, checkPageResultListener);
        assertThat(checkPageResultListener.needMoreResult, is(false));

        List<Object[]> result = batchConsumer.getResult();
        assertThat(result.toArray(), arrayContainingInAnyOrder(
            new Object[] {"foo"},
            new Object[] {"bar"},
            new Object[] {"universe"},
            new Object[] {"universe"}
        ));
    }

    @Test
    public void testBatchIteratorIsCompletedExceptionallyIfMergeBucketFails() throws Exception {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        PageBucketReceiver ctx = getPageDownstreamContext(batchConsumer, new FailOnMergePagingIterator<>(2), 2).getBucketReceiver((byte) 0);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        ctx.setBucket(0, bucket, false, pageResultListener);
        ctx.setBucket(1, bucket, false, pageResultListener);
        ctx.setBucket(0, bucket, true, pageResultListener);
        ctx.setBucket(1, bucket, true, pageResultListener);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("raised on merge");
        batchConsumer.getResult();
    }

    private static class CheckPageResultListener implements PageResultListener {

        private boolean needMoreResult;

        @Override
        public void needMore(boolean needMore) {
            needMoreResult = needMore;
        }
    }

    private static class FailOnMergePagingIterator<TKey, TRow> extends ForwardingIterator<TRow> implements PagingIterator<TKey, TRow> {

        private Iterator<TRow> iterator = Collections.emptyIterator();
        private final ImmutableList.Builder<KeyIterable<TKey, TRow>> iterables = ImmutableList.builder();
        private final int mergesCallCountUntilError;
        private int mergesCallCount = 0;

        public FailOnMergePagingIterator(int mergesCallCountUntilError) {
            this.mergesCallCountUntilError = mergesCallCountUntilError;
        }

        @Override
        protected Iterator<TRow> delegate() {
            return iterator;
        }

        @Override
        public void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
            if (++mergesCallCount == mergesCallCountUntilError) {
                throw new RuntimeException("raised on merge");
            }
            Iterable<TRow> concat = Iterables.concat(iterables);

            if (iterator.hasNext()) {
                iterator = Iterators.concat(iterator, concat.iterator());
            } else {
                iterator = concat.iterator();
            }

        }

        @Override
        public void finish() {

        }

        @Override
        public TKey exhaustedIterable() {
            return null;
        }

        @Override
        public Iterable<TRow> repeat() {
            return null;
        }
    }
}
