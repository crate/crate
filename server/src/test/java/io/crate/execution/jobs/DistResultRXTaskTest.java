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

package io.crate.execution.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.common.collections.Iterables;
import io.crate.common.collections.Iterators;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.testing.TestingHelpers;

public class DistResultRXTaskTest extends ESTestCase {

    private DistResultRXTask getPageDownstreamContext(TestingRowConsumer batchConsumer,
                                                      PagingIterator<Integer, Row> pagingIterator,
                                                      int numBuckets) {

        PageBucketReceiver pageBucketReceiver = new CumulativePageBucketReceiver(
            "n1",
            1,
            Runnable::run,
            new Streamer[1],
            batchConsumer,
            pagingIterator,
            numBuckets);

        return new DistResultRXTask(
            1,
            "dummy",
            pageBucketReceiver,
            RamAccounting.NO_ACCOUNTING,
            numBuckets
        );
    }

    @Test
    public void testCantSetSameBucketTwiceWithoutReceivingFullPage() throws Throwable {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        DistResultRXTask ctx = getPageDownstreamContext(batchConsumer, PassThroughPagingIterator.oneShot(), 3);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();
        bucketReceiver.setBucket(1, bucket, false, pageResultListener);
        bucketReceiver.setBucket(1, bucket, false, pageResultListener);

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
        assertThat(throwable.get()).isExactlyInstanceOf(CompletionException.class);

        expectedException.expect(InterruptedException.class);
        batchConsumer.getResult();
    }

    @Test
    public void testPagingWithSortedPagingIterator() throws Throwable {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(
            batchConsumer,
            PagingIterator.createSorted(Comparator.comparingInt(r -> (int)r.get(0)), false),
            2
        );

        Bucket b1 = new ArrayBucket(new Object[][]{
            new Object[]{1},
            new Object[]{1},
        });
        Bucket b11 = new ArrayBucket(new Object[][]{
            new Object[]{2},
            new Object[]{2},
        });
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();
        bucketReceiver.setBucket(0, b1, false, needMore -> {
            if (needMore) {
                bucketReceiver.setBucket(0, b11, true, mock(PageResultListener.class));
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

        PageResultListener listener = mock(PageResultListener.class);
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();
        bucketReceiver.setBucket(0, Bucket.EMPTY, false, listener);
        bucketReceiver.kill(new Exception("dummy"));

        verify(listener, times(1)).needMore(false);
    }

    @Test
    public void testListenerCalledAfterOthersHasFailed() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(consumer, PassThroughPagingIterator.oneShot(), 2);
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();

        bucketReceiver.kill(new Exception("dummy"));
        PageResultListener listener = mock(PageResultListener.class);
        bucketReceiver.setBucket(1, Bucket.EMPTY, true, listener);

        verify(listener, times(1)).needMore(false);
    }

    @Test
    public void testSetBucketOnAKilledCtxReleasesListener() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        DistResultRXTask ctx = getPageDownstreamContext(consumer, PassThroughPagingIterator.oneShot(), 2);
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();
        ctx.kill(new InterruptedException("killed"));

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
        assertThat(bucketReceiver).isNotNull();

        final PageResultListener mockListener = mock(PageResultListener.class);

        Bucket b1 = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        bucketReceiver.setBucket(0, b1, true, mockListener);

        Bucket b2 = new CollectionBucket(Collections.singletonList(new Object[] { "bar" }));
        bucketReceiver.setBucket(3, b2, true, mockListener);

        Bucket b3 = new CollectionBucket(Collections.singletonList(new Object[] { "universe" }));
        CheckPageResultListener checkPageResultListener = new CheckPageResultListener();
        bucketReceiver.setBucket(42, b3, false, checkPageResultListener);
        assertThat(checkPageResultListener.needMoreResult).isTrue();
        bucketReceiver.setBucket(42, b3, true, checkPageResultListener);
        assertThat(checkPageResultListener.needMoreResult).isFalse();

        List<Object[]> result = batchConsumer.getResult();
        assertThat(result.toArray(), arrayContainingInAnyOrder(
            new Object[] {"foo"},
            new Object[] {"bar"},
            new Object[] {"universe"},
            new Object[] {"universe"}
        ));
    }

    @Test
    public void test_batch_iterator_is_completed_exceptionally_if_merge_buckets_on_next_page_fails() throws Exception {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        DistResultRXTask ctx = getPageDownstreamContext(batchConsumer, new FailOnMergePagingIterator<>(2), 2);
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        bucketReceiver.setBucket(0, bucket, false, pageResultListener);
        bucketReceiver.setBucket(1, bucket, false, pageResultListener);
        bucketReceiver.setBucket(0, bucket, true, pageResultListener);
        bucketReceiver.setBucket(1, bucket, true, pageResultListener);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("raised on merge");
        batchConsumer.getResult();
    }

    @Test
    public void test_batch_iterator_is_completed_exceptionally_if_first_merge_buckets_fails() throws Exception {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        DistResultRXTask ctx = getPageDownstreamContext(batchConsumer, new FailOnMergePagingIterator<>(1), 1);
        PageBucketReceiver bucketReceiver = ctx.getBucketReceiver((byte) 0);
        assertThat(bucketReceiver).isNotNull();

        PageResultListener pageResultListener = mock(PageResultListener.class);
        Bucket bucket = new CollectionBucket(Collections.singletonList(new Object[] { "foo" }));
        bucketReceiver.setBucket(0, bucket, true, pageResultListener);

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

    private static class FailOnMergePagingIterator<TKey, TRow> implements PagingIterator<TKey, TRow> {

        private Iterator<TRow> iterator = Collections.emptyIterator();
        private final List<KeyIterable<TKey, TRow>> iterables = new ArrayList<>();
        private final int mergesCallCountUntilError;
        private int mergesCallCount = 0;

        public FailOnMergePagingIterator(int mergesCallCountUntilError) {
            this.mergesCallCountUntilError = mergesCallCountUntilError;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public TRow next() {
            return iterator.next();
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
