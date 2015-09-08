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
import io.crate.core.collections.*;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import io.crate.testing.RowCollectionBucket;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

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
                new CollectingProjector(), mockedPagingIterator, Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        SettableFuture<Bucket> b2 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1, b2)), PAGE_CONSUME_LISTENER);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends Iterable<Row>>>any());
        b1.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(0)).merge(Mockito.<Iterable<? extends Iterable<Row>>>any());
        b2.set(Bucket.EMPTY);
        verify(mockedPagingIterator, times(1)).merge(Mockito.<Iterable<? extends Iterable<Row>>>any());
    }

    @Test
    public void testBucketFailureIsPassedToDownstream() throws Exception {
        IllegalStateException dummy = new IllegalStateException("dummy");
        expectedException.expectCause(equalTo(dummy));

        CollectingProjector collectingProjector = new CollectingProjector();
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                collectingProjector, mockedPagingIterator, Optional.<Executor>absent());

        SettableFuture<Bucket> b1 = SettableFuture.create();
        downstream.nextPage(new BucketPage(ImmutableList.of(b1)), PAGE_CONSUME_LISTENER);
        b1.setException(dummy);
        collectingProjector.result().get(20, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testMultipleFinishPropagatesOnlyOnceToDownstream() throws Exception {
        final RowDownstreamHandle handle = mock(RowDownstreamHandle.class);
        RowDownstream rowDownstream = new RowDownstream() {
            @Override
            public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
                return handle;
            }
        };
        IteratorPageDownstream downstream = new IteratorPageDownstream(
                rowDownstream, mockedPagingIterator, Optional.<Executor>absent());

        downstream.finish();
        downstream.finish();

        verify(handle, times(1)).finish();
    }

    @Test
    public void testRejectedExecutionDoesNotCauseBucketMergerToGetStuck() throws Exception {
         expectedException.expectCause(Matchers.<Throwable>instanceOf(EsRejectedExecutionException.class));
         final CollectingProjector rowDownstream = new CollectingProjector();

         IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                 rowDownstream,
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
                 rowDownstream.finish();
             }

             @Override
             public void finish() {
                 rowDownstream.finish();
             }
         });
         rowDownstream.result().get(20, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRepeat() throws Exception {
        CollectingProjector rowDownstream = new CollectingProjector();
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                rowDownstream,
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
        pageDownstream.repeat();
        pageDownstream.finish();
        assertThat(TestingHelpers.printedTable(rowDownstream.result().get(20, TimeUnit.MILLISECONDS)), is(
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
        final List<Row> collectedRows = new ArrayList<>();
        RowDownstream rowDownstream = new RowDownstream() {
            @Override
            public RowDownstreamHandle registerUpstream(final RowUpstream upstream) {
                return new RowDownstreamHandle() {

                    int rows = 0;

                    @Override
                    public boolean setNextRow(Row row) {
                        collectedRows.add(new RowN(row.materialize()));
                        rows++;
                        if (rows == 2) {
                            upstream.pause();
                            return true;
                        }
                        return true;
                    }

                    @Override
                    public void finish() {
                    }

                    @Override
                    public void fail(Throwable throwable) {
                    }
                };
            }
        };
        IteratorPageDownstream pageDownstream = new IteratorPageDownstream(
                rowDownstream,
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
        assertThat(collectedRows.size(), is(2));
        pageDownstream.resume(false);
        assertThat(collectedRows.size(), is(3));

        pageDownstream.finish();
        pageDownstream.repeat();

        assertThat(TestingHelpers.printedTable(new RowCollectionBucket(collectedRows)), is(
                "a\n" +
                "b\n" +
                "c\n" +
                "a\n" +
                "b\n" +
                "c\n"));


    }
}