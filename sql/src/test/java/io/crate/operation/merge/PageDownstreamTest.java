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

package io.crate.operation.merge;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.*;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PageDownstreamTest {

    public static interface PageDownstreamBuilder <T extends PageDownstream> {

        T create(RowDownstream downstream);
    }

    public static <T extends PageDownstream> void verifyNoSetNextRowAfterFinished(PageDownstreamBuilder<T> builder) throws Throwable{
        final AtomicReference<Throwable> t = new AtomicReference();
        final T pageDownstream = builder.create(new RowDownstream() {
            @Override
            public RowDownstreamHandle registerUpstream(RowUpstream upstream) {

                return new RowDownstreamHandle() {

                    private boolean finished = false;

                    @Override
                    public boolean setNextRow(Row row) {
                        if (finished) {
                            t.set(new AssertionError("setNextRow called after finish"));
                        }
                        return true;
                    }

                    @Override
                    public void finish() {
                        finished = true;
                    }

                    @Override
                    public void fail(Throwable throwable) {
                    }
                };
            }

        });
        final SettableFuture<Bucket> bucketFuture1 = SettableFuture.create();
        bucketFuture1.set(new Bucket() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public Iterator<Row> iterator() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    t.set(e);
                }
                return new ArrayBucket(new Object[][]{{ "A" }, { "B" }}).iterator();
            }
        });
        final PageConsumeListener listener = mock(PageConsumeListener.class);
        Thread mergeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                pageDownstream.nextPage(new BucketPage(bucketFuture1), listener);
            }
        });
        mergeThread.start();
        pageDownstream.finish();
        mergeThread.join();
        Throwable throwable = t.get();
        if(throwable != null) {
            throw throwable;
        }
        verify(listener, times(1)).finish();
    }


    @Test
    public void testFinishNonSortingBucketMerger() throws Throwable {
        verifyNoSetNextRowAfterFinished(new PageDownstreamBuilder<NonSortingBucketMerger>() {
            @Override
            public NonSortingBucketMerger create(RowDownstream downstream) {
                return new NonSortingBucketMerger(downstream);
            }
        });
    }


    @Test
    public void testFinishSortingBucketMerger() throws Throwable {
        verifyNoSetNextRowAfterFinished(new PageDownstreamBuilder<PageDownstream>() {
            @Override
            public SortingBucketMerger create(RowDownstream downstream) {
                SortingBucketMerger sortingBucketMerger =
                        new SortingBucketMerger(1, new int[0], new boolean[0], new Boolean[0], Optional.<Executor>absent());
                sortingBucketMerger.downstream(downstream);
                return sortingBucketMerger;
            }
        });
    }
}
