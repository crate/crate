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

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.operation.Input;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static io.crate.testing.TestingHelpers.createPage;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class NonSortingBucketMergerTest extends CrateUnitTest {

    private Bucket mergeWith(BucketPage... pages)
            throws ExecutionException, InterruptedException {

        CollectingProjector collectingProjector = new CollectingProjector();
        final NonSortingBucketMerger merger = new NonSortingBucketMerger();
        merger.downstream(collectingProjector);
        final Iterator<BucketPage> pageIter = Iterators.forArray(pages);
        if (pageIter.hasNext()) {
            merger.nextPage(pageIter.next(), new PageConsumeListener() {
                @Override
                public void needMore() {
                    if (pageIter.hasNext()) {
                        merger.nextPage(pageIter.next(), this);
                    } else {
                        merger.finish();
                    }
                }

                @Override
                public void finish() {
                    merger.finish();
                }
            });
        } else {
            merger.finish();
        }
        return collectingProjector.result().get();
    }

    private void assertMerged(Bucket bucket, String ... rows) {
        assertThat(Arrays.asList(TestingHelpers.printedTable(bucket).split("\n")), containsInAnyOrder(rows));
    }

    @Test
    public void testMergeWithManyPages() throws Exception {
        BucketPage page1 = createPage(
                Arrays.asList(
                        new Object[]{"B"},
                        new Object[]{"B"}
                ),
                Arrays.<Object[]>asList(),
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"A"},
                        new Object[]{"C"}
                )
        );

        BucketPage page2 = createPage(
                Arrays.asList(
                        new Object[]{1},
                        new Object[]{2}
                ),
                Arrays.<Object[]>asList(),
                Arrays.asList(
                        new Object[]{3},
                        new Object[]{4},
                        new Object[]{42}
                )
        );
        Bucket merged = mergeWith(page1, page2);
        assertMerged(merged, "B", "B", "A", "A", "1", "2", "C", "42", "4", "3");
    }

    @Test
    public void testMergeWith3Buckets() throws Exception {
        BucketPage page1 = createPage(
                Arrays.asList(
                        new Object[]{"B"},
                        new Object[]{"B"}
                ),
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"C"}
                ),
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"A"},
                        new Object[]{"B"}
                )
        );
        Bucket bucket = mergeWith(page1);
        assertMerged(bucket, "A", "A", "B", "A", "B", "B", "C");
    }

    @Test
    public void testAllBucketsAreEmpty() throws Exception {
        BucketPage p1Buckets = createPage(
                Arrays.<Object[]>asList(),
                Arrays.<Object[]>asList()
        );
        Bucket bucket = mergeWith(p1Buckets);
        Assert.assertThat(bucket.size(), is(0));
    }

    @Test
    public void testWithOnlyOneBucket() throws Exception {
        BucketPage p1Buckets = createPage(
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"B"},
                        new Object[]{"C"}));
        BucketPage p2Buckets = createPage(
                Arrays.asList(
                        new Object[]{"C"},
                        new Object[]{"C"},
                        new Object[]{"D"}));

        Bucket bucket = mergeWith(p1Buckets, p2Buckets);
        Assert.assertThat(bucket.size(), is(6));
        assertMerged(bucket, "A", "B", "C", "C", "C", "D");
    }

    @Test
    public void testWithTwoBucketsButOneIsEmpty() throws Exception {
        BucketPage buckets = createPage(
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"B"},
                        new Object[]{"C"}),
                Arrays.<Object[]>asList());

        Bucket bucket = mergeWith(buckets);
        Assert.assertThat(bucket.size(), is(3));
        assertMerged(bucket, "A", "B", "C");
    }

    @Test
    public void testThreaded() throws Exception {
        final SettableFuture<Bucket> bucketFuture1 = SettableFuture.create();
        final SettableFuture<Bucket> bucketFuture2 = SettableFuture.create();
        final BucketPage buckets = new BucketPage(Arrays.<ListenableFuture<Bucket>>asList(bucketFuture1, bucketFuture2));
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture1.set(new ArrayBucket(new Object[][]{{ "A" }, { "B" }}));
            }
        });
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture2.set(new ArrayBucket(new Object[][] {{ "D" },{ "C" }}));
            }
        });
        final SettableFuture<Bucket> future = SettableFuture.create();
        Thread mergeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    future.set(mergeWith(buckets));
                } catch (ExecutionException | InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        mergeThread.start();
        thread1.start();
        thread2.start();

        assertMerged(future.get(), "D", "C", "B", "A");
        thread1.join();
        thread2.join();
        mergeThread.join();
    }

    @Test
    public void testThreadedNoMoreRowsPlease() throws Exception {
        InputCollectExpression<Object> collectExpression = new InputCollectExpression<>(0);
        SimpleTopNProjector topNProjector = new SimpleTopNProjector(
                Arrays.<Input<?>>asList(collectExpression),
                Arrays.asList(collectExpression).toArray(new CollectExpression[1]),
                2,
                0);
        final CollectingProjector collectingProjector = new CollectingProjector();
        final NonSortingBucketMerger merger = new NonSortingBucketMerger();
        merger.downstream(topNProjector);
        topNProjector.downstream(collectingProjector);

        final SettableFuture<Bucket> bucketFuture1 = SettableFuture.create();
        final SettableFuture<Bucket> bucketFuture2 = SettableFuture.create();
        final BucketPage buckets = new BucketPage(Arrays.<ListenableFuture<Bucket>>asList(bucketFuture1, bucketFuture2));
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture1.set(new ArrayBucket(new Object[][]{{ "A" }, { "B" }}));
            }
        });
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture2.set(new ArrayBucket(new Object[][] {{ "D" },{ "C" }}));
            }
        });
        final SettableFuture<Bucket> future = SettableFuture.create();
        Thread mergeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    merger.nextPage(buckets, new PageConsumeListener() {
                        @Override
                        public void needMore() {
                            fail("wo don't need more");
                        }

                        @Override
                        public void finish() {
                            merger.finish();
                        }
                    });
                    Bucket merged = collectingProjector.result().get();
                    future.set(merged);
                } catch (ExecutionException | InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        mergeThread.start();
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        mergeThread.join();

        Bucket merged = future.get();
        assertThat(merged.size(), is(2));
    }

    @Test
    public void testFailingBucket() throws Exception {
        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(TestingHelpers.cause(IllegalArgumentException.class, "bla"));

        BucketPage page1 = new BucketPage(
            Arrays.asList(
                Futures.<Bucket>immediateFailedFuture(new IllegalArgumentException("bla")),
                Futures.<Bucket>immediateFuture(new ArrayBucket(new Object[][]{{"A", "B"}}))
            )
        );
        mergeWith(page1);
    }
}
