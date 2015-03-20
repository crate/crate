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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.crate.testing.TestingHelpers.createPage;
import static org.hamcrest.Matchers.is;


@SuppressWarnings("unchecked")
public class SortingBucketMergerTest extends CrateUnitTest {

    private Bucket mergeWith(int buckets, @Nullable Boolean nullsFirst, BucketPage... pages)
            throws ExecutionException, InterruptedException {

        CollectingProjector collectingProjector = new CollectingProjector();
        final SortingBucketMerger merger = new SortingBucketMerger(
                buckets, new int[] { 0 }, new boolean[] { false }, new Boolean[] { nullsFirst }, Optional.<Executor>absent());
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

    protected void assertRows(Bucket bucket, String... expected) {
        assertThat(TestingHelpers.printedTable(bucket), is(Joiner.on("\n").join(expected) + "\n"));
    }

    @Test
    public void testRowsWithMultipleColumns() throws Exception {
        BucketPage page1 = createPage(
                Arrays.asList(
                        new Object[]{"A", 1},
                        new Object[]{"B", 1}
                ),
                Arrays.asList(
                        new Object[]{"A", 2},
                        new Object[]{"B", 2}
                )
        );
        Bucket bucket = mergeWith(2, null, page1);
        assertRows(bucket, "A| 1", "A| 2", "B| 1", "B| 2");
    }

    @Test
    public void testNullsFirst() throws Exception {
        BucketPage page1 = createPage(
                Arrays.asList(
                        new Object[]{null, 1},
                        new Object[]{"A", 1},
                        new Object[]{"B", 1}
                ),
                Arrays.asList(
                        new Object[]{null, 2},
                        new Object[]{"A", 2},
                        new Object[]{"B", 2}
                )
        );
        BucketPage page2 = createPage(
                Arrays.<Object[]>asList(
                        new Object[]{"C", 3}
                ),
                Arrays.<Object[]>asList(
                        new Object[]{"D", 3}
                )
        );
        Bucket bucket = mergeWith(2, false, page1, page2);
        assertRows(bucket, "NULL| 1", "NULL| 2", "A| 1", "A| 2", "B| 1", "B| 2", "C| 3", "D| 3");
    }

    @Test
    public void testNullsLast() throws Exception {
        BucketPage page1 = createPage(
                Arrays.asList(
                        new Object[]{"A", 1},
                        new Object[]{"B", 1}
                ),
                Arrays.asList(
                        new Object[]{"A", 2},
                        new Object[]{"B", 2}
                )
        );
        BucketPage page2 = createPage(
                Arrays.asList(
                        new Object[]{"C", 3},
                        new Object[]{null, 1}
                ),
                Arrays.asList(
                        new Object[]{"D", 3},
                        new Object[]{null, 2}
                )
        );
        Bucket bucket = mergeWith(2, false, page1, page2);
        assertRows(bucket, "A| 1", "A| 2", "B| 1", "B| 2", "C| 3", "D| 3", "NULL| 1", "NULL| 2");
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
        Bucket bucket = mergeWith(3, null, page1);
        assertRows(bucket, "A", "A", "A", "B", "B", "B", "C");
    }

    @Test
    public void testMerge() throws Exception {
        // page1
        BucketPage p1Buckets = createPage(Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"A"},
                        new Object[]{"B"},
                        new Object[]{"C"}),
                Arrays.asList(
                        new Object[]{"B"},
                        new Object[]{"B"})
        );
        BucketPage p2Buckets = createPage(
                Arrays.asList(
                        new Object[]{"C"},
                        new Object[]{"C"},
                        new Object[]{"D"}),
                Arrays.asList(
                        new Object[]{"B"},
                        new Object[]{"B"},
                        new Object[]{"D"}));

        Bucket bucket = mergeWith(2, null, p1Buckets, p2Buckets);
        assertRows(bucket, "A", "A", "B", "B", "B", "B", "B", "C", "C", "C", "D", "D");
    }

    @Test
    public void testBucketFromP1CantBeConsumedUntilPage3() throws Exception {
        // this tests that the iterator chaining works correctly

        BucketPage p1 = createPage(
                Arrays.asList(
                        new Object[]{"X"},
                        new Object[]{"X"},
                        new Object[]{"X"}
                ),
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"A"}
                )
        );
        BucketPage p2 = createPage(
                Arrays.asList(
                        new Object[]{"Y"},
                        new Object[]{"Y"},
                        new Object[]{"Y"}
                ),
                Arrays.asList(
                        new Object[]{"B"},
                        new Object[]{"B"}
                )
        );
        BucketPage p3 = createPage(
                Arrays.asList(
                        new Object[]{"Z"},
                        new Object[]{"Z"},
                        new Object[]{"Z"}
                ),
                Arrays.asList(
                        new Object[]{"C"},
                        new Object[]{"C"}
                )
        );
        Bucket bucket = mergeWith(2, null, p1, p2, p3);
        assertRows(bucket, "A", "A", "B", "B", "C", "C", "X", "X", "X", "Y", "Y", "Y", "Z", "Z", "Z");
    }

    @Test
    public void testAllBucketsAreEmpty() throws Exception {
        BucketPage p1Buckets = createPage(
                Arrays.<Object[]>asList(),
                Arrays.<Object[]>asList()
        );
        Bucket bucket = mergeWith(2, null, p1Buckets);
        assertThat(bucket.size(), is(0));
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

        Bucket bucket = mergeWith(1, null, p1Buckets, p2Buckets);
        assertThat(bucket.size(), is(6));
        assertRows(bucket, "A", "B", "C", "C", "C", "D");
    }


    @Test
    public void testWithTwoBucketsButOneIsEmpty() throws Exception {
        BucketPage buckets = createPage(
                Arrays.asList(
                        new Object[]{"A"},
                        new Object[]{"B"},
                        new Object[]{"C"}),
                Arrays.<Object[]>asList());

        Bucket bucket = mergeWith(2, null, buckets);
        assertThat(bucket.size(), is(3));
        assertRows(bucket, "A", "B", "C");
    }

    @Test
    public void testThreaded() throws Exception {
        final SettableFuture<Bucket> bucketFuture1 = SettableFuture.create();
        final SettableFuture<Bucket> bucketFuture2 = SettableFuture.create();
        final BucketPage page1 = new BucketPage(Arrays.<ListenableFuture<Bucket>>asList(bucketFuture1, bucketFuture2));

        final SettableFuture<Bucket> bucketFuture3 = SettableFuture.create();
        final SettableFuture<Bucket> bucketFuture4 = SettableFuture.create();
        final BucketPage page2 = new BucketPage(Arrays.<ListenableFuture<Bucket>>asList(bucketFuture3, bucketFuture4));

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture1.set(new ArrayBucket(new Object[][]{{ "A" }, { "B" }}));
                try {
                    Thread.sleep(randomIntBetween(10, 100));
                } catch (InterruptedException e) {
                    bucketFuture3.setException(e);
                }
                bucketFuture3.set(new ArrayBucket(new Object[][]{{ "D" }, { "D" }}));
            }
        });
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                bucketFuture2.set(new ArrayBucket(new Object[][] {{ "C" }, { "D" }}));
                try {
                    Thread.sleep(randomIntBetween(10, 100));
                } catch (InterruptedException e) {
                    bucketFuture4.setException(e);
                }
                bucketFuture4.set(new ArrayBucket(new Object[][]{{ "D" }, { "E" }}));
            }
        });
        final SettableFuture<Bucket> future = SettableFuture.create();
        Thread mergeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    future.set(mergeWith(2, null, page1, page2));
                } catch (ExecutionException | InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        mergeThread.start();
        thread1.start();
        thread2.start();

        assertRows(future.get(), "A", "B", "C", "D", "D", "D", "D", "E");
        thread1.join();
        thread2.join();
        mergeThread.join();
    }
}