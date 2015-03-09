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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.crate.testing.BucketHelpers.createBucketFutures;
import static org.hamcrest.Matchers.is;


@SuppressWarnings("unchecked")
public class SortingBucketMergerTest extends AbstractRandomizedTest {

    private Bucket mergeWith(int buckets, int offset, int limit, List<ListenableFuture<Bucket>> ... pages)
            throws ExecutionException, InterruptedException {

        CollectingProjector collectingProjector = new CollectingProjector();
        SortingBucketMerger merger = new SortingBucketMerger(
                buckets, offset, limit, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });
        merger.downstream(collectingProjector);

        for (List<ListenableFuture<Bucket>> page : pages) {
            merger.merge(page);
        }
        merger.finish();
        return collectingProjector.result().get();
    }

    protected void assertRows(Bucket bucket, String... expected) {
        assertThat(TestingHelpers.printedTable(bucket), is(Joiner.on("\n").join(expected) + "\n"));
    }

    @Test
    public void testRowsWithMultipleColumns() throws Exception {
        List<ListenableFuture<Bucket>> page1 = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A", 1 },
                        new Object[] { "B", 1 }
                ),
                Arrays.asList(
                        new Object[] { "A", 2 },
                        new Object[] { "B", 2 }
                )
        );
        Bucket bucket = mergeWith(2, 0, 10, page1);
        assertRows(bucket, "A| 1", "A| 2", "B| 1", "B| 2");
    }

    @Test
    public void testMergeWith3Buckets() throws Exception {
        List<ListenableFuture<Bucket>> page1 = createBucketFutures(
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
        Bucket bucket = mergeWith(3, 0, 11, page1);
        assertRows(bucket, "A", "A", "A", "B", "B", "B", "C");
    }

    @Test
    public void testMerge() throws Exception {
        // page1
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" }),
                Arrays.asList(
                        new Object[] { "B" },
                        new Object[] { "B" })
        );
        List<ListenableFuture<Bucket>> p2Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "C" },
                        new Object[] { "C" },
                        new Object[] { "D" }),
                Arrays.asList(
                        new Object[] { "B" },
                        new Object[] { "B" },
                        new Object[] { "D" }));

        Bucket bucket = mergeWith(2, 0, 11, p1Buckets, p2Buckets);
        assertRows(bucket, "A", "A", "B", "B", "B", "B", "B", "C", "C", "C", "D" );
    }

    @Test
    public void testBucketFromP1CantBeConsumedUntilPage3() throws Exception {
        // this tests that the iterator chaining works correctly

        List<ListenableFuture<Bucket>> p1 = createBucketFutures(
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
        List<ListenableFuture<Bucket>> p2 = createBucketFutures(
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
        List<ListenableFuture<Bucket>> p3 = createBucketFutures(
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
        Bucket bucket = mergeWith(2, 0, 100, p1, p2, p3);
        assertRows(bucket, "A", "A", "B", "B", "C", "C", "X", "X", "X", "Y", "Y", "Y", "Z", "Z", "Z");
    }

    @Test
    public void testAllBucketsAreEmpty() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.<Object[]>asList(),
                Arrays.<Object[]>asList()
        );
        Bucket bucket = mergeWith(2, 0, 11, p1Buckets);
        assertThat(bucket.size(), is(0));
    }

    @Test
    public void testWithOnlyOneBucket() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" }));
        List<ListenableFuture<Bucket>> p2Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "C" },
                        new Object[] { "C" },
                        new Object[] { "D" }));

        Bucket bucket = mergeWith(1, 0, 11, p1Buckets, p2Buckets);
        assertThat(bucket.size(), is(6));
        assertRows(bucket, "A", "B", "C", "C", "C", "D");
    }

    @Test
    public void testWithOneBucketOffsetAndLimit() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" },
                        new Object[] { "D" }
                ));

        Bucket bucket = mergeWith(1, 1, 2, p1Buckets);
        assertThat(bucket.size(), is(2));
        assertRows(bucket, "B", "C");
    }

    @Test
    public void testWithTwoBucketsButOneIsEmptyAndOffset() throws Exception {
        List<ListenableFuture<Bucket>> buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" }),
                Arrays.<Object[]>asList());

        Bucket bucket = mergeWith(2, 1, 3, buckets);
        assertThat(bucket.size(), is(2));
        assertRows(bucket, "B", "C");
    }
}