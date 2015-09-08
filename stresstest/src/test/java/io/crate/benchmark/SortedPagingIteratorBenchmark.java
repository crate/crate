/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.merge.NumberedIterable;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;

import static io.crate.testing.TestingHelpers.range;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SortedPagingIteratorBenchmark {

    @Rule
    public BenchmarkRule benchmarkRule = new BenchmarkRule();

    public static final int NUM_REPEATS = 10;
    public static final Ordering<Row> ORDERING =
            OrderingByPosition.rowOrdering(new int[]{0}, new boolean[]{false}, new Boolean[]{null});

    private Bucket bucket1;
    private Bucket bucket2;
    private Bucket bucket3;

    @Before
    public void prepare() {
        bucket1 = new ArrayBucket(range(0, 1_000_000));
        bucket2 = new ArrayBucket(range(500_000, 1_500_000));
        bucket3 = new ArrayBucket(range(1_000_000, 2_000_000));
    }

    @SafeVarargs
    private final Iterable<? extends NumberedIterable<Row>> numbered(Iterable<Row>... buckets) {

        return Iterables.transform(Arrays.asList(buckets), new Function<Iterable<Row>, NumberedIterable<Row>>() {
            private int i = 0;
            @Nullable
            @Override
            public NumberedIterable<Row> apply(Iterable<Row> input) {
                return new NumberedIterable<>(i++, input);
            }
        });
    }

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
    @Test
    public void testIterateWithRepeat() throws Exception {
        SortedPagingIterator<Row> iterator = new SortedPagingIterator<>(ORDERING, true);
        iterator.merge(numbered(bucket1, bucket2));
        int size1 = 0;
        while (iterator.hasNext()) {
            iterator.next();
            size1++;
        }
        iterator.merge(numbered(bucket3));
        iterator.finish();
        while (iterator.hasNext()) {
            iterator.next();
            size1++;
        }
        assertThat(size1, is(3_000_000));

        for (int i = 0; i < NUM_REPEATS; i++) {
            Iterator<Row> repeatIter = iterator.repeat();
            int size2 = 0;
            while (repeatIter.hasNext()) {
                repeatIter.next();
                size2++;
            }
            assertThat(size2, is(3_000_000));
        }


    }
}
