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

import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SortedPagingIteratorTest {

    public static final com.google.common.collect.Ordering<io.crate.core.collections.Row> ORDERING =
            OrderingByPosition.rowOrdering(new int[]{0}, new boolean[]{false}, new Boolean[]{null});

    @Test
    public void testTwoBucketsAndTwoPagesAreSortedCorrectly() throws Exception {
        SortedPagingIterator<Row> pagingIterator = new SortedPagingIterator<>(ORDERING);

        pagingIterator.merge(Arrays.asList(
                new ArrayBucket(new Object[][] {
                        new Object[] {"a"} ,
                        new Object[] {"b"},
                        new Object[] {"c"}}).iterator(),
                new ArrayBucket(new Object[][] {
                        new Object[] {"x"},
                        new Object[] {"y"},
                }).iterator()
        ));

        List<Object[]> rows = new ArrayList<>();

        consumeRows(pagingIterator, rows);

        assertThat(rows.size(), is(3));
        assertThat(TestingHelpers.printRows(rows), is("a\nb\nc\n"));

        pagingIterator.merge(Arrays.asList(
                new ArrayBucket(new Object[][] {
                        new Object[] {"d"},
                        new Object[] {"e"},
                }).iterator(),
                new ArrayBucket(new Object[][] {
                        new Object[] {"y"},
                        new Object[] {"z"},
                }).iterator()
        ));

        consumeRows(pagingIterator, rows);
        assertThat(rows.size(), is(5));
        assertThat(TestingHelpers.printRows(rows), is("a\nb\nc\nd\ne\n"));

        pagingIterator.finish();
        consumeRows(pagingIterator, rows);
        assertThat(rows.size(), is(9));
        assertThat(TestingHelpers.printRows(rows), is("a\nb\nc\nd\ne\nx\ny\ny\nz\n"));
    }

    private void consumeRows(SortedPagingIterator<Row> pagingIterator, List<Object[]> rows) {
        while (pagingIterator.hasNext()) {
            rows.add(pagingIterator.next().materialize());
        }
    }
}