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

package io.crate.data.testing;

import java.util.Iterator;
import java.util.NoSuchElementException;

import io.crate.data.Row;
import io.crate.data.RowN;

public class RowGenerator {

    /**
     * Generate a range of rows.
     * Both increasing (e.g. 0 -> 10) and decreasing ranges (10 -> 0) are supported.
     *
     * @param from start (inclusive)
     * @param to   end (exclusive)
     */
    public static Iterable<Row> range(final int from, final int to) {
        return () -> new Iterator<>() {

            private final Object[] columns = new Object[1];
            private final RowN sharedRow = new RowN(columns);
            private final int step = from < to ? 1 : -1;
            private int i = from;

            @Override
            public boolean hasNext() {
                return step >= 0 ? i < to : i > to;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator exhausted");
                }
                columns[0] = i;
                i += step;
                return sharedRow;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not supported");
            }
        };
    }

    /**
     * Return a Iterable where each Row has the same instance, but the data changes depending on the given iterable.
     *
     * @param iterable iterable containing values for the first column of the Rows in the result.
     */
    public static <T> Iterable<Row> fromSingleColValues(Iterable<T> iterable) {
        return () -> new Iterator<>() {

            private final Object[] cells = new Object[1];
            private final RowN row = new RowN(cells);
            private final Iterator<?> iterator = iterable.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Row next() {
                cells[0] = iterator.next();
                return row;
            }
        };
    }
}
