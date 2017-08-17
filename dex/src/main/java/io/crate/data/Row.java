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

package io.crate.data;

/**
 * Represents a row.
 *
 * Row instances are typically shared to avoid allocations.
 * This means the same row instance might return different values on subsequent {@link #get(int)} calls
 * if the underlying source changed positions.
 * (Ex. if a {@link BatchIterator} provided the row and {@link BatchIterator#moveNext()} is called again.
 *
 * If data from a Row must be buffered, it's therefore necessary to use {@link #materialize()}
 * or access the column values directly via {@link #get(int)}
 */
public interface Row {

    Row EMPTY = new Row() {

        private final Object[] EMPTY_CELLS = new Object[0];

        @Override
        public int numColumns() {
            return 0;
        }

        @Override
        public Object get(int index) {
            throw new IndexOutOfBoundsException("EMPTY row has no cells");
        }

        @Override
        public Object[] materialize() {
            return EMPTY_CELLS;
        }
    };

    int numColumns();

    /**
     * Returns the element at the specified column
     *
     * @param index index of the column to return
     * @return the value at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    Object get(int index);

    /**
     * Returns a materialized view of this row.
     */
    Object[] materialize();
}
