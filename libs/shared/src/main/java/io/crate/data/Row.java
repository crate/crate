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

import java.util.Objects;

/**
 * Represents a row.
 *
 * Row instances are typically shared to avoid allocations.
 * This means the same row instance might return different values on subsequent {@link #get(int)} calls
 * if the underlying source changed positions.
 * (Ex. if a BatchIterator provided the row and BatchIterator#moveNext() is called again.
 *
 * If data from a Row must be buffered, it's therefore necessary to use {@link #materialize()}
 * or access the column values directly via {@link #get(int)}
 */
public abstract class Row {

    public static final Row EMPTY = new Row() {

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

    public abstract int numColumns();

    /**
     * Returns the element at the specified column
     *
     * @param index index of the column to return
     * @return the value at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    public abstract Object get(int index);

    /**
     * Returns a materialized copy of this row.
     */
    public Object[] materialize() {
        Object[] result = new Object[numColumns()];
        for (int i = 0; i < result.length; i++) {
            result[i] = get(i);
        }
        return result;
    }


    // We need all Row implementations to compare based on their values.
    // This is necessary for a "peek" → "materialize" pattern like it is used in
    // TopNDistinctBatchIterator to work.

    @Override
    public final int hashCode() {
        int size = numColumns();
        int result = size;
        for (int i = 0; i < size; i++) {
            Object element = get(i);
            result = 31 * result + (element == null ? 0 : element.hashCode());
        }
        return result;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Row)) {
            return false;
        }
        Row o = (Row) obj;
        int size = numColumns();
        if (o.numColumns() != size) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (!Objects.equals(get(i), o.get(i))) {
                return false;
            }
        }
        return true;
    }
}
