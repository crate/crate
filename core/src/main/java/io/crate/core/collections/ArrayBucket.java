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

package io.crate.core.collections;

import java.util.Iterator;

public class ArrayBucket implements Bucket {

    private final Object[][] rows;
    private final int rowSize;


    /**
     * Cosntructs a new ArrayBucket with rows of the given size, regardless what length of the row arrays is.
     *
     * @param rows    the backing array of this bucket
     * @param rowSize the size of rows emitted from this bucket
     */
    public ArrayBucket(Object[][] rows, int rowSize) {
        this.rows = rows;
        this.rowSize = rowSize;
    }

    public ArrayBucket(Object[][] rows) {
        this.rows = rows;
        if (rows.length > 0) {
            rowSize = rows[0].length;
        } else {
            rowSize = -1;
        }
    }

    @Override
    public int size() {
        return rows.length;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {
            int pos = 0;
            Object[] current;
            final Row row = new Row() {
                @Override
                public int size() {
                    return rowSize;
                }

                @Override
                public Object get(int index) {
                    return current[index];
                }
            };

            @Override
            public boolean hasNext() {
                return pos < rows.length;
            }

            @Override
            public Row next() {
                current = rows[pos++];
                return row;
            }

            @Override
            public void remove() {

            }
        };
    }
}
