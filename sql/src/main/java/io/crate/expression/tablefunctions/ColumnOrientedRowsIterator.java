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

package io.crate.expression.tablefunctions;

import io.crate.data.Row;
import io.crate.data.RowN;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

class ColumnOrientedRowsIterator implements Iterable<Row> {

    private final Supplier<Iterator<Object>[]> iteratorsPerColumn;

    ColumnOrientedRowsIterator(Supplier<Iterator<Object>[]> iteratorsPerColumn) {
        this.iteratorsPerColumn = iteratorsPerColumn;
    }

    @Nonnull
    @Override
    public Iterator<Row> iterator() {
        Iterator<Object>[] iterators = iteratorsPerColumn.get();
        Object[] cells = new Object[iterators.length];
        RowN row = new RowN(cells);

        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                for (Iterator<Object> it : iterators) {
                    if (it.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more rows");
                }
                for (int i = 0; i < iterators.length; i++) {
                    Iterator<Object> iterator = iterators[i];
                    cells[i] = iterator.hasNext() ? iterator.next() : null;
                }
                return row;
            }
        };
    }
}
