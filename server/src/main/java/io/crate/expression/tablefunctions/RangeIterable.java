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

package io.crate.expression.tablefunctions;


import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.crate.data.Row;
import io.crate.data.RowN;


public final class RangeIterable<T> implements Iterable<Row> {

    private final Object [] columns;
    private final RowN row;
    private final T endInclusive;
    private final UnaryOperator<T> nextValue;
    private final Comparator<T> comparator;
    private final Function<T, Object> resultExtractor;
    private final boolean reversed;
    private T value;

    public RangeIterable(T startInclusive,
                         T endInclusive,
                         UnaryOperator<T> nextValue,
                         Comparator<T> comparator,
                         Function<T, Object> valueExtractor) {
        columns = new Object[]{ null };
        row = new RowN(columns);
        value = startInclusive;
        this.endInclusive = endInclusive;
        this.nextValue = nextValue;
        this.comparator = comparator;
        this.resultExtractor = valueExtractor;
        reversed = comparator.compare(startInclusive, endInclusive) > 0;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                int cmp = comparator.compare(value, endInclusive);
                return reversed ? cmp >= 0 : cmp <= 0;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("no more rows");
                }
                columns[0] = resultExtractor.apply(value);
                value = nextValue.apply(value);
                return row;
            }
        };
    }
}
