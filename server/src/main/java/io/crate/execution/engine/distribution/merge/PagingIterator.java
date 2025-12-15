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

package io.crate.execution.engine.distribution.merge;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.planner.PositionalOrderBy;
import io.crate.types.DataType;

public interface PagingIterator<TKey, TRow> extends Iterator<TRow> {

    /**
     * Add additional iterables to the PagingIterator. (E.g. due to a new Page that has arrived)
     */
    void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables);

    /**
     * This is called if the last page has been received and merge has been called for the last time.
     * If the PagingIterator implementation has been holding rows back, these rows should now be
     * returned on hasNext/next calls.
     */
    void finish();

    TKey exhaustedIterable();

    /**
     * create an iterable to repeat the previous iteration
     *
     * @return an iterable that will iterate through the already emitted items and emit them again in the same order as before
     */
    Iterable<TRow> repeat();

    /**
     * Returns the suitable {@link PagingIterator} according to the use case.
     * If requiresRepeat is true or we're looking to create and ordered iterator then the PagingIterator is wrapped with
     * {@link RamAccountingPageIterator} which calculates the memory usage and applies CircuitBreaker logic.
     */
    static <TKey> PagingIterator<TKey, Row> create(int numUpstreams,
                                                   List<? extends DataType<?>> inputTypes,
                                                   boolean requiresRepeat,
                                                   @Nullable PositionalOrderBy orderBy,
                                                   Supplier<RowAccounting<Row>> rowAccountingSupplier) {
        PagingIterator<TKey, Row> pagingIterator;
        if (numUpstreams == 1 || orderBy == null) {
            if (requiresRepeat) {
                pagingIterator = new RamAccountingPageIterator<>(
                    PassThroughPagingIterator.repeatable(),
                    rowAccountingSupplier.get()
                );
            } else {
                pagingIterator = PassThroughPagingIterator.oneShot();
            }
        } else {
            Comparator<Row> rowOrdering = OrderingByPosition.rowOrdering(inputTypes, orderBy);
            pagingIterator = new RamAccountingPageIterator<>(
                createSorted(rowOrdering, requiresRepeat),
                rowAccountingSupplier.get()
            );
        }

        return pagingIterator;
    }

    static <TKey, TRow> PagingIterator<TKey, TRow> createSorted(Comparator<? super TRow> comparator,
                                                                boolean requiresRepeat) {
        if (requiresRepeat) {
            return new RecordingSortedMergeIterator<>(comparator);
        } else {
            return new PlainSortedMergeIterator<>(comparator);
        }
    }
}
