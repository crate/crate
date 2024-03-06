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

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;

/**
 * Wraps a PagingIterator and uses {@link io.crate.data.breaker.RamAccounting} to apply the circuit breaking logic
 */
public class RamAccountingPageIterator<TKey> implements PagingIterator<TKey, Row> {

    @VisibleForTesting
    final PagingIterator<TKey, Row> delegatePagingIterator;
    private final RowAccounting<Row> rowAccounting;

    public RamAccountingPageIterator(PagingIterator<TKey, Row> delegatePagingIterator, RowAccounting<Row> rowAccounting) {
        this.delegatePagingIterator = delegatePagingIterator;
        this.rowAccounting = rowAccounting;
    }

    @Override
    public void merge(Iterable<? extends KeyIterable<TKey, Row>> keyIterables) {
        for (KeyIterable<TKey, Row> iterable : keyIterables) {
            for (Row row : iterable) {
                rowAccounting.accountForAndMaybeBreak(row);
            }
        }
        delegatePagingIterator.merge(keyIterables);
    }

    @Override
    public void finish() {
        delegatePagingIterator.finish();
    }

    @Override
    public TKey exhaustedIterable() {
        return delegatePagingIterator.exhaustedIterable();
    }

    @Override
    public Iterable<Row> repeat() {
        return delegatePagingIterator.repeat();
    }

    @Override
    public boolean hasNext() {
        return delegatePagingIterator.hasNext();
    }

    @Override
    public Row next() {
        return delegatePagingIterator.next();
    }
}
