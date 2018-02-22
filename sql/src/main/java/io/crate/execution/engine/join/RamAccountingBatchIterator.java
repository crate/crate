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

package io.crate.execution.engine.join;

import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.Row;

import javax.annotation.Nonnull;

/**
 * Wraps a {@link BatchIterator} and uses {@link io.crate.breaker.RamAccountingContext}
 * to apply the circuit breaking logic by calculating memory occupied for all rows of the iterator.
 * <p>
 * This wrapper can be typically used when the BatchIterator consumer "reads" all
 * elements of the iterator and keeps them in memory. e.g.: {@link HashInnerJoinBatchIterator}
 * keeps in memory all elements of the left side to build the HashMap used for the Hash-Join execution.
 */
public class RamAccountingBatchIterator<T extends Row> extends ForwardingBatchIterator<T> {

    private final BatchIterator<T> delegateBatchIterator;
    private final RowAccounting rowAccounting;

    public RamAccountingBatchIterator(BatchIterator<T> delegatePagingIterator, RowAccounting rowAccounting) {
        this.delegateBatchIterator = delegatePagingIterator;
        this.rowAccounting = rowAccounting;
    }

    @Override
    protected BatchIterator<T> delegate() {
        return delegateBatchIterator;
    }

    @Override
    public boolean moveNext() {
        boolean result = delegateBatchIterator.moveNext();
        if (result) {
            rowAccounting.accountForAndMaybeBreak(delegateBatchIterator.currentElement());
        }
        return result;
    }

    @Override
    public void close() {
        rowAccounting.close();
        super.close();
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        rowAccounting.close();
        super.kill(throwable);
    }
}
