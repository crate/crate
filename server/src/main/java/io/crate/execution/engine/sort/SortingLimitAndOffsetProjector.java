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

package io.crate.execution.engine.sort;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collector;

import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.data.breaker.RowAccounting;
import io.crate.execution.engine.collect.CollectExpression;

public class SortingLimitAndOffsetProjector implements Projector {

    private final Collector<Row, ?, Bucket> collector;
    private final boolean hasNoResult;

    /**
     * @param rowAccounting               sorting is a pipeline breaker so account for the used memory
     * @param inputs                      contains output {@link Input}s and orderBy {@link Input}s
     * @param collectExpressions          gathered from outputs and orderBy inputs
     * @param numOutputs                  <code>inputs</code> contains this much output {@link Input}s starting form index 0
     * @param ordering                    ordering that is used to compare the rows
     * @param limit                       the number of rows to gather, pass to upStream
     * @param offset                      the initial offset, this number of rows are skipped
     * @param unboundedCollectorThreshold if (limit + offset) is greater than this threshold an unbounded collector will
     *                                    be used, otherwise a bounded one is used.
     */
    public SortingLimitAndOffsetProjector(RowAccounting<Object[]> rowAccounting,
                                          Collection<? extends Input<?>> inputs,
                                          Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                          int numOutputs,
                                          Comparator<Object[]> ordering,
                                          int limit,
                                          int offset,
                                          int unboundedCollectorThreshold) {
        this.hasNoResult = limit + offset == 0;
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid OFFSET: value must be >= 0; got: " + offset);
        } else if (hasNoResult) {
            collector = null;
        } else if ((limit + offset) > unboundedCollectorThreshold) {
            /**
            * We'll use an unbounded queue with the initial capacity of {@link unboundedCollectorThreshold}
            * if the maximum number of rows we have to accommodate in the queue in order to maintain correctness is
            * greater than this threshold.
            *
            * Otherwise, we'll use a bounded queue as we want to avoid the case where we pre-allocate a large queue that
            * will never be filled.
            */
            collector = new UnboundedSortingLimitAndOffsetCollector(
                rowAccounting,
                inputs,
                collectExpressions,
                numOutputs,
                ordering,
                unboundedCollectorThreshold,
                limit,
                offset
            );
        } else {
            collector = new BoundedSortingLimitAndOffsetCollector(
                rowAccounting,
                inputs,
                collectExpressions,
                numOutputs,
                ordering,
                limit,
                offset
            );
        }
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        if (hasNoResult) {
            batchIterator.close();
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        return CollectingBatchIterator.newInstance(batchIterator, collector);
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}
