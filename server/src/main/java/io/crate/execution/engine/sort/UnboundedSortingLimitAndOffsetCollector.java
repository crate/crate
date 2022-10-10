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

import io.crate.breaker.RowAccounting;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import org.apache.lucene.util.ArrayUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;


/**
 * Collector implementation which collects rows into an unbounded priorityQueue in order to sort the rows and apply a
 * limit + offset.
 * The final result is a sorted bucket with limit and offset applied.
 */
public class UnboundedSortingLimitAndOffsetCollector implements Collector<Row, PriorityQueue<Object[]>, Bucket> {

    private final Collection<? extends Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> expressions;
    private final int numOutputs;
    private final Comparator<Object[]> comparator;
    private final int initialCapacity;
    private final int offset;
    private final int maxNumberOfRowsInQueue;
    private final RowAccounting<Object[]> rowAccounting;

    /**
     * @param rowAccounting   sorting is a pipeline breaker so account for the used memory
     * @param inputs          contains output {@link Input}s and orderBy {@link Input}s
     * @param expressions     expressions linked to the inputs
     * @param numOutputs      number of output columns
     * @param comparator      used to sort the rows
     * @param initialCapacity the initial capacity of the backing queue
     * @param limit           the max number of rows the result should contain
     * @param offset          the number of rows to skip (after sort)
     */
    public UnboundedSortingLimitAndOffsetCollector(RowAccounting<Object[]> rowAccounting,
                                                   Collection<? extends Input<?>> inputs,
                                                   Iterable<? extends CollectExpression<Row, ?>> expressions,
                                                   int numOutputs,
                                                   Comparator<Object[]> comparator,
                                                   int initialCapacity,
                                                   int limit,
                                                   int offset) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Invalid initial capacity: value must be > 0; got: " + initialCapacity);
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Invalid LIMIT: value must be > 0; got: " + limit);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid OFFSET: value must be >= 0; got: " + offset);
        }
        this.rowAccounting = rowAccounting;
        this.inputs = inputs;
        this.expressions = expressions;
        this.numOutputs = numOutputs;
        this.comparator = comparator;
        this.initialCapacity = initialCapacity;
        this.offset = offset;
        this.maxNumberOfRowsInQueue = limit + offset;

        if (maxNumberOfRowsInQueue >= ArrayUtil.MAX_ARRAY_LENGTH || maxNumberOfRowsInQueue < 0) {
            // Throw exception to prevent confusing OOME in PriorityQueue
            // 1) if offset + limit exceeds maximum array length
            // 2) if offset + limit exceeds Integer.MAX_VALUE (then maxSize is negative!)
            throw new IllegalArgumentException(
                "Invalid LIMIT + OFFSET: value must be <= " + (ArrayUtil.MAX_ARRAY_LENGTH - 1) + "; got: " + maxNumberOfRowsInQueue);
        }
    }

    @Override
    public Supplier<PriorityQueue<Object[]>> supplier() {
        return () -> new PriorityQueue<>(initialCapacity, comparator.reversed());
    }

    @Override
    public BiConsumer<PriorityQueue<Object[]>, Row> accumulator() {
        return this::onNextRow;
    }

    @Override
    public BinaryOperator<PriorityQueue<Object[]>> combiner() {
        return (pq1, pq2) -> {
            throw new UnsupportedOperationException("combine not supported");
        };
    }

    @Override
    public Function<PriorityQueue<Object[]>, Bucket> finisher() {
        return this::pqToIterable;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private void onNextRow(PriorityQueue<Object[]> pq, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        Object[] rowCells = new Object[inputs.size()];
        int i = 0;
        for (Input<?> input : inputs) {
            rowCells[i] = input.value();
            i++;
        }
        rowAccounting.accountForAndMaybeBreak(rowCells);
        if (pq.size() == maxNumberOfRowsInQueue) {
            Object[] highestElementInOrder = pq.peek();
            if (highestElementInOrder == null || comparator.compare(rowCells, highestElementInOrder) < 0) {
                pq.poll();
                pq.add(rowCells);
            }
        } else {
            pq.add(rowCells);
        }
    }

    private Bucket pqToIterable(PriorityQueue<Object[]> pq) {
        if (offset > pq.size()) {
            return new ArrayBucket(new Object[0][0], numOutputs);
        }
        int resultSize = Math.max(Math.min(maxNumberOfRowsInQueue - offset, pq.size() - offset), 0);

        Object[][] rows = new Object[resultSize][];
        for (int i = resultSize - 1; i >= 0; i--) {
            rows[i] = pq.poll();
        }
        return new ArrayBucket(rows, numOutputs);
    }
}
