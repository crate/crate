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

package io.crate.operation.projectors;

import com.google.common.base.Preconditions;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.RowPriorityQueue;
import org.apache.lucene.util.ArrayUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;


/**
 * Collector implementation which collects rows into a priorityQueue in order to sort the rows and apply a limit + offset.
 * The final result is a sorted bucket with limit and offset applied.
 */
public class SortingTopNCollector implements Collector<Row, RowPriorityQueue<Object[]>, Bucket> {

    private final Collection<? extends Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> expressions;
    private final int numOutputs;
    private final Comparator<Object[]> comparator;
    private final int offset;
    private final int maxSize;

    private Object[] spare;

    /**
     * @param inputs             contains output {@link Input}s and orderBy {@link Input}s
     * @param expressions        expressions linked to the inputs
     * @param numOutputs         number of output columns
     * @param comparator         used to sort the rows
     * @param limit              the max number of rows the result should contain
     * @param offset             the number of rows to skip (after sort)
     */
    public SortingTopNCollector(Collection<? extends Input<?>> inputs,
                                Iterable<? extends CollectExpression<Row, ?>> expressions,
                                int numOutputs,
                                Comparator<Object[]> comparator,
                                int limit,
                                int offset) {
        Preconditions.checkArgument(limit > 0, "Invalid LIMIT: value must be > 0; got: " + limit);
        Preconditions.checkArgument(offset >= 0, "Invalid OFFSET: value must be >= 0; got: " + offset);

        this.inputs = inputs;
        this.expressions = expressions;
        this.numOutputs = numOutputs;
        this.comparator = comparator;
        this.offset = offset;
        this.maxSize = limit + offset;

        if (maxSize >= ArrayUtil.MAX_ARRAY_LENGTH || maxSize < 0)  {
            // Throw exception to prevent confusing OOME in PriorityQueue
            // 1) if offset + limit exceeds maximum array length
            // 2) if offset + limit exceeds Integer.MAX_VALUE (then maxSize is negative!)
            throw new IllegalArgumentException("Invalid LIMIT + OFFSET: value must be <= " + (ArrayUtil.MAX_ARRAY_LENGTH - 1) + "; got: " + maxSize);
        }
    }

    public int numOutputs() {
        return numOutputs;
    }

    @Override
    public Supplier<RowPriorityQueue<Object[]>> supplier() {
        return () -> new RowPriorityQueue<>(maxSize, comparator);
    }

    @Override
    public BiConsumer<RowPriorityQueue<Object[]>, Row> accumulator() {
        return this::onNextRow;
    }

    @Override
    public BinaryOperator<RowPriorityQueue<Object[]>> combiner() {
        return (pq1, pq2) -> { throw new UnsupportedOperationException("combine not supported"); };
    }

    @Override
    public Function<RowPriorityQueue<Object[]>, Bucket> finisher() {
        return this::pqToIterable;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private void onNextRow(RowPriorityQueue<Object[]> pq, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        if (spare == null) {
            spare = new Object[inputs.size()];
        }
        int i = 0;
        for (Input<?> input : inputs) {
            spare[i] = input.value();
            i++;
        }
        spare = pq.insertWithOverflow(spare);
    }

    private Bucket pqToIterable(RowPriorityQueue<Object[]> pq) {
        int resultSize = Math.max(pq.size() - offset, 0);
        Object[][] rows = new Object[resultSize][];
        for (int i = resultSize - 1; i >= 0; i--) {
            rows[i] = pq.pop();
        }
        return new ArrayBucket(rows, numOutputs);
    }
}
