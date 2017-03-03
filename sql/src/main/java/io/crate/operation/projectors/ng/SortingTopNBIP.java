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

package io.crate.operation.projectors.ng;

import com.google.common.base.Preconditions;
import io.crate.data.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.RowPriorityQueue;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;


/**
 * Collector implementation which collects rows into a priorityQueue in order to sort the rows and apply a limit + offset.
 * The final result is a sorted bucket with limit and offset applied.
 */
public class SortingTopNBIP implements BatchIteratorProjector {


    private final Collection<? extends Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> expressions;
    private final int numOutputs;
    private final Comparator<Object[]> comparator;
    private final int limit;
    private final int offset;

    private Object[] spare;

    /**
     * @param inputs      contains output {@link Input}s and orderBy {@link Input}s
     * @param expressions expressions linked to the inputs
     * @param numOutputs  number of output columns
     * @param comparator  used to sort the rows
     * @param limit       the max number of rows the result should contain
     * @param offset      the number of rows to skip (after sort)
     */
    public SortingTopNBIP(Collection<? extends Input<?>> inputs,
                          Iterable<? extends CollectExpression<Row, ?>> expressions,
                          int numOutputs,
                          Comparator<Object[]> comparator,
                          int limit,
                          int offset) {
        Preconditions.checkArgument(limit > 0, "invalid limit %s, this collector only supports positive limits", limit);
        Preconditions.checkArgument(offset >= 0, "invalid offset %s", offset);

        this.inputs = inputs;
        this.expressions = expressions;
        this.numOutputs = numOutputs;
        this.comparator = comparator;
        this.limit = limit;
        this.offset = offset;
    }

    public int numOutputs() {
        return numOutputs;
    }

    class Listener implements CollectingProjectors.BatchCollectorListener {

        private final Row row;
        private final RowPriorityQueue<Object[]> pq = new RowPriorityQueue<>(offset + limit, comparator);
        Object[] current;

        Listener(Row row) {
            this.row = row;
        }

        public Object[] current() {
            return current;
        }

        @Override
        public boolean onRow() {
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
            return true;
        }

        @Override
        public Iterable<?> post() {
            int resultSize = Math.max(pq.size() - offset, 0);
            Object[][] rows = new Object[resultSize][];
            for (int i = resultSize - 1; i >= 0; i--) {
                rows[i] = pq.pop();
            }
            return (Iterable<Void>) () -> new Iterator<Void>() {
                int pos = -1;
                @Override
                public boolean hasNext() {
                    return pos+1 < rows.length;
                }

                @Override
                public Void next() {
                    current = rows[++pos];
                    return null;
                }
            };
        }
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        Listener l = new Listener(RowBridging.toRow(batchIterator.rowData()));
        return CollectingProjectors.collecting(batchIterator, l,
            ListBackedColumns.forArraySupplier(numOutputs).apply(l::current));
    }
}
