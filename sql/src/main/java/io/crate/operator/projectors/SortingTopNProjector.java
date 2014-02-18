/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.projectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import org.apache.lucene.util.PriorityQueue;
import org.cratedb.Constants;
import org.cratedb.core.collections.ArrayIterator;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Projector used for sorted limit and offset queries
 *
 * storing rows in a sorted PriorityQueue.
 * Passes rows over to upStresm projector in {@link SortingTopNProjector#finishProjection()} phase.
 */
public class SortingTopNProjector extends AbstractProjector {

    class RowPriorityQueue extends PriorityQueue<Object[]> {

        public RowPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Object[] a, Object[] b) {
            for (Comparator c : comparators) {
                int compared = c.compare(a, b);
                if (compared < 0) return true;
                if (compared == 0) continue;
                if (compared > 0) return false;
            }
            return false;
        }

        public Object[] result() {
            return getHeapArray();
        }
    }

    class ColOrdering extends Ordering<Object[]> {

        private final int col;
        private final boolean reverse;
        private final Ordering<Comparable> ordering;

        ColOrdering(int col, boolean reverse) {
            this.col = col;
            this.reverse = reverse;

            // note, that we are reverse for the queue so this conditional is by intent
            if (reverse) {
                ordering = Ordering.natural();
            } else {
                ordering = Ordering.natural().reverse();
            }
        }

        @Override
        public int compare(@Nullable Object[] left, @Nullable Object[] right) {
            return ordering.compare((Comparable) left[col], (Comparable) right[col]);
        }
    }


    private final int start;
    private final int end;
    private final int numOutputs;

    private RowPriorityQueue pq;
    private final AtomicInteger collected = new AtomicInteger(0);
    private final Comparator[] comparators;
    private Object[][] result = Constants.EMPTY_RESULT;

    /**
     *
     * @param inputs contains output {@link io.crate.operator.Input}s and orderBy {@link io.crate.operator.Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs <code>inputs</code> contains this much output {@link io.crate.operator.Input}s starting form index 0
     * @param orderBy indices of {@link io.crate.operator.Input}s in parameter <code>inputs</code> we sort by
     * @param reverseFlags for every index orderBy a boolean indicates ascending (<code>false</code>) or descending (<code>true</code>) order
     * @param limit the number of rows to gather, pass to upStream
     * @param offset the initial offset, this number of rows are skipped
     */
    public SortingTopNProjector(Input<?>[] inputs, CollectExpression<?>[] collectExpressions,
                                int numOutputs,
                                int[] orderBy, boolean[] reverseFlags,
                                int limit, int offset) {
        super(inputs, collectExpressions);
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset>=0, "invalid offset");
        this.start = offset;
        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.end = this.start + limit;

        this.numOutputs = numOutputs;

        comparators = new Comparator[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            int col = orderBy[i];
            boolean reverse = reverseFlags[i];
            comparators[i] = new ColOrdering(col, reverse);
        }
    }

    @Override
    public void startProjection() {
        collected.set(0);
        pq = new RowPriorityQueue(end);
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        collected.incrementAndGet();
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        Object[] evaluatedRow = new Object[inputs.length];
        int i = 0;
        for (Input<?> input: inputs) {
            evaluatedRow[i++] = input.value();
        }
        pq.insertWithOverflow(evaluatedRow);
        return true;
    }

    @Override
    public void finishProjection() {
        final int resultSize = Math.max(pq.size() - start, 0);
        if (downStream.isPresent()) {
            // pass rows to downStream
            Projector projector = downStream.get();

            projector.startProjection();
            for (int i = (resultSize - 1); i >= 0; i--) {
                Object[] row = Arrays.copyOfRange(pq.pop(), 0, numOutputs); // strip order by inputs
                if (!projector.setNextRow(row)) {
                    break;
                }
            }
            projector.finishProjection();
        } else {
            // store result-array in local buffer
            result = new Object[resultSize][];
            for (int i = resultSize - 1; i >= 0; i--) {
                result[i] = Arrays.copyOfRange(pq.pop(), 0, numOutputs); // strip order by inputs
            }
        }
        pq.clear();
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return result;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new ArrayIterator(result, 0, result.length);
    }

}
