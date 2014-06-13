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

package io.crate.operation.projectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.ArrayIterator;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.PriorityQueue;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class SortingTopNProjector implements Projector, ResultProvider {


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

    static class ColOrdering extends Ordering<Object[]> {

        private final int col;
        private final Ordering<Comparable> ordering;

        ColOrdering(int col, boolean reverse, Boolean nullFirst) {
            this.col = col;

            // note, that we are reverse for the queue so this conditional is by intent
            Ordering<Comparable> ordering;
            nullFirst = nullFirst != null ? !nullFirst : null; // swap because queue is reverse
            if (reverse) {
                ordering = Ordering.natural();
                if (nullFirst == null || !nullFirst) {
                    ordering = ordering.nullsLast();
                } else {
                    ordering = ordering.nullsFirst();
                }
            } else {
                ordering = Ordering.natural().reverse();
                if (nullFirst == null || nullFirst) {
                    ordering = ordering.nullsFirst();
                } else {
                    ordering = ordering.nullsLast();
                }
            }
            this.ordering = ordering;
        }

        @Override
        public int compare(@Nullable Object[] left, @Nullable Object[] right) {
            Comparable l = left != null ? (Comparable) left[col] : null;
            Comparable r = right != null ? (Comparable) right[col] : null;
            return ordering.compare(l, r);
        }
    }


    private final int offset;
    private final int maxSize;
    private final int numOutputs;

    private RowPriorityQueue pq;
    private final Comparator[] comparators;
    private final Input<?>[] inputs;
    private final CollectExpression<?>[] collectExpressions;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final SettableFuture<Object[][]> result = SettableFuture.create();

    /**
     * @param inputs             contains output {@link io.crate.operation.Input}s and orderBy {@link io.crate.operation.Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link io.crate.operation.Input}s starting form index 0
     * @param orderBy            indices of {@link io.crate.operation.Input}s in parameter <code>inputs</code> we sort by
     * @param reverseFlags       for every index orderBy a boolean indicates ascending (<code>false</code>) or descending (<code>true</code>) order
     * @param limit              the number of rows to gather, pass to upStream, must be > 0
     * @param offset             the initial offset, this number of rows are skipped
     */
    public SortingTopNProjector(Input<?>[] inputs,
                                CollectExpression<?>[] collectExpressions,
                                int numOutputs,
                                int[] orderBy,
                                boolean[] reverseFlags,
                                Boolean[] nullsFirst,
                                int limit,
                                int offset) {
        Preconditions.checkArgument(limit > TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset >= 0, "invalid offset");
        assert nullsFirst.length == reverseFlags.length;

        this.inputs = inputs;
        this.numOutputs = numOutputs;
        this.collectExpressions = collectExpressions;
        this.offset = offset;

        limit = Math.max(limit, 0);
        this.maxSize = this.offset + limit;
        comparators = new Comparator[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            int col = orderBy[i];
            boolean reverse = reverseFlags[i];
            comparators[i] = new ColOrdering(col, reverse, nullsFirst[i]);
        }
    }

    @Override
    public void startProjection() {
        pq = new RowPriorityQueue(maxSize);
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
            return;
        }
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        Object[] evaluatedRow = evaluateRow(row);
        pq.insertWithOverflow(evaluatedRow);
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    private Object[] evaluateRow(Object[] row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        Object[] evaluatedRow = new Object[inputs.length];
        int i = 0;
        for (Input<?> input : inputs) {
            evaluatedRow[i++] = input.value();
        }
        return evaluatedRow;
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            generateResult();
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            result.setException(throwable);
        }
    }

    private void generateResult() {
        final int resultSize = Math.max(pq.size() - offset, 0);
        Object[][] rows = new Object[resultSize][];
        for (int i = resultSize - 1; i >= 0; i--) {
            rows[i] = Arrays.copyOfRange(pq.pop(), 0, numOutputs); // strip order by inputs
        }
        result.set(rows);
        pq.clear();
    }

    @Override
    public ListenableFuture<Object[][]> result() {
        return result;
    }

    @Override
    public Iterator<Object[]> iterator() throws IllegalStateException {
        if (!result.isDone()) {
            throw new IllegalStateException("result not ready.");
        }
        try {
            return new ArrayIterator(result.get(), 0, result.get().length);
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void downstream(Projector downstream) {
        throw new UnsupportedOperationException(
                "SortingTopNProjector is a ResultProvider. Doesn't support downstreams");
    }

    @Override
    public Projector downstream() {
        return null;
    }
}
