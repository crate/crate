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
import io.crate.Constants;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.distributed.ResultProviderBase;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.projectors.sorting.RowPriorityQueue;

import java.util.Comparator;

public class SortingTopNProjector extends ResultProviderBase implements Projector, RowUpstream, RowDownstreamHandle {

    private final int offset;
    private final int maxSize;
    private final int numOutputs;

    private RowDownstreamHandle downstream;

    private RowPriorityQueue<Object[]> pq;
    private final Comparator[] comparators;
    private final Input<?>[] inputs;
    private final CollectExpression<?>[] collectExpressions;
    private Object[] spare;

    /**
     * @param inputs             contains output {@link io.crate.operation.Input}s and orderBy {@link io.crate.operation.Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link io.crate.operation.Input}s starting form index 0
     * @param orderBy            indices of {@link io.crate.operation.Input}s in parameter <code>inputs</code> we sort by
     * @param reverseFlags       for every index orderBy a boolean indicates ascending (<code>false</code>) or descending (<code>true</code>) order
     * @param limit              the number of rows to gather, pass to upStream
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
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset >= 0, "invalid offset");
        assert nullsFirst.length == reverseFlags.length;

        this.inputs = inputs;
        this.numOutputs = numOutputs;
        this.collectExpressions = collectExpressions;
        this.offset = offset;

        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.maxSize = this.offset + limit;
        comparators = new Comparator[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            int col = orderBy[i];
            boolean reverse = reverseFlags[i];
            comparators[i] = OrderingByPosition.arrayOrdering(col, reverse, nullsFirst[i]);
        }
    }

    @Override
    public void startProjection() {
        super.startProjection();
        synchronized (this){
            if (pq==null) {
                pq = new RowPriorityQueue<>(maxSize, comparators);
            }
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (spare == null) {
            spare = new Object[inputs.length];
        }
        evaluateRow(row);
        spare = pq.insertWithOverflow(spare);
        return true;
    }

    private synchronized void evaluateRow(Row row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        int i = 0;
        for (Input<?> input : inputs) {
            spare[i++] = input.value();
        }
    }

    @Override
    public Bucket doFinish() {
        Bucket bucket;
        if (pq != null){
            final int resultSize = Math.max(pq.size() - offset, 0);
            Object[][] rows = new Object[resultSize][];
            for (int i = resultSize - 1; i >= 0; i--) {
                rows[i] = pq.pop();
            }
            pq.clear();
            bucket = new ArrayBucket(rows, numOutputs);
        } else {
            bucket = Bucket.EMPTY;
        }
        if (downstream != null) {
            for (Row row : bucket) {
                downstream.setNextRow(row);
            }
            downstream.finish();
        }
        return bucket;
    }

    @Override
    public Throwable doFail(Throwable t) {
        if (downstream != null) {
            downstream.fail(t);
        }
        return t;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }


}
