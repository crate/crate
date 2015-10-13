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
import com.google.common.collect.Sets;
import io.crate.Constants;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.RowPriorityQueue;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

public class SortingTopNProjector extends AbstractProjector {

    private final int offset;
    private final int numOutputs;

    private final RowPriorityQueue<Object[]> pq;
    private final Collection<? extends Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private Object[] spare;
    private Set<Requirement> requirements;

    /**
     * @param inputs             contains output {@link io.crate.operation.Input}s and orderBy {@link io.crate.operation.Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link io.crate.operation.Input}s starting form index 0
     * @param ordering           ordering that is used to compare the rows
     * @param limit              the number of rows to gather, pass to upStream
     * @param offset             the initial offset, this number of rows are skipped
     */
    public SortingTopNProjector(Collection<? extends Input<?>> inputs,
                                Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                int numOutputs,
                                Ordering<Object[]> ordering,
                                int limit,
                                int offset) {
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset >= 0, "invalid offset");

        this.inputs = inputs;
        this.numOutputs = numOutputs;
        this.collectExpressions = collectExpressions;
        this.offset = offset;

        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        int maxSize = this.offset + limit;
        pq = new RowPriorityQueue<>(maxSize, ordering);
    }

    @Override
    public boolean setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        if (spare == null) {
            spare = new Object[inputs.size()];
        }
        int i = 0;
        for (Input<?> input : inputs) {
            spare[i++] = input.value();
        }
        spare = pq.insertWithOverflow(spare);
        return true;
    }

    @Override
    public void finish() {
        final int resultSize = Math.max(pq.size() - offset, 0);
        if (resultSize == 0) {
            downstream.finish();
            return;
        }
        IterableRowEmitter rowEmitter = createRowEmitter(resultSize);
        rowEmitter.run();
    }

    private IterableRowEmitter createRowEmitter(int resultSize) {
        Object[][] rows = new Object[resultSize][];
        for (int i = resultSize - 1; i >= 0; i--) {
            rows[i] = pq.pop();
        }
        return new IterableRowEmitter(downstream, executionState, new ArrayBucket(rows, numOutputs));
    }

    @Override
    public void fail(Throwable t) {
        downstream.fail(t);
    }

    @Override
    public void repeat() {
        final int resultSize = Math.max(pq.size() - offset, 0);
        IterableRowEmitter rowEmitter = createRowEmitter(resultSize);
        rowEmitter.run();
    }

    @Override
    public Set<Requirement> requirements() {
        if (requirements == null) {
            requirements = Sets.newEnumSet(downstream.requirements(), Requirement.class);
            requirements.remove(Requirement.REPEAT);
        }
        return requirements;
    }
}
