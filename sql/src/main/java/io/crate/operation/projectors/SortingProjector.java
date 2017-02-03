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
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;

import java.util.*;

/**
 * Sort rows by ordering criteria and process given offset before emitting.
 * <p>
 * Compared to {@link SortingTopNProjector} this projector does not support limiting,
 * while the {@link SortingTopNProjector} does not work WITHOUT a limit.
 */
class SortingProjector extends AbstractProjector {

    private final Collection<? extends Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private Set<Requirement> requirements;

    private final Ordering<Object[]> ordering;
    private final int offset;
    private final int numOutputs;
    private final List<Object[]> rows = new ArrayList<>();
    private IterableRowEmitter rowEmitter = null;

    /**
     * @param inputs             contains output {@link io.crate.operation.Input}s and orderBy {@link io.crate.operation.Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link io.crate.operation.Input}s starting form index 0
     * @param ordering           ordering that is used to compare the rows
     * @param offset             the initial offset, this number of rows are skipped
     */
    SortingProjector(Collection<? extends Input<?>> inputs,
                     Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                     int numOutputs,
                     Ordering<Object[]> ordering,
                     int offset) {
        Preconditions.checkArgument(offset >= 0, "invalid offset %s", offset);
        this.numOutputs = numOutputs;
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        this.ordering = ordering;
        this.offset = offset;
    }

    @Override
    public Result setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        Object[] newRow = new Object[inputs.size()];
        int i = 0;
        for (Input<?> input : inputs) {
            newRow[i++] = input.value();
        }
        rows.add(newRow);
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        // sort, we must reverse the order (back to original one) because order was reserved for used on queues
        Collections.sort(rows, Collections.reverseOrder(ordering));

        // emit
        rowEmitter = createRowEmitter();
        rowEmitter.run();
    }

    private IterableRowEmitter createRowEmitter() {
        CollectionBucket collectionBucket;
        // process offset
        if (offset != 0) {
            collectionBucket = new CollectionBucket(rows.subList(offset, rows.size()), numOutputs);
        } else {
            collectionBucket = new CollectionBucket(rows, numOutputs);
        }

        return new IterableRowEmitter(downstream, collectionBucket);
    }

    @Override
    public void kill(Throwable throwable) {
        IterableRowEmitter emitter = rowEmitter;
        if (emitter == null) {
            downstream.kill(throwable);
        } else {
            emitter.kill(throwable);
        }
    }

    @Override
    public void fail(Throwable t) {
        downstream.fail(t);
    }

    @Override
    public Set<Requirement> requirements() {
        if (requirements == null) {
            requirements = Requirements.remove(downstream.requirements(), Requirement.REPEAT);
        }
        return requirements;
    }
}
