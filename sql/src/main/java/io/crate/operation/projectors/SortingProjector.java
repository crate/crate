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
import io.crate.data.*;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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

    private final Comparator<Object[]> comparator;
    private final int offset;
    private final int numOutputs;
    private final List<Object[]> rows = new ArrayList<>();
    private IterableRowEmitter rowEmitter = null;

    /**
     * @param inputs             contains output {@link Input}s and orderBy {@link Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link Input}s starting form index 0
     * @param comparator         ordering that is used to compare the rows
     * @param offset             the initial offset, this number of rows are skipped
     */
    SortingProjector(Collection<? extends Input<?>> inputs,
                     Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                     int numOutputs,
                     Comparator<Object[]> comparator,
                     int offset) {
        Preconditions.checkArgument(offset >= 0, "invalid offset %s", offset);
        this.numOutputs = numOutputs;
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        this.comparator = comparator;
        this.offset = offset;
    }

    @Override
    public Result setNextRow(Row row) {
        Object[] newRow = getCells(row);
        rows.add(newRow);
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        rowEmitter = new IterableRowEmitter(downstream, sortAndCreateBucket(rows));
        rowEmitter.run();
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

    @Nullable
    @Override
    public Function<BatchIterator, Tuple<BatchIterator, RowReceiver>> batchIteratorProjection() {
        return bi -> {
            Collector<Row, ?, Bucket> collector = Collectors.mapping(
                this::getCells,
                Collectors.collectingAndThen(Collectors.toList(), this::sortAndCreateBucket));
            return new Tuple<>(CollectingBatchIterator.newInstance(bi, collector, numOutputs), downstream);
        };
    }

    private Object[] getCells(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        Object[] newRow = new Object[inputs.size()];
        int i = 0;
        for (Input<?> input : inputs) {
            newRow[i++] = input.value();
        }
        return newRow;
    }

    private Bucket sortAndCreateBucket(List<Object[]> rows) {
        rows.sort(comparator.reversed());
        if (offset == 0) {
            return new CollectionBucket(rows, numOutputs);
        }
        return new CollectionBucket(rows.subList(offset, rows.size()), numOutputs);
    }
}
