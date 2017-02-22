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

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.sorting.RowPriorityQueue;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SortingTopNProjector extends AbstractProjector {

    private final SortingTopNCollector collector;
    private final BiConsumer<RowPriorityQueue<Object[]>, Row> accumulator;
    private final RowPriorityQueue<Object[]> pq;
    private Set<Requirement> requirements;
    private volatile IterableRowEmitter rowEmitter = null;

    /**
     * @param inputs             contains output {@link Input}s and orderBy {@link Input}s
     * @param collectExpressions gathered from outputs and orderBy inputs
     * @param numOutputs         <code>inputs</code> contains this much output {@link Input}s starting form index 0
     * @param ordering           ordering that is used to compare the rows
     * @param limit              the number of rows to gather, pass to upStream
     * @param offset             the initial offset, this number of rows are skipped
     */
    public SortingTopNProjector(Collection<? extends Input<?>> inputs,
                                Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                int numOutputs,
                                Comparator<Object[]> ordering,
                                int limit,
                                int offset) {
        collector = new SortingTopNCollector(
            inputs,
            collectExpressions,
            numOutputs,
            ordering,
            limit,
            offset
        );
        pq = collector.supplier().get();
        accumulator = collector.accumulator();
    }

    @Override
    public Result setNextRow(Row row) {
        accumulator.accept(pq, row);
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        rowEmitter = new IterableRowEmitter(downstream, collector.finisher().apply(pq));
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
        return bi -> new Tuple<>(CollectingBatchIterator.newInstance(bi, collector), downstream);
    }
}
