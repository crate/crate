/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowFilter;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.symbol.Literal;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;

public class RowsCollector<R> implements CrateCollector, ExecutionState {

    private final IterableRowEmitter emitter;
    private volatile boolean killed;

    public static <T> RowsCollector<T> empty(RowReceiver rowDownstream) {
        return new RowsCollector<>(
                ImmutableList.<Input<?>>of(),
                ImmutableList.<CollectExpression<T, ?>>of(),
                rowDownstream,
                ImmutableList.<T>of(),
                Literal.BOOLEAN_FALSE
        );
    }

    public static RowsCollector<Row> single(List<Input<?>> inputs, RowReceiver rowDownstream) {
        return new RowsCollector<>(
                inputs,
                ImmutableList.<CollectExpression<Row, ?>>of(),
                rowDownstream,
                ImmutableList.<Row>of(new InputRow(inputs)),
                Literal.BOOLEAN_TRUE
        );
    }

    public RowsCollector(final List<Input<?>> inputs,
                         final Collection<CollectExpression<R, ?>> collectExpressions,
                         RowReceiver rowDownstream,
                         Iterable<R> rows,
                         final Input<Boolean> condition) {

        this.emitter = new IterableRowEmitter(
                rowDownstream,
                this,
                Iterables.filter(Iterables.transform(rows, new Function<R, Row>() {

                            final RowFilter<R> rowFilter = new RowFilter<>(collectExpressions, condition);
                            final Row row = new InputRow(inputs);

                            @Nullable
                            @Override
                            public Row apply(@Nullable R input) {
                                if (killed) {
                                    throw new CancellationException();
                                }
                                if (rowFilter.matches(input)) {
                                    return row;
                                }
                                return null;
                            }
                        }

                ), Predicates.notNull()));
    }

    @Override
    public void doCollect() {
        emitter.run();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed = true;
    }

    @Override
    public boolean isKilled() {
        return killed;
    }
}
