/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.elasticsearch.Version;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputCondition;
import io.crate.expression.symbol.AggregateMode;
import io.crate.memory.MemoryManager;

/**
 * Collector implementation which uses {@link AggregationFunction}s to aggregate the rows it will receive.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregateCollector implements Collector<Row, Object[], Iterable<Row>> {

    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final RamAccounting ramAccounting;
    private final MemoryManager memoryManager;
    private final AggregationFunction[] aggregations;
    private final Input<Boolean>[] filters;
    private final Input[][] inputs;
    private final BiConsumer<Object[], Row> accumulator;
    private final Function<Object[], Iterable<Row>> finisher;
    private final Version minNodeVersion;

    public AggregateCollector(List<? extends CollectExpression<Row, ?>> expressions,
                              RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              Version minNodeVersion,
                              AggregateMode mode,
                              AggregationFunction<?, ?>[] aggregations,
                              Input<?>[][] inputs,
                              Input<Boolean>[] filters) {
        this.expressions = expressions;
        this.ramAccounting = ramAccounting;
        this.memoryManager = memoryManager;
        this.minNodeVersion = minNodeVersion;
        this.aggregations = aggregations;
        this.filters = filters;
        this.inputs = inputs;
        switch (mode) {
            case ITER_PARTIAL:
                accumulator = this::iterate;
                finisher = s -> Collections.singletonList(new RowN(s));
                break;

            case ITER_FINAL:
                accumulator = this::iterate;
                finisher = this::finishCollect;
                break;

            case PARTIAL_FINAL:
                accumulator = this::reduce;
                finisher = this::finishCollect;
                break;

            default:
                throw new AssertionError("Invalid mode: " + mode.name());
        }
    }

    @Override
    public Supplier<Object[]> supplier() {
        return this::prepareState;
    }

    @Override
    public BiConsumer<Object[], Row> accumulator() {
        return accumulator;
    }

    @Override
    public BinaryOperator<Object[]> combiner() {
        return (state1, state2) -> {
            throw new UnsupportedOperationException("combine not supported");
        };
    }

    @Override
    public Function<Object[], Iterable<Row>> finisher() {
        return finisher;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private Object[] prepareState() {
        Object[] states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            states[i] = aggregations[i].newState(ramAccounting, minNodeVersion, memoryManager);
        }
        return states;
    }

    private void iterate(Object[] state, Row row) {
        setRow(row);
        for (int i = 0; i < aggregations.length; i++) {
            if (InputCondition.matches(filters[i])) {
                state[i] = aggregations[i].iterate(ramAccounting, memoryManager, state[i], inputs[i]);
            }
        }
    }

    private void reduce(Object[] state, Row row) {
        setRow(row);
        for (int i = 0; i < aggregations.length; i++) {
            state[i] = aggregations[i].reduce(ramAccounting, state[i], inputs[i][0].value());
        }
    }

    private Iterable<Row> finishCollect(Object[] state) {
        for (int i = 0; i < aggregations.length; i++) {
            state[i] = aggregations[i].terminatePartial(ramAccounting, state[i]);
        }
        return Collections.singletonList(new RowN(state));
    }

    private void setRow(Row row) {
        for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
            expressions.get(i).setNextRow(row);
        }
    }
}
