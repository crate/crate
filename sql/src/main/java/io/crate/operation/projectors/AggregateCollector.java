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

package io.crate.operation.projectors;

import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collector implementation which uses {@link AggregationFunction}s to aggregate the rows it will receive.
 */
public class AggregateCollector implements Collector<Row, Object[], Object[]> {

    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final RamAccountingContext ramAccounting;
    private final AggregationFunction[] aggregations;
    private final Version indexVersionCreated;
    private final BigArrays bigArrays;
    private final Input[][] inputs;
    private final BiConsumer<Object[], Row> accumulator;
    private final Function<Object[], Object[]> finisher;

    public AggregateCollector(List<? extends CollectExpression<Row, ?>> expressions,
                       RamAccountingContext ramAccounting,
                       AggregateMode mode,
                       AggregationFunction[] aggregations,
                       Version indexVersionCreated,
                       BigArrays bigArrays,
                       Input[]... inputs) {
        this.expressions = expressions;
        this.ramAccounting = ramAccounting;
        this.aggregations = aggregations;
        this.indexVersionCreated = indexVersionCreated;
        this.bigArrays = bigArrays;
        this.inputs = inputs;
        switch (mode) {
            case ITER_PARTIAL:
                accumulator = this::iterate;
                finisher = s -> s;
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
    public Function<Object[], Object[]> finisher() {
        return finisher;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private Object[] prepareState() {
        Object[] states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            states[i] = aggregations[i].newState(ramAccounting, indexVersionCreated, bigArrays);
        }
        return states;
    }

    private void iterate(Object[] state, Row row) {
        setRow(row);
        for (int i = 0; i < aggregations.length; i++) {
            state[i] = aggregations[i].iterate(ramAccounting, state[i], inputs[i]);
        }
    }

    private void reduce(Object[] state, Row row) {
        setRow(row);
        for (int i = 0; i < aggregations.length; i++) {
            state[i] = aggregations[i].reduce(ramAccounting, state[i], inputs[i][0].value());
        }
    }

    private Object[] finishCollect(Object[] state) {
        for (int i = 0; i < aggregations.length; i++) {
            state[i] = aggregations[i].terminatePartial(ramAccounting, state[i]);
        }
        return state;
    }

    private void setRow(Row row) {
        for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
            expressions.get(i).setNextRow(row);
        }
    }
}
