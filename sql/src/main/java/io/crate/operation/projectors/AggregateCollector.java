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

import io.crate.data.Row;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collector implementation which uses {@link Aggregator}s to aggregate the rows it will receive.
 */
public class AggregateCollector implements Collector<Row, Object[], Object[]> {

    private final Iterable<? extends CollectExpression<Row, ?>> expressions;
    private final Aggregator[] aggregators;

    public AggregateCollector(Iterable<? extends CollectExpression<Row, ?>> expressions,
                              Aggregator[] aggregators) {
        this.expressions = expressions;
        this.aggregators = aggregators;
    }

    @Override
    public Supplier<Object[]> supplier() {
        return this::prepareState;
    }

    @Override
    public BiConsumer<Object[], Row> accumulator() {
        return this::processRow;
    }

    @Override
    public BinaryOperator<Object[]> combiner() {
        return (state1, state2) -> { throw new UnsupportedOperationException("combine not supported"); };
    }

    @Override
    public Function<Object[], Object[]> finisher() {
        return this::finishCollect;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private Object[] prepareState() {
        Object[] states = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
            states[i] = aggregators[i].prepareState();
        }
        return states;
    }

    private void processRow(Object[] state, Row item) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(item);
        }
        for (int i = 0; i < aggregators.length; i++) {
            state[i] = aggregators[i].processRow(state[i]);
        }
    }

    private Object[] finishCollect(Object[] state) {
        Object[] cells = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
            cells[i] = aggregators[i].finishCollect(state[i]);
        }
        return cells;
    }
}
