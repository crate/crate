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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import io.crate.types.DataType;

/**
 * Collector implementation which uses {@link AggregateMode}s and {@code keyInputs}
 * to group rows by key and aggregate the grouped values.
 *
 * @param <K> type of the key
 */
public class GroupingCollector<K> implements Collector<Row, Map<K, Object[]>, Iterable<Row>> {

    private final CollectExpression<Row, ?>[] expressions;
    private final AggregationFunction[] aggregations;
    private final AggregateMode mode;
    private final Input[][] inputs;
    private final Input<Boolean>[] filters;
    private final RamAccounting ramAccounting;
    private final MemoryManager memoryManager;
    private final BiConsumer<K, Object[]> applyKeyToCells;
    private final int numKeyColumns;
    private final BiConsumer<Map<K, Object[]>, K> accountForNewEntry;
    private final Function<Row, K> keyExtractor;
    private final BiConsumer<Map<K, Object[]>, Row> accumulator;
    private final Supplier<Map<K, Object[]>> supplier;
    private final Version minNodeVersion;

    static GroupingCollector<Object> singleKey(CollectExpression<Row, ?>[] expressions,
                                               AggregateMode mode,
                                               AggregationFunction[] aggregations,
                                               Input[][] inputs,
                                               Input<Boolean>[] filters,
                                               RamAccounting ramAccounting,
                                               MemoryManager memoryManager,
                                               Version minNodeVersion,
                                               Input<?> keyInput,
                                               DataType keyType) {
        return new GroupingCollector<>(
            expressions,
            aggregations,
            mode,
            inputs,
            filters,
            ramAccounting,
            memoryManager,
            minNodeVersion,
            (key, cells) -> cells[0] = key,
            1,
            GroupByMaps.accountForNewEntry(ramAccounting, keyType),
            row -> keyInput.value(),
            GroupByMaps.mapForType(keyType)
        );
    }

    static GroupingCollector<List<Object>> manyKeys(CollectExpression<Row, ?>[] expressions,
                                                    AggregateMode mode,
                                                    AggregationFunction[] aggregations,
                                                    Input[][] inputs,
                                                    Input<Boolean>[] filters,
                                                    RamAccounting ramAccountingContext,
                                                    MemoryManager memoryManager,
                                                    Version minNodeVersion,
                                                    List<Input<?>> keyInputs,
                                                    List<? extends DataType> keyTypes) {
        return new GroupingCollector<>(
            expressions,
            aggregations,
            mode,
            inputs,
            filters,
            ramAccountingContext,
            memoryManager,
            minNodeVersion,
            GroupingCollector::applyKeysToCells,
            keyInputs.size(),
            GroupByMaps.accountForNewEntry(ramAccountingContext, keyTypes),
            row -> evalKeyInputs(keyInputs),
            HashMap::new
        );
    }

    private static List<Object> evalKeyInputs(List<Input<?>> keyInputs) {
        List<Object> key = new ArrayList<>(keyInputs.size());
        for (Input<?> keyInput : keyInputs) {
            key.add(keyInput.value());
        }
        return key;
    }

    private static void applyKeysToCells(List<Object> keys, Object[] cells) {
        for (int i = 0; i < keys.size(); i++) {
            cells[i] = keys.get(i);
        }
    }

    private GroupingCollector(CollectExpression<Row, ?>[] expressions,
                              AggregationFunction[] aggregations,
                              AggregateMode mode,
                              Input[][] inputs,
                              Input<Boolean>[] filters,
                              RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              Version minNodeVersion,
                              BiConsumer<K, Object[]> applyKeyToCells,
                              int numKeyColumns,
                              BiConsumer<Map<K, Object[]>, K> accountForNewEntry,
                              Function<Row, K> keyExtractor,
                              Supplier<Map<K, Object[]>> supplier) {
        this.expressions = expressions;
        this.aggregations = aggregations;
        this.mode = mode;
        this.inputs = inputs;
        this.filters = filters;
        this.ramAccounting = ramAccounting;
        this.memoryManager = memoryManager;
        this.applyKeyToCells = applyKeyToCells;
        this.numKeyColumns = numKeyColumns;
        this.accountForNewEntry = accountForNewEntry;
        this.keyExtractor = keyExtractor;
        this.accumulator = mode == AggregateMode.PARTIAL_FINAL ? this::reduce : this::iter;
        this.supplier = supplier;
        this.minNodeVersion = minNodeVersion;
    }

    @Override
    public Supplier<Map<K, Object[]>> supplier() {
        return supplier;
    }

    @Override
    public BiConsumer<Map<K, Object[]>, Row> accumulator() {
        return accumulator;
    }

    @Override
    public BinaryOperator<Map<K, Object[]>> combiner() {
        return (state1, state2) -> {
            throw new UnsupportedOperationException("combine not supported");
        };
    }

    @Override
    public Function<Map<K, Object[]>, Iterable<Row>> finisher() {
        return this::mapToRows;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private void reduce(Map<K, Object[]> statesByKey, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        K key = keyExtractor.apply(row);
        Object[] states = statesByKey.get(key);
        if (states == null) {
            states = new Object[aggregations.length];
            for (int i = 0; i < aggregations.length; i++) {
                states[i] = inputs[i][0].value();
            }
            addWithAccounting(statesByKey, key, states);
        } else {
            for (int i = 0; i < aggregations.length; i++) {
                states[i] = aggregations[i].reduce(ramAccounting, states[i], inputs[i][0].value());
            }
        }
    }

    private void addWithAccounting(Map<K, Object[]> statesByKey, K key, Object[] states) {
        accountForNewEntry.accept(statesByKey, key);
        statesByKey.put(key, states);
    }

    private void iter(Map<K, Object[]> statesByKey, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        K key = keyExtractor.apply(row);
        Object[] states = statesByKey.get(key);
        if (states == null) {
            addNewEntry(statesByKey, key);
        } else {
            for (int i = 0; i < aggregations.length; i++) {
                if (InputCondition.matches(filters[i])) {
                    //noinspection unchecked
                    states[i] = aggregations[i].iterate(ramAccounting, memoryManager, states[i], inputs[i]);
                }
            }
        }
    }

    private void addNewEntry(Map<K, Object[]> statesByKey, K key) {
        Object[] states;
        states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationFunction aggregation = aggregations[i];

            var newState = aggregation.newState(ramAccounting, minNodeVersion, memoryManager);
            if (InputCondition.matches(filters[i])) {
                //noinspection unchecked
                states[i] = aggregation.iterate(ramAccounting, memoryManager, newState, inputs[i]);
            } else {
                states[i] = newState;
            }
        }
        addWithAccounting(statesByKey, key, states);
    }

    private Iterable<Row> mapToRows(Map<K, Object[]> statesByKey) {

        return () -> new Iterator<>() {
            final Iterator<Map.Entry<K, Object[]>> iterator = statesByKey.entrySet().iterator();
            final RowN row = new RowN(numKeyColumns + aggregations.length);
            final Object[] cells = new Object[row.numColumns()];

            {
                row.cells(cells);
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Row next() {
                Map.Entry<K, Object[]> input = iterator.next();
                assert input != null : "input must not be null";
                applyKeyToCells.accept(input.getKey(), cells);
                int c = numKeyColumns;
                Object[] states = input.getValue();
                for (int i = 0; i < states.length; i++) {
                    cells[c] = mode.finishCollect(ramAccounting, aggregations[i], states[i]);
                    c++;
                }
                return row;
            }
        };
    }
}
