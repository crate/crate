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

import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

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
    private final RamAccountingContext ramAccountingContext;
    private final BiConsumer<K, Object[]> applyKeyToCells;
    private final int numKeyColumns;
    private final SizeEstimator<K> keySizeEstimator;
    private final Function<Row, K> keyExtractor;
    private final Version indexVersionCreated;
    private final BigArrays bigArrays;

    static GroupingCollector<Object> singleKey(CollectExpression<Row, ?>[] expressions,
                                               AggregateMode mode,
                                               AggregationFunction[] aggregations,
                                               Input[][] inputs,
                                               RamAccountingContext ramAccountingContext,
                                               Input<?> keyInput,
                                               DataType keyType,
                                               Version indexVersionCreated,
                                               BigArrays bigArrays) {
        return new GroupingCollector<>(
            expressions,
            aggregations,
            mode,
            inputs,
            ramAccountingContext,
            (key, cells) -> cells[0] = key,
            1,
            SizeEstimatorFactory.create(keyType),
            row -> keyInput.value(),
            indexVersionCreated,
            bigArrays
        );
    }

    static GroupingCollector<List<Object>> manyKeys(CollectExpression<Row, ?>[] expressions,
                                                    AggregateMode mode,
                                                    AggregationFunction[] aggregations,
                                                    Input[][] inputs,
                                                    RamAccountingContext ramAccountingContext,
                                                    List<Input<?>> keyInputs,
                                                    List<? extends DataType> keyTypes,
                                                    Version indexVersionCreated,
                                                    BigArrays bigArrays) {
        return new GroupingCollector<>(
            expressions,
            aggregations,
            mode,
            inputs,
            ramAccountingContext,
            GroupingCollector::applyKeysToCells,
            keyInputs.size(),
            new MultiSizeEstimator(keyTypes),
            row -> evalKeyInputs(keyInputs),
            indexVersionCreated,
            bigArrays
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
                              RamAccountingContext ramAccountingContext,
                              BiConsumer<K, Object[]> applyKeyToCells,
                              int numKeyColumns,
                              SizeEstimator<K> keySizeEstimator,
                              Function<Row, K> keyExtractor,
                              Version indexVersionCreated,
                              BigArrays bigArrays) {
        this.expressions = expressions;
        this.aggregations = aggregations;
        this.mode = mode;
        this.inputs = inputs;
        this.ramAccountingContext = ramAccountingContext;
        this.applyKeyToCells = applyKeyToCells;
        this.numKeyColumns = numKeyColumns;
        this.keySizeEstimator = keySizeEstimator;
        this.keyExtractor = keyExtractor;
        this.indexVersionCreated = indexVersionCreated;
        this.bigArrays = bigArrays;
    }

    @Override
    public Supplier<Map<K, Object[]>> supplier() {
        return HashMap::new;
    }

    @Override
    public BiConsumer<Map<K, Object[]>, Row> accumulator() {
        return this::onNextRow;
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

    private void onNextRow(Map<K, Object[]> statesByKey, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        K key = keyExtractor.apply(row);
        Object[] states = statesByKey.get(key);
        if (states == null) {
            addNewEntry(statesByKey, key);
        } else {
            for (int i = 0; i < aggregations.length; i++) {
                states[i] = mode.onRow(ramAccountingContext, aggregations[i], states[i], inputs[i]);
            }
        }
    }

    private void addNewEntry(Map<K, Object[]> statesByKey, K key) {
        Object[] states;
        states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationFunction aggregation = aggregations[i];
            states[i] = mode.onRow(
                ramAccountingContext, aggregation,
                aggregation.newState(ramAccountingContext, indexVersionCreated, bigArrays), inputs[i]);
        }
        // key size + 32 bytes for entry + 4 bytes for increased capacity
        ramAccountingContext.addBytes(
            RamAccountingContext.roundUp(keySizeEstimator.estimateSize(key) + 36L));
        statesByKey.put(key, states);
    }

    private Iterable<Row> mapToRows(Map<K, Object[]> statesByKey) {
        return Iterables.transform(statesByKey.entrySet(), new com.google.common.base.Function<Map.Entry<K, Object[]>, Row>() {

            RowN row = new RowN(numKeyColumns + aggregations.length);
            Object[] cells = new Object[row.numColumns()];

            {
                row.cells(cells);
            }

            @Nullable
            @Override
            public Row apply(@Nullable Map.Entry<K, Object[]> input) {
                assert input != null : "input must not be null";

                applyKeyToCells.accept(input.getKey(), cells);
                int c = numKeyColumns;
                Object[] states = input.getValue();
                for (int i = 0; i < states.length; i++) {
                    cells[c] = mode.finishCollect(ramAccountingContext, aggregations[i], states[i]);
                    c++;
                }
                return row;
            }
        });
    }


    private static class MultiSizeEstimator extends SizeEstimator<List<Object>> {

        private final List<SizeEstimator<Object>> subEstimators;

        MultiSizeEstimator(List<? extends DataType> keyTypes) {
            subEstimators = new ArrayList<>(keyTypes.size());
            for (DataType keyType : keyTypes) {
                subEstimators.add(SizeEstimatorFactory.create(keyType));
            }
        }

        @Override
        public long estimateSize(@Nullable List<Object> value) {
            assert value != null && value.size() == subEstimators.size()
                : "value must have the same number of items as there are keyTypes/sizeEstimators";

            long size = 0;
            for (int i = 0; i < value.size(); i++) {
                size += subEstimators.get(i).estimateSize(value.get(i));
            }
            return size;
        }
    }
}
