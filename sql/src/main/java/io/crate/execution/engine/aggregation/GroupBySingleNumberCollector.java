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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import io.netty.util.collection.ByteObjectHashMap;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.ShortObjectHashMap;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public final class GroupBySingleNumberCollector implements Collector<Row, GroupBySingleNumberCollector.Groups, Iterable<Row>> {

    private final DataType valueType;
    private final CollectExpression<Row, ?>[] expressions;
    private final AggregateMode mode;
    private final AggregationFunction[] aggregations;
    private final Input[][] inputs;
    private final RamAccountingContext ramAccounting;
    private final Input<Number> keyInput;
    private final Version indexVersionCreated;
    private final BigArrays bigArrays;
    private final BiConsumer<Groups, Row> accumulator;

    public static final Set<DataType<?>> SUPPORTED_TYPES = Set.of(
        DataTypes.BYTE,
        DataTypes.SHORT,
        DataTypes.INTEGER,
        DataTypes.LONG,
        DataTypes.TIMESTAMP,
        DataTypes.TIMESTAMPZ
    );

    GroupBySingleNumberCollector(DataType valueType,
                                 CollectExpression<Row, ?>[] expressions,
                                 AggregateMode mode,
                                 AggregationFunction[] aggregations,
                                 Input[][] inputs,
                                 RamAccountingContext ramAccounting,
                                 Input keyInput,
                                 Version indexVersionCreated,
                                 BigArrays bigArrays) {
        this.valueType = valueType;
        this.expressions = expressions;
        this.mode = mode;
        this.aggregations = aggregations;
        this.inputs = inputs;
        this.ramAccounting = ramAccounting;
        this.keyInput = keyInput;
        this.indexVersionCreated = indexVersionCreated;
        this.bigArrays = bigArrays;
        this.accumulator = mode == AggregateMode.PARTIAL_FINAL ? this::reduce : this::iter;
    }

    static class Groups {

        final Map<Number, Object[]> statesByKey;
        final long entryOverhead;
        Object[] statesByNullValue = null;

        Groups(Map<Number, Object[]> statesByKey, long entryOverhead) {
            this.statesByKey = statesByKey;
            this.entryOverhead = entryOverhead;
        }
    }

    @Override
    public Supplier<Groups> supplier() {
        /* The entry overhead computation is "best effort" accounting for the "entry overhead" (newStates are already
         * accounted for).
         * The LongObjectHashMap internally has a long array for keys and an object array for values
         *
         * They're not-resized per key added but capacity is doubled if it runs out
         *
         * Size has been inferred using JOL
         *
         * LongObjectHashMap
         * 24 [J                     .keys      [0]
         * 24 [Ljava.lang.Object;    .values    [null]
         *
         * 32 [J                     .keys      [10, 0]
         * 24 [Ljava.lang.Object;    .values    [(object), null]
         *
         * 48 [J                     .keys      [20, 0, 10, 0]
         * 32 [Ljava.lang.Object;    .values    [(object), null, (object), null]
         *
         * 80 [J                     .keys      [0, 0, 10, 0, 20, 0, 30, 0]
         * 48 [Ljava.lang.Object;    .values    [null, null, (object), null, (object), null, (object), null]
         *
         * The entry overhead is computed by observing the effect/key and /value on the used space when the capacity is
         * doubled eg. for Integer going from 2 items to 4 items yields an 8 byte increase in used space for both keys
         * and values (so an extra 4 byte / key and 4 byte / value) which results in a total of 8 bytes overhead/ entry.
         *
         * During the construction of the map we reserve space for 4 elements,
         * because of the defaults of 8 for initial capacity and 0.5 for load factor
         * the map starts with a size of 4 and doubles its size to 8 once the 5th element
         * is about to be added.
         *
         * The Ram accounting for new elements takes place in batches when the map is about
         * to grow by doubling its size. See #addWithAccounting()
         */
        switch (valueType.id()) {
            case ByteType.ID:
                return () -> {
                    long entryOverhead = 6L;
                    ramAccounting.addBytes(4 * entryOverhead);
                    return new Groups((Map) new ByteObjectHashMap<Object[]>(), entryOverhead);
                };

            case ShortType.ID:
                return () -> {
                    long entryOverhead = 6L;
                    ramAccounting.addBytes(4 * entryOverhead);
                    return new Groups((Map) new ShortObjectHashMap<Object[]>(), entryOverhead);
                };

            case IntegerType.ID:
                return () -> {
                    long entryOverhead = 8L;
                    ramAccounting.addBytes(4 * entryOverhead);
                    return new Groups((Map) new IntObjectHashMap<Object[]>(), entryOverhead);
                };

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return () -> {
                    long entryOverhead = 12L;
                    ramAccounting.addBytes(4 * entryOverhead);
                    return new Groups((Map) new LongObjectHashMap<Object[]>(), entryOverhead);
                };

            default:
                throw new IllegalArgumentException("Unsupported input type " + valueType.getName());
        }
    }

    @Override
    public BiConsumer<Groups, Row> accumulator() {
        return accumulator;
    }

    @Override
    public BinaryOperator<Groups> combiner() {
        return (state1, state2) -> {
            throw new UnsupportedOperationException("Combine not supported");
        };
    }

    @Override
    public Function<Groups, Iterable<Row>> finisher() {
        return this::groupsToRows;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    private void reduce(Groups groups, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        Number key = keyInput.value();
        if (key == null) {
            if (groups.statesByNullValue == null) {
                groups.statesByNullValue = statesFromInputRow();
            } else {
                mergeStatesWithInputRowInPlace(groups.statesByNullValue);
            }
        } else {
            Object[] states = groups.statesByKey.get(key);
            if (states == null) {
                addWithAccounting(groups, key, statesFromInputRow());
            } else {
                mergeStatesWithInputRowInPlace(states);
            }
        }
    }

    private void mergeStatesWithInputRowInPlace(Object[] states) {
        for (int i = 0; i < aggregations.length; i++) {
            states[i] = aggregations[i].reduce(ramAccounting, states[i], inputs[i][0].value());
        }
    }

    private Object[] statesFromInputRow() {
        Object[] states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            states[i] = inputs[i][0].value();
        }
        return states;
    }

    private void iter(Groups groups, Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        Number key = keyInput.value();
        if (key == null) {
            if (groups.statesByNullValue == null) {
                groups.statesByNullValue = createNewStates();
            } else {
                for (int i = 0; i < aggregations.length; i++) {
                    //noinspection unchecked
                    groups.statesByNullValue[i] = aggregations[i].iterate(ramAccounting, groups.statesByNullValue[i], inputs[i]);
                }
            }
        } else {
            Object[] states = groups.statesByKey.get(key);
            if (states == null) {
                addWithAccounting(groups, key, createNewStates());
            } else {
                for (int i = 0; i < aggregations.length; i++) {
                    //noinspection unchecked
                    states[i] = aggregations[i].iterate(ramAccounting, states[i], inputs[i]);
                }
            }
        }
    }

    private void addWithAccounting(Groups groups, Number key, Object[] newStates) {
        int mapSize = groups.statesByKey.size();
        // If mapSize is a power of 2 then the map is going to grow by doubling its size.
        // The first 4 elements have been calculated during the map construction.
        if (mapSize >= 4 && (mapSize & (mapSize - 1)) == 0) {
            ramAccounting.addBytes(mapSize * groups.entryOverhead);
        }

        groups.statesByKey.put(key, newStates);
    }

    private Object[] createNewStates() {
        Object[] states = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationFunction aggregation = aggregations[i];
            //noinspection unchecked
            states[i] = aggregation.iterate(
                ramAccounting,
                aggregation.newState(ramAccounting, indexVersionCreated, bigArrays),
                inputs[i]
            );
        }

        // account for the states object array
        ramAccounting.addBytes(24L);
        return states;
    }

    private Iterable<Row> groupsToRows(Groups groups) {
        final Object[] cells = new Object[1 + aggregations.length];
        final Row row = new RowN(cells);
        Supplier<Stream<Row>> rows = () -> groups.statesByKey.entrySet().stream()
            .map(entry -> {
                cells[0] = entry.getKey();
                Object[] states = entry.getValue();
                for (int i = 0; i < states.length; i++) {
                    //noinspection unchecked
                    cells[i + 1] = mode.finishCollect(ramAccounting, aggregations[i], states[i]);
                }
                return row;
            });

        if (groups.statesByNullValue == null) {
            return () -> rows.get().iterator();
        }

        Object[] nullRow = new Object[1 + aggregations.length];
        nullRow[0] = null;
        for (int i = 0; i < groups.statesByNullValue.length; i++) {
            //noinspection unchecked
            nullRow[i + 1] = mode.finishCollect(ramAccounting, aggregations[i], groups.statesByNullValue[i]);
        }
        // the Iterable contract doesn't prevent the caller from calling iterator() multiple times but java streams
        // are "one consumption" only so create a new stream on every iterator() request
        return () -> Stream.concat(rows.get(), Stream.of(new RowN(nullRow))).iterator();
    }
}
