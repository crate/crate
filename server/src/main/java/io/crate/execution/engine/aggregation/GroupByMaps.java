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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.lucene.util.RamUsageEstimator;

import io.crate.common.TriConsumer;
import io.crate.data.breaker.RamAccounting;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import io.netty.util.collection.ByteObjectHashMap;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.ShortObjectHashMap;

public final class GroupByMaps {

    public static <K, V> TriConsumer<ResizeAwareMap<K, V>, K, Object[]> accountForNewEntry(RamAccounting ramAccounting, DataType<K> type) {
        return (map, key, states) -> {
            long keyBytes = RamUsageEstimator.alignObjectSize(type.valueBytes(key) + 36);
            long statesShallowBytes = RamUsageEstimator.shallowSizeOf(states);
            ramAccounting.addBytes(keyBytes + statesShallowBytes + map.expectedCapacityIncreaseBytes());
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <K, V> TriConsumer<ResizeAwareMap<K, V>, K, Object[]> accountForNewEntry(RamAccounting ramAccounting,
                                                                 List<? extends DataType> types) {
        return (map, key, states) -> {
            assert key instanceof List : "keys must be a list if there are multiple key types";
            long size = 0;
            for (int i = 0; i < types.size(); i++) {
                DataType dataType = types.get(i);
                Object value = ((List) key).get(i);
                size += dataType.valueBytes(value);
            }
            long keyBytes = RamUsageEstimator.alignObjectSize(size + 36);
            long statesShallowBytes = RamUsageEstimator.shallowSizeOf(states);
            ramAccounting.addBytes(keyBytes + statesShallowBytes + map.expectedCapacityIncreaseBytes());
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <K, V> Supplier<ResizeAwareMap<K, V>> mapForType(DataType<K> type) {
        return switch (type.id()) {
            case ByteType.ID -> () -> (ResizeAwareMap<K, V>) new ResizeAwareMap<>(
                new PrimitiveMapWithNulls<>(new ByteObjectHashMap<>()),
                ByteObjectHashMap.DEFAULT_CAPACITY,
                ByteObjectHashMap.DEFAULT_LOAD_FACTOR,
                true,
                Byte.BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );

            case ShortType.ID -> () -> (ResizeAwareMap<K, V>) new ResizeAwareMap<>(
                new PrimitiveMapWithNulls<>(new ShortObjectHashMap<>()),
                ShortObjectHashMap.DEFAULT_CAPACITY,
                ShortObjectHashMap.DEFAULT_LOAD_FACTOR,
                true,
                Short.BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );

            case IntegerType.ID -> () -> (ResizeAwareMap<K, V>) new ResizeAwareMap<>(
                new PrimitiveMapWithNulls<>(new IntObjectHashMap<>()),
                IntObjectHashMap.DEFAULT_CAPACITY,
                IntObjectHashMap.DEFAULT_LOAD_FACTOR,
                true,
                Integer.BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );

            case LongType.ID, TimestampType.ID_WITH_TZ, TimestampType.ID_WITHOUT_TZ ->
                () -> (ResizeAwareMap<K, V>) new ResizeAwareMap<>(
                    new PrimitiveMapWithNulls<>(new LongObjectHashMap<>()),
                    LongObjectHashMap.DEFAULT_CAPACITY,
                    LongObjectHashMap.DEFAULT_LOAD_FACTOR,
                    true,
                    Long.BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                );

            default -> () -> wrapperForJDKMap(new HashMap<>());
        };
    }

    public static <K, V> ResizeAwareMap<K, V> wrapperForJDKMap(Map<K, V> map) {
        return new ResizeAwareMap<>(
            map,
            16, // HashMap.DEFAULT_INITIAL_CAPACITY
            0.75f, // HashMap.DEFAULT_LOAD_FACTOR
            false,
            RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }


}
