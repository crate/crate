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

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
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
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class GroupByMaps {

    private static final Map<DataType<?>, Integer> ENTRY_OVERHEAD_PER_TYPE = Map.ofEntries(
        Map.entry(DataTypes.BYTE, 6),
        Map.entry(DataTypes.SHORT, 6),
        Map.entry(DataTypes.INTEGER, 8),
        Map.entry(DataTypes.LONG, 12)
    );

    public static <K, V> BiConsumer<Map<K, V>, K> accountForNewEntry(RamAccounting ramAccounting,
                                                                     SizeEstimator<K> sizeEstimator,
                                                                     @Nullable DataType<K> type) {
        Integer entryOverHead = type == null ? null : ENTRY_OVERHEAD_PER_TYPE.get(type);
        if (entryOverHead == null) {
            return (map, k) -> ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(k) + 36));
        } else {
            return (map, k) -> {
                int mapSize = map.size();
                // If mapSize is a power of 2 then the map is going to grow by doubling its size.
                if (mapSize >= 4 && (mapSize & (mapSize - 1)) == 0) {
                    ramAccounting.addBytes(mapSize * (long) entryOverHead);
                }
            };
        }
    }

    public static <K, V> Supplier<Map<K, V>> mapForType(DataType<K> type) {
        switch (type.id()) {
            case ByteType.ID:
                return () -> (Map) new PrimitiveMapWithNulls<>(new ByteObjectHashMap<>());
            case ShortType.ID:
                return () -> (Map) new PrimitiveMapWithNulls<>(new ShortObjectHashMap<>());
            case IntegerType.ID:
                return () -> (Map) new PrimitiveMapWithNulls<>(new IntObjectHashMap<>());

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return () -> (Map) new PrimitiveMapWithNulls<>(new LongObjectHashMap<>());

            default:
                return HashMap::new;
        }
    }
}
