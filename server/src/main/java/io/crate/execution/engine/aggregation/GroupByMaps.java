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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.lucene.util.RamUsageEstimator;

import io.crate.breaker.RamAccounting;
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

    public static <K, V> BiConsumer<Map<K, V>, K> accountForNewEntry(RamAccounting ramAccounting, DataType<K> type) {
        return (map, k) -> ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(type.valueBytes(k) + 36));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <K, V> BiConsumer<Map<K, V>, K> accountForNewEntry(RamAccounting ramAccounting,
                                                                     List<? extends DataType> types) {
        return (map, k) -> {
            assert k instanceof List : "keys must be a list if there are multiple key types";
            long size = 0;
            for (int i = 0; i < types.size(); i++) {
                DataType dataType = types.get(i);
                Object value = ((List) k).get(i);
                size += dataType.valueBytes(value);
            }
            ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(size + 36));
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
