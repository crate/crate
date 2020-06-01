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

package io.crate.testing;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.BiasedNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimeTZParser;
import io.crate.types.TimeTZType;
import io.crate.types.TimestampType;
import org.joda.time.Period;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

public class DataTypeTesting {

    public static final List<DataType> ALL_TYPES_EXCEPT_ARRAYS = ImmutableList.<DataType>builder()
        .addAll(DataTypes.PRIMITIVE_TYPES)
        .add(DataTypes.GEO_POINT)
        .add(DataTypes.GEO_SHAPE)
        .add(DataTypes.INTERVAL)
        .add(DataTypes.UNTYPED_OBJECT)
        .build();

    public static DataType<?> randomType() {
        return RandomPicks.randomFrom(RandomizedContext.current().getRandom(), ALL_TYPES_EXCEPT_ARRAYS);
    }

    @SuppressWarnings("unchecked")
    public static <T> Supplier<T> getDataGenerator(DataType<T> type) {
        Random random = RandomizedContext.current().getRandom();
        switch (type.id()) {
            case ByteType.ID:
                return () -> (T) (Byte) (byte) random.nextInt(Byte.MAX_VALUE);
            case BooleanType.ID:
                return () -> (T) (Boolean) random.nextBoolean();

            case StringType.ID:
                return () -> (T) RandomizedTest.randomAsciiLettersOfLength(random.nextInt(10));

            case IpType.ID:
                return () -> {
                    if (random.nextBoolean()) {
                        return (T) randomIPv4Address(random);
                    } else {
                        return (T) randomIPv6Address(random);
                    }
                };

            case DoubleType.ID:
                return () -> (T) (Double) random.nextDouble();

            case FloatType.ID:
                return () -> (T) (Float) random.nextFloat();

            case ShortType.ID:
                return () -> (T) (Short) (short) random.nextInt(Short.MAX_VALUE);

            case IntegerType.ID:
                return () -> (T) (Integer) random.nextInt();

            case TimeTZType.ID:
                return () -> (T) (Long) Math.min(Math.abs(random.nextLong()), TimeTZParser.MAX_MICROS);

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return () -> (T) (Long) random.nextLong();

            case GeoPointType.ID:
                return () -> (T) new PointImpl(
                    BiasedNumbers.randomDoubleBetween(random, -180, 180),
                    BiasedNumbers.randomDoubleBetween(random, -90, 90),
                    JtsSpatialContext.GEO
                );

            case GeoShapeType.ID:
                return () -> {
                    // Can't use immutable Collections.singletonMap; insert-analyzer mutates the map
                    Map<String, Object> geoShape = new HashMap<>(2);
                    geoShape.put("coordinates", Arrays.asList(10.2d, 32.2d));
                    geoShape.put("type", "Point");
                    return (T) geoShape;
                };

            case ObjectType.ID:
                Supplier<?> innerValueGenerator = getDataGenerator(randomType());
                return () -> {
                    // Can't use immutable Collections.singletonMap; insert-analyzer mutates the map
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("x", innerValueGenerator.get());
                    return (T) map;
                };
            case IntervalType.ID:
                return () -> {
                    return (T) new Period().withSeconds(RandomNumbers.randomIntBetween(random, 0, Integer.MAX_VALUE));
                };

        }

        throw new AssertionError("No data generator for type " + type.getName());
    }

    private static String randomIPv6Address(Random random) {
        String[] parts = new String[8];
        for (int i = 0; i < 8; i++) {
            parts[i] = Integer.toHexString(random.nextInt(2 ^ 16));
        }
        return Joiner.on(":").join(parts);
    }

    private static String randomIPv4Address(Random random) {
        return (random.nextInt(255) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256);
    }
}
