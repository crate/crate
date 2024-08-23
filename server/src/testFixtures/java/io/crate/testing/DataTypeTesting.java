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

package io.crate.testing;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.joda.time.Period;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.BiasedNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.sql.tree.BitString;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.ObjectType.Builder;
import io.crate.types.Regclass;
import io.crate.types.RegclassType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import io.crate.types.UndefinedType;

public class DataTypeTesting {

    public static Set<DataType<?>> getStorableTypesExceptArrays(Random random) {
        return DataTypes.TYPES_BY_NAME_OR_ALIAS
            .values().stream()
            .map(type -> {
                // Numeric needs precision to support storage
                if (type instanceof NumericType) {
                    int precision = random.nextInt(2, 39);
                    int scale = random.nextInt(0, precision - 1);
                    return new NumericType(precision, scale);
                }
                return type;
            })
            .filter(x -> x.storageSupport() != null && x.id() != ArrayType.ID && x.id() != UndefinedType.ID)
            .collect(Collectors.toSet());
    }

    public static DataType<?> randomType() {
        Random random = RandomizedContext.current().getRandom();
        return RandomPicks.randomFrom(random, getStorableTypesExceptArrays(random));
    }

    public static DataType<?> randomTypeExcluding(Set<DataType<?>> excluding) {
        Random random = RandomizedContext.current().getRandom();
        Set<DataType<?>> pickFrom = getStorableTypesExceptArrays(random).stream()
            .filter(t -> excluding.contains(t) == false)
            .collect(Collectors.toSet());
        return RandomPicks.randomFrom(random, pickFrom);
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
                Integer maxLength = type.characterMaximumLength();
                return () -> {
                    int length = maxLength == null ? random.nextInt(10) : random.nextInt(maxLength + 1);
                    return (T) RandomizedTest.randomAsciiLettersOfLength(length);
                };

            case CharacterType.ID:
                return () -> (T) RandomizedTest.randomAsciiLettersOfLength(type.characterMaximumLength());

            case IpType.ID:
                return () -> {
                    if (random.nextBoolean()) {
                        return (T) NetworkAddress.format(InetAddresses.forString(randomIPv4Address(random)));
                    } else {
                        return (T) NetworkAddress.format(InetAddresses.forString(randomIPv6Address(random)));
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

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case DateType.ID :
                return () -> (T) (Long) random.nextLong();

            case RegclassType.ID:
                return () -> {
                    int oid = random.nextInt();
                    return (T) new Regclass(oid, String.valueOf(oid));
                };

            case GeoPointType.ID:
                return () -> (T) new PointImpl(
                    BiasedNumbers.randomDoubleBetween(random, -180, 180),
                    BiasedNumbers.randomDoubleBetween(random, -90, 90),
                    JtsSpatialContext.GEO
                );

            case GeoShapeType.ID:
                return () -> {
                    // Can't use immutable Collections.singletonMap; insert-analyzer mutates the map
                    Map<String, Object> geoShape = HashMap.newHashMap(2);
                    geoShape.put(
                        "coordinates",
                        Arrays.asList(
                            BiasedNumbers.randomDoubleBetween(random, -180, 180),
                            BiasedNumbers.randomDoubleBetween(random, -90, 90)
                        )
                    );
                    geoShape.put("type", "Point");
                    return (T) geoShape;
                };

            case ObjectType.ID:
                ObjectType objectType = (ObjectType) type;
                final Map<String, Supplier<?>> typeGen;
                if (objectType.innerTypes().isEmpty()) {
                    typeGen = Map.of("x", getDataGenerator(randomType()));
                } else {
                    typeGen = new HashMap<>();
                    for (var entry : objectType.innerTypes().entrySet()) {
                        String columnName = entry.getKey();
                        DataType<?> innerType = entry.getValue();
                        typeGen.put(columnName, getDataGenerator(innerType));
                    }
                }
                return () -> {
                    // Can't use immutable Collections.singletonMap; insert-analyzer mutates the map
                    HashMap<String, Object> map = new HashMap<>();
                    for (var entry : typeGen.entrySet()) {
                        String column = entry.getKey();
                        Supplier<?> valueGenerator = entry.getValue();
                        map.put(column, valueGenerator.get());
                    }
                    return (T) map;
                };

            case IntervalType.ID:
                return () -> (T) new Period().withSeconds(RandomNumbers.randomIntBetween(random, 0, Integer.MAX_VALUE));

            case NumericType.ID:
                return () -> {
                    var numericType = (NumericType) type;
                    Integer precision = numericType.numericPrecision();
                    int scale = numericType.scale() == null ? 0 : numericType.scale();
                    int maxDigits = precision == null ? 131072 : precision;
                    int numDigits = random.nextInt(scale == 0 ? 1 : scale, maxDigits + 1);
                    StringBuilder sb = new StringBuilder(numDigits);
                    for (int i = 0; i < numDigits; i++) {
                        sb.append(random.nextInt(10));
                    }
                    BigInteger bigInt = new BigInteger(sb.toString());
                    return (T) new BigDecimal(bigInt, scale, numericType.mathContext());
                };
            case BitStringType.ID:
                return () -> {
                    int length = ((BitStringType) type).length();
                    var bitSet = new BitSet(length);
                    for (int i = 0; i < length; i++) {
                        bitSet.set(i, random.nextBoolean());
                    }
                    return (T) new BitString(bitSet, length);
                };

            case FloatVectorType.ID:
                return () -> {
                    int length = type.characterMaximumLength();
                    float[] result = new float[length];
                    for (int i = 0; i < length; i++) {
                        result[i] = random.nextFloat();
                    }
                    return (T) result;
                };

            case ArrayType.ID:
                ArrayType<?> arrayType = (ArrayType<?>) type;
                Supplier<?> elementGenerator = getDataGenerator(arrayType.innerType());
                return () -> {
                    int length = random.nextInt(21);
                    ArrayList<Object> values = new ArrayList<>(length);
                    for (int i = 0; i < length; i++) {
                        if (random.nextInt(30) == 0) {
                            values.add(null);
                        } else {
                            values.add(elementGenerator.get());
                        }
                    }
                    return (T) values;
                };
            default:
                throw new AssertionError("No data generator for type " + type.getName());
        }
    }

    /**
     * Adds innerTypes to object type definitions
     **/
    public static DataType<?> extendedType(DataType<?> type, Object value) {
        if (type.id() == ObjectType.ID) {
            var entryIt = ((Map<?, ?>) value).entrySet().iterator();
            Builder builder = ObjectType.builder();
            while (entryIt.hasNext()) {
                var entry = entryIt.next();
                String innerName = (String) entry.getKey();
                Object innerValue = entry.getValue();
                DataType<?> innerType = DataTypes.guessType(innerValue);
                if (innerType.id() == ObjectType.ID && innerValue instanceof Map<?, ?> m && m.containsKey("coordinates")) {
                    innerType = DataTypes.GEO_SHAPE;
                }
                builder.setInnerType(innerName, extendedType(innerType, innerValue));
            }
            return builder.build();
        } else {
            return type;
        }
    }

    private static String randomIPv6Address(Random random) {
        String[] parts = new String[8];
        for (int i = 0; i < 8; i++) {
            parts[i] = Integer.toHexString(random.nextInt(2 ^ 16));
        }
        return String.join(":", parts);
    }

    private static String randomIPv4Address(Random random) {
        return (random.nextInt(255) + 1) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256);
    }
}
