/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.Streamer;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class DataTypes {

    private static final Logger logger = Loggers.getLogger(DataTypes.class);

    /**
     * If you add types here make sure to update the SizeEstimatorFactory in the SQL module.
     */
    public static final UndefinedType UNDEFINED = UndefinedType.INSTANCE;
    public static final NotSupportedType NOT_SUPPORTED = NotSupportedType.INSTANCE;

    public static final ByteType BYTE = ByteType.INSTANCE;
    public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

    public static final StringType STRING = StringType.INSTANCE;
    public static final IpType IP = IpType.INSTANCE;

    public static final DoubleType DOUBLE = DoubleType.INSTANCE;
    public static final FloatType FLOAT = FloatType.INSTANCE;

    public static final ShortType SHORT = ShortType.INSTANCE;
    public static final IntegerType INTEGER = IntegerType.INSTANCE;
    public static final LongType LONG = LongType.INSTANCE;
    public static final TimestampType TIMESTAMP = TimestampType.INSTANCE;

    public static final ObjectType OBJECT = ObjectType.INSTANCE;

    public static final GeoPointType GEO_POINT = GeoPointType.INSTANCE;
    public static final GeoShapeType GEO_SHAPE = GeoShapeType.INSTANCE;

    public static final DataType DOUBLE_ARRAY = new ArrayType(DOUBLE);
    public static final DataType OBJECT_ARRAY = new ArrayType(OBJECT);
    public static final DataType STRING_ARRAY = new ArrayType(STRING);

    public static final ImmutableList<DataType> PRIMITIVE_TYPES = ImmutableList.of(
        BYTE,
        BOOLEAN,
        STRING,
        IP,
        DOUBLE,
        FLOAT,
        SHORT,
        INTEGER,
        LONG,
        TIMESTAMP
    );

    public static final ImmutableList<DataType> NUMERIC_PRIMITIVE_TYPES = ImmutableList.of(
        DOUBLE,
        FLOAT,
        BYTE,
        SHORT,
        INTEGER,
        LONG
    );

    /**
     * Type registry mapping type ids to the according data type instance.
     */
    private static final Map<Integer, Supplier<DataType>> TYPE_REGISTRY = new MapBuilder<Integer, Supplier<DataType>>()
        .put(UndefinedType.ID, () -> UNDEFINED)
        .put(NotSupportedType.ID, () -> NOT_SUPPORTED)
        .put(ByteType.ID, () -> BYTE)
        .put(BooleanType.ID, () -> BOOLEAN)
        .put(StringType.ID, () -> STRING)
        .put(IpType.ID, () -> IP)
        .put(DoubleType.ID, () -> DOUBLE)
        .put(FloatType.ID, () -> FLOAT)
        .put(ShortType.ID, () -> SHORT)
        .put(IntegerType.ID, () -> INTEGER)
        .put(LongType.ID, () -> LONG)
        .put(TimestampType.ID, () -> TIMESTAMP)
        .put(ObjectType.ID, () -> OBJECT)
        .put(GeoPointType.ID, () -> GEO_POINT)
        .put(GeoShapeType.ID, () -> GEO_SHAPE)
        .put(ArrayType.ID, ArrayType::new)
        .put(SetType.ID, SetType::new)
        .put(SingleColumnTableType.ID, SingleColumnTableType::new)
        .map();


    private static final Set<DataType> NUMBER_CONVERSIONS = ImmutableSet.<DataType>builder()
        .addAll(NUMERIC_PRIMITIVE_TYPES)
        .add(BOOLEAN)
        .add(STRING, TIMESTAMP, IP)
        .build();
    // allowed conversion from key to one of the value types
    // the key type itself does not need to be in the value set
    static final ImmutableMap<Integer, Set<DataType>> ALLOWED_CONVERSIONS = ImmutableMap.<Integer, Set<DataType>>builder()
        .put(BYTE.id(), NUMBER_CONVERSIONS)
        .put(SHORT.id(), NUMBER_CONVERSIONS)
        .put(INTEGER.id(), NUMBER_CONVERSIONS)
        .put(LONG.id(), NUMBER_CONVERSIONS)
        .put(FLOAT.id(), NUMBER_CONVERSIONS)
        .put(DOUBLE.id(), NUMBER_CONVERSIONS)
        .put(BOOLEAN.id(), ImmutableSet.of(STRING))
        .put(STRING.id(), ImmutableSet.<DataType>builder()
            .addAll(NUMBER_CONVERSIONS)
            .add(GEO_SHAPE)
            .add(GEO_POINT)
            .add(BOOLEAN)
            .add(OBJECT)
            .build())
        .put(IP.id(), ImmutableSet.of(STRING))
        .put(TIMESTAMP.id(), ImmutableSet.of(DOUBLE, LONG, STRING))
        .put(UNDEFINED.id(), ImmutableSet.of()) // actually convertible to every type, see NullType
        .put(GEO_POINT.id(), ImmutableSet.of(new ArrayType(DOUBLE)))
        .put(OBJECT.id(), ImmutableSet.of(GEO_SHAPE))
        .put(ArrayType.ID, ImmutableSet.of()) // convertability handled in ArrayType
        .put(SetType.ID, ImmutableSet.of()) // convertability handled in SetType
        .put(SingleColumnTableType.ID, ImmutableSet.of()) // convertability handled in SingleColumnTableType
        .build();

    /**
     * Contains number conversions which are "safe" (= a conversion would not reduce the number of bytes
     * used to store the value)
     */
    private static final ImmutableMap<Integer, Set<DataType>> SAFE_CONVERSIONS = ImmutableMap.<Integer, Set<DataType>>builder()
        .put(BYTE.id(), ImmutableSet.of(SHORT, INTEGER, LONG, TIMESTAMP, FLOAT, DOUBLE))
        .put(SHORT.id(), ImmutableSet.of(INTEGER, LONG, TIMESTAMP, FLOAT, DOUBLE))
        .put(INTEGER.id(), ImmutableSet.of(LONG, TIMESTAMP, FLOAT, DOUBLE))
        .put(LONG.id(), ImmutableSet.of(TIMESTAMP, DOUBLE))
        .put(FLOAT.id(), ImmutableSet.of(DOUBLE))
        .build();

    public static boolean isCollectionType(DataType type) {
        return type.id() == ArrayType.ID || type.id() == SetType.ID || type.id() == SingleColumnTableType.ID;
    }

    public static DataType fromStream(StreamInput in) throws IOException {
        int i = in.readVInt();
        try {
            DataType type = TYPE_REGISTRY.get(i).get();
            type.readFrom(in);
            return type;
        } catch (NullPointerException e) {
            logger.error(String.format(Locale.ENGLISH, "%d is missing in TYPE_REGISTRY", i), e);
            throw e;
        }
    }

    public static void toStream(DataType type, StreamOutput out) throws IOException {
        out.writeVInt(type.id());
        type.writeTo(out);
    }

    private static final Map<Class<?>, DataType> POJO_TYPE_MAPPING = ImmutableMap.<Class<?>, DataType>builder()
        .put(Double.class, DOUBLE)
        .put(Float.class, FLOAT)
        .put(Integer.class, INTEGER)
        .put(Long.class, LONG)
        .put(Short.class, SHORT)
        .put(Byte.class, BYTE)
        .put(Boolean.class, BOOLEAN)
        .put(Map.class, OBJECT)
        .put(String.class, STRING)
        .put(BytesRef.class, STRING)
        .put(Character.class, STRING)
        .build();

    public static DataType<?> guessType(Object value) {
        if (value == null) {
            return UNDEFINED;
        } else if (value instanceof Map) {
            return OBJECT;
        } else if (value instanceof List) {
            return valueFromList((List) value);
        } else if (value.getClass().isArray()) {
            return valueFromList(Arrays.asList((Object[]) value));
        }
        return POJO_TYPE_MAPPING.get(value.getClass());
    }

    private static DataType valueFromList(List<Object> value) {
        DataType highest = DataTypes.UNDEFINED;
        for (Object o : value) {
            if (o == null) {
                continue;
            }
            DataType current = guessType(o);
            // JSON libraries tend to optimize things like [ 0.0, 1.2 ] to [ 0, 1.2 ]; so we allow mixed types
            // in such cases.
            if (!current.equals(highest) && !safeConversionPossible(current, highest)) {
                throw new IllegalArgumentException(
                    "Mixed dataTypes inside a list are not supported. Found " + highest + " and " + current);
            }
            if (current.precedes(highest)) {
                highest = current;
            }
        }
        return new ArrayType(highest);
    }

    private static boolean safeConversionPossible(DataType type1, DataType type2) {
        final DataType source;
        final DataType target;
        if (type1.precedes(type2)) {
            source = type2;
            target = type1;
        } else {
            source = type1;
            target = type2;
        }
        if (source.id() == DataTypes.UNDEFINED.id()) {
            return true;
        }
        Set<DataType> conversions = SAFE_CONVERSIONS.get(source.id());
        return conversions != null && conversions.contains(target);
    }

    private static final ImmutableMap<String, DataType> staticTypesNameMap = ImmutableMap.<String, DataType>builder()
        .put(UNDEFINED.getName(), UNDEFINED)
        .put(BYTE.getName(), BYTE)
        .put(BOOLEAN.getName(), BOOLEAN)
        .put(STRING.getName(), STRING)
        .put(IP.getName(), IP)
        .put(DOUBLE.getName(), DOUBLE)
        .put(FLOAT.getName(), FLOAT)
        .put(SHORT.getName(), SHORT)
        .put(INTEGER.getName(), INTEGER)
        .put(LONG.getName(), LONG)
        .put(TIMESTAMP.getName(), TIMESTAMP)
        .put(OBJECT.getName(), OBJECT)
        .put(GEO_POINT.getName(), GEO_POINT)
        .put(GEO_SHAPE.getName(), GEO_SHAPE)
        .build();

    public static DataType ofName(String name) {
        DataType dataType = staticTypesNameMap.get(name);
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot find data type of name " + name);
        }
        return dataType;
    }

    private static final ImmutableMap<String, DataType> MAPPING_NAMES_TO_TYPES = ImmutableMap.<String, DataType>builder()
        .put("date", DataTypes.TIMESTAMP)
        .put("string", DataTypes.STRING)
        .put("keyword", DataTypes.STRING)
        .put("text", DataTypes.STRING)
        .put("boolean", DataTypes.BOOLEAN)
        .put("byte", DataTypes.BYTE)
        .put("short", DataTypes.SHORT)
        .put("integer", DataTypes.INTEGER)
        .put("long", DataTypes.LONG)
        .put("float", DataTypes.FLOAT)
        .put("double", DataTypes.DOUBLE)
        .put("ip", DataTypes.IP)
        .put("geo_point", DataTypes.GEO_POINT)
        .put("geo_shape", DataTypes.GEO_SHAPE)
        .put("object", DataTypes.OBJECT)
        .put("nested", DataTypes.OBJECT).build();

    @Nullable
    public static DataType ofMappingName(String name) {
        return MAPPING_NAMES_TO_TYPES.get(name);
    }

    public static DataType ofMappingNameSafe(String name) {
        DataType dataType = ofMappingName(name);
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot find data type of mapping name " + name);
        }
        return dataType;
    }

    public static boolean isPrimitive(DataType type) {
        return PRIMITIVE_TYPES.contains(type);
    }

    /**
     * Register a custom data type to the type registry.
     *
     * <p>Note: If registering is done inside a static block, be sure the class is loaded initially.
     * Otherwise it might not be registered on all nodes.
     * </p>
     */
    public static void register(int id, Supplier<DataType> dataType) {
        if (TYPE_REGISTRY.put(id, dataType) != null) {
            throw new IllegalArgumentException("Already got a dataType with id " + id);
        }
    }

    public static Streamer<?>[] getStreamers(Collection<? extends DataType> dataTypes) {
        Streamer<?>[] streamer = new Streamer[dataTypes.size()];
        int idx = 0;
        for (DataType dataType : dataTypes) {
            streamer[idx] = dataType.streamer();
            idx++;
        }
        return streamer;
    }

    /**
     * Returns the first data type that is not {@link UndefinedType}, or {@code UNDEFINED} if none found.
     */
    public static DataType tryFindNotNullType(Iterable<? extends DataType> dataTypes) {
        return Iterables.find(dataTypes, input -> input != UNDEFINED, UNDEFINED);
    }

    public static DataType fromId(Integer id) {
        return TYPE_REGISTRY.get(id).get();
    }

}
