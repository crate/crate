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

import com.google.common.base.Predicate;
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
import java.util.*;

public final class DataTypes {

    private final static Logger logger = Loggers.getLogger(DataTypes.class);

    /**
     * If you add types here make sure to update the SizeEstimatorFactory in the SQL module.
     */
    public final static AnyType ANY = AnyType.INSTANCE;
    public final static UndefinedType UNDEFINED = UndefinedType.INSTANCE;
    public final static NotSupportedType NOT_SUPPORTED = NotSupportedType.INSTANCE;

    public final static ByteType BYTE = ByteType.INSTANCE;
    public final static BooleanType BOOLEAN = BooleanType.INSTANCE;

    public final static StringType STRING = StringType.INSTANCE;
    public final static IpType IP = IpType.INSTANCE;

    public final static DoubleType DOUBLE = DoubleType.INSTANCE;
    public final static FloatType FLOAT = FloatType.INSTANCE;

    public final static ShortType SHORT = ShortType.INSTANCE;
    public final static IntegerType INTEGER = IntegerType.INSTANCE;
    public final static LongType LONG = LongType.INSTANCE;
    public final static TimestampType TIMESTAMP = TimestampType.INSTANCE;

    public final static ObjectType OBJECT = ObjectType.INSTANCE;

    public final static GeoPointType GEO_POINT = GeoPointType.INSTANCE;
    public final static GeoShapeType GEO_SHAPE = GeoShapeType.INSTANCE;

    public final static DataType ANY_ARRAY = new ArrayType(ANY);
    public final static DataType DOUBLE_ARRAY = new ArrayType(DOUBLE);
    public final static DataType OBJECT_ARRAY = new ArrayType(OBJECT);

    public final static DataType ANY_SET = new SetType(ANY);

    public final static ImmutableList<DataType> PRIMITIVE_TYPES = ImmutableList.<DataType>of(
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
    public final static ImmutableList<DataType> NUMERIC_PRIMITIVE_TYPES = ImmutableList.<DataType>of(
        DOUBLE,
        FLOAT,
        BYTE,
        SHORT,
        INTEGER,
        LONG
    );

    public static final ImmutableList<DataType> ALL_TYPES = ImmutableList.<DataType>builder()
        .addAll(PRIMITIVE_TYPES)
        .add(ANY_ARRAY)
        .add(OBJECT)
        .add(GEO_POINT)
        .add(GEO_SHAPE)
        .build();

    private static final ImmutableList<DataType> ANY_TYPES = ImmutableList.of(
        ANY,
        ANY_ARRAY,
        ANY_SET
    );

    /**
     * Type registry list contains no member of {@link #ANY_TYPES} with intent, {@link #ANY_TYPES} are only used for
     * {@code Signature} matching and do not support streaming.
     */
    private static final Map<Integer, DataTypeFactory> TYPE_REGISTRY = new MapBuilder<Integer, DataTypeFactory>()
        .put(UndefinedType.ID, UNDEFINED)
        .put(NotSupportedType.ID, NOT_SUPPORTED)
        .put(ByteType.ID, BYTE)
        .put(BooleanType.ID, BOOLEAN)
        .put(StringType.ID, STRING)
        .put(IpType.ID, IP)
        .put(DoubleType.ID, DOUBLE)
        .put(FloatType.ID, FLOAT)
        .put(ShortType.ID, SHORT)
        .put(IntegerType.ID, INTEGER)
        .put(LongType.ID, LONG)
        .put(TimestampType.ID, TIMESTAMP)
        .put(ObjectType.ID, OBJECT)
        .put(GeoPointType.ID, GEO_POINT)
        .put(GeoShapeType.ID, GEO_SHAPE)
        .put(ArrayType.ID, new DataTypeFactory() {
            @Override
            public DataType<?> create() {
                return new ArrayType();
            }
        })
        .put(SetType.ID, new DataTypeFactory() {
            @Override
            public DataType<?> create() {
                return new SetType();
            }
        }).map();

    private static final Set<DataType> NUMBER_CONVERSIONS = ImmutableSet.<DataType>builder()
        .addAll(NUMERIC_PRIMITIVE_TYPES)
        .add(BOOLEAN)
        .add(STRING, TIMESTAMP, IP)
        .build();
    // allowed conversion from key to one of the value types
    // the key type itself does not need to be in the value set
    public static final ImmutableMap<Integer, Set<DataType>> ALLOWED_CONVERSIONS = ImmutableMap.<Integer, Set<DataType>>builder()
        .put(BYTE.id(), NUMBER_CONVERSIONS)
        .put(SHORT.id(), NUMBER_CONVERSIONS)
        .put(INTEGER.id(), NUMBER_CONVERSIONS)
        .put(LONG.id(), NUMBER_CONVERSIONS)
        .put(FLOAT.id(), NUMBER_CONVERSIONS)
        .put(DOUBLE.id(), NUMBER_CONVERSIONS)
        .put(BOOLEAN.id(), ImmutableSet.<DataType>of(STRING))
        .put(STRING.id(), ImmutableSet.<DataType>builder()
            .addAll(NUMBER_CONVERSIONS)
            .add(GEO_SHAPE)
            .add(GEO_POINT)
            .add(BOOLEAN)
            .build())
        .put(IP.id(), ImmutableSet.<DataType>of(STRING))
        .put(TIMESTAMP.id(), ImmutableSet.<DataType>of(LONG))
        .put(UNDEFINED.id(), ImmutableSet.<DataType>of()) // actually convertible to every type, see NullType
        .put(GEO_POINT.id(), ImmutableSet.<DataType>of(new ArrayType(DOUBLE)))
        .put(OBJECT.id(), ImmutableSet.<DataType>of(GEO_SHAPE))
        .put(ArrayType.ID, ImmutableSet.<DataType>of()) // convertability handled in ArrayType
        .put(SetType.ID, ImmutableSet.<DataType>of()) // convertability handled in SetType
        .build();

    public static boolean isCollectionType(DataType type) {
        return type.id() == ArrayType.ID || type.id() == SetType.ID;
    }

    public static DataType fromStream(StreamInput in) throws IOException {
        int i = in.readVInt();
        try {
            DataType type = TYPE_REGISTRY.get(i).create();
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
        List<DataType> innerTypes = new ArrayList<>(value.size());
        if (value.isEmpty()) {
            return new ArrayType(UNDEFINED);
        }
        DataType previous = null;
        DataType current = null;
        for (Object o : value) {
            if (o == null) {
                continue;
            }
            current = guessType(o);
            if (previous != null && !current.equals(previous)) {
                throw new IllegalArgumentException("Mixed dataTypes inside a list are not supported");
            }
            innerTypes.add(current);
            previous = current;
        }

        if (innerTypes.isEmpty() || (innerTypes.size() > 0 && current == null)) {
            return new ArrayType(UNDEFINED);
        } else {
            return new ArrayType(current);
        }
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
    public static void register(int id, DataTypeFactory dataTypeFactory) {
        if (TYPE_REGISTRY.put(id, dataTypeFactory) != null) {
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

    private static final Predicate<DataType> NOT_NULL_TYPE_FILTER = new Predicate<DataType>() {
        @Override
        public boolean apply(DataType input) {
            return input != UNDEFINED;
        }
    };

    /**
     * Returns the first data type that is not {@link UndefinedType}, or {@code UNDEFINED} if none found.
     */
    public static DataType tryFindNotNullType(Iterable<? extends DataType> dataTypes) {
        return Iterables.find(dataTypes, NOT_NULL_TYPE_FILTER, UNDEFINED);
    }

    /**
     * Returns true if the ID of the given data type matches one of the {@link #ANY_TYPES} id's.
     * A {@link List#contains(Object)} call would always return true because the {@link AnyType#equals(Object)} method
     * matches on any {@link DataType} for correct {@code Signature} matching.
     */
    public static boolean isAnyOrAnyCollection(DataType givenType) {
        for (DataType dt : ANY_TYPES) {
            if (dt.id() == givenType.id()) {
                return true;
            }
        }
        return false;
    }
}
