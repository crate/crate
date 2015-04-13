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
import io.crate.Streamer;
import io.crate.TimestampFormat;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.*;

public class DataTypes {

    private final static ESLogger logger = Loggers.getLogger(DataTypes.class);

    /**
     * If you add types here make sure to update the SizeEstimatorFactory in the SQL module.
     */
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

    public static final Map<Integer, DataTypeFactory> TYPE_REGISTRY = new MapBuilder<Integer, DataTypeFactory>()
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
        .put(ArrayType.ID, new CollectionTypeFactory() {
            @Override
            public DataType<?> create() {
                return new ArrayType();
            }

            @Override
            public DataType<?> create(DataType innerType) {
                return new ArrayType(innerType);
            }
        })
        .put(SetType.ID, new CollectionTypeFactory() {
            @Override
            public DataType<?> create() {
                return new SetType();
            }

            @Override
            public DataType<?> create(DataType innerType) {
                return new SetType(innerType);
            }
        }).map();

    private static final Set<DataType> NUMBER_CONVERSIONS = ImmutableSet.<DataType>builder()
            .addAll(NUMERIC_PRIMITIVE_TYPES)
            .add(STRING, TIMESTAMP, IP, UNDEFINED)
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
            .put(BOOLEAN.id(), ImmutableSet.<DataType>of(STRING, UNDEFINED))
            .put(STRING.id(), NUMBER_CONVERSIONS)
            .put(IP.id(), ImmutableSet.<DataType>of(STRING, UNDEFINED))
            .put(TIMESTAMP.id(), ImmutableSet.<DataType>of(LONG, UNDEFINED))
            .put(UNDEFINED.id(), ImmutableSet.<DataType>of()) // actually convertible to every type, see NullType
            .put(NOT_SUPPORTED.id(), ImmutableSet.<DataType>of(UNDEFINED))
            .put(GEO_POINT.id(), ImmutableSet.<DataType>of(new ArrayType(DOUBLE), UNDEFINED))
            .put(OBJECT.id(), ImmutableSet.<DataType>of(UNDEFINED))
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

    public static DataType<?> guessType(Object value) {
        return guessType(value, true);
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

    public static DataType<?> guessType(Object value, boolean strict) {
        if (value == null) {
            return UNDEFINED;
        } else if (value instanceof Map) {
            return OBJECT;
        } else if (value instanceof List) {
            return valueFromList((List)value, strict);
        } else if (value.getClass().isArray()) {
            return valueFromList(Arrays.asList((Object[]) value), strict);
        } else if (!strict && (value instanceof BytesRef || value instanceof String)) {
            // special treatment for timestamp strings
            if (TimestampFormat.isDateFormat((
                    value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : (String) value))) {
                return TIMESTAMP;
            } else {
                return STRING;
            }
        }
        return POJO_TYPE_MAPPING.get(value.getClass());
    }

    private static DataType valueFromList(List<Object> value, boolean strict) {
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
            current = guessType(o, strict);
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
            .build();

    public static DataType ofName(String name) {
        DataType dataType = staticTypesNameMap.get(name);
        if (dataType == null) {
            throw new IllegalArgumentException("Cannot find data type of name " + name);
        }
        return dataType;
    }

    public static DataType ofJsonObject(Object type) {
        if (type instanceof List) {
            int idCollectionType = (Integer) ((List) type).get(0);
            int idInnerType = (Integer) ((List) type).get(1);
            return ((CollectionTypeFactory) TYPE_REGISTRY.get(idCollectionType)).create(ofJsonObject(idInnerType));
        }
        assert type instanceof Integer;
        return TYPE_REGISTRY.get(type).create();
    }

    public static void register(int id, DataTypeFactory dataTypeFactory) {
        if (TYPE_REGISTRY.put(id, dataTypeFactory) != null) {
            throw new IllegalArgumentException("Already got a dataType with id " + id);
        };
    }

    public static Streamer<?>[] getStreamer(Collection<? extends DataType> dataTypes) {
        Streamer<?>[] streamer = new Streamer[dataTypes.size()];
        int idx = 0;
        for (DataType dataType : dataTypes) {
            streamer[idx] = dataType.streamer();
            idx++;
        }
        return streamer;
    }
}
