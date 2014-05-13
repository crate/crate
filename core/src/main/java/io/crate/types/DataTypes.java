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
import io.crate.TimestampFormat;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

public class DataTypes {

    public final static NullType NULL = NullType.INSTANCE;
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
            SHORT,
            INTEGER,
            LONG
    );

    public static final ImmutableMap<Integer, DataTypeFactory> typeRegistry = ImmutableMap.<Integer, DataTypeFactory>builder()
        .put(NullType.ID, NULL)
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
        }).build();

    public static boolean isCollectionType(DataType type) {
        return type.id() == ArrayType.ID || type.id() == SetType.ID;
    }

    public static DataType fromStream(StreamInput in) throws IOException {
        int i = in.readVInt();
        DataType type = typeRegistry.get(i).create();
        type.readFrom(in);
        return type;
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
            return NULL;
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
            return new ArrayType(NULL);
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

        if (innerTypes.size() > 0 && current == null) {
            return new ArrayType(NULL);
        } else {
            return new ArrayType(current);
        }
    }
}