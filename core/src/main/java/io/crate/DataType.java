/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import com.google.common.collect.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

// NOTE: all streamers must support null values, there for we do check for null with an additional byte
// maybe we find a better way to handle NULL values?
public enum DataType {

    BYTE("byte", new Streamer<Byte>() {
        @Override
        public Byte readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readByte();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeByte(((Number) v).byteValue());
            }
        }
    }),
    SHORT("short", new Streamer<Short>() {
        @Override
        public Short readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeShort(((Number) v).shortValue());
            }
        }
    }),
    INTEGER("integer", new Streamer<Integer>() {
        @Override
        public Integer readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readInt(); // readVInt does not support negative values
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeInt(((Number) v).intValue());
            }
        }
    }),
    LONG("long", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeLong(((Number) v).longValue());
            }
        }
    }),
    FLOAT("float", new Streamer<Float>() {
        @Override
        public Float readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readFloat();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeFloat(((Number) v).floatValue());
            }
        }
    }),
    DOUBLE("double", new Streamer<Double>() {
        @Override
        public Double readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeDouble(((Number) v).doubleValue());
            }
        }
    }),
    BOOLEAN("boolean", new Streamer<Boolean>() {
        @Override
        public Boolean readFrom(StreamInput in) throws IOException {
            return in.readOptionalBoolean();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeOptionalBoolean((Boolean) v);
        }
    }),
    STRING("string", Streamer.BYTES_REF),
    TIMESTAMP("timestamp", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readBoolean() ? null : in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean(v == null);
            if (v != null) {
                out.writeLong((Long) v);
            }
        }
    }),
    OBJECT("object", new Streamer<Map<String, Object>>() {
        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> readFrom(StreamInput in) throws IOException {
            return (Map<String, Object>) in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeGenericValue(v);
        }
    }),
    IP("ip", Streamer.BYTES_REF),

    // TODO: remove DataType
    NOT_SUPPORTED("NOT SUPPORTED", new Streamer<Object>() {
        @Override
        public Object readFrom(StreamInput in) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {

        }
    }),
    NULL("null", new Streamer<Void>() {
        @Override
        public Void readFrom(StreamInput in) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
        }
    }),
    BYTE_SET("byte_set", new SetStreamer<Byte>(BYTE.streamer())),
    SHORT_SET("short_set", new SetStreamer<Short>(SHORT.streamer())),
    INTEGER_SET("integer_set", new SetStreamer<Integer>(INTEGER.streamer())),
    LONG_SET("long_set", new SetStreamer<Long>(LONG.streamer())),
    FLOAT_SET("float_set", new SetStreamer<Float>(FLOAT.streamer())),
    DOUBLE_SET("double_set", new SetStreamer<Double>(DOUBLE.streamer())),
    BOOLEAN_SET("boolean_set", new SetStreamer<Boolean>(BOOLEAN.streamer())),
    STRING_SET("string_set", SetStreamer.BYTES_REF_SET),
    IP_SET("ip_set", SetStreamer.BYTES_REF_SET),
    TIMESTAMP_SET("timestamp_set", new SetStreamer<Long>(TIMESTAMP.streamer())),
    OBJECT_SET("object_set", new SetStreamer<Map<String, Object>>(OBJECT.streamer())),
    NULL_SET("null_set", new SetStreamer<Void>(NULL.streamer())),

    BYTE_ARRAY("byte_array", new ArrayStreamer<Byte>(BYTE.streamer())),
    SHORT_ARRAY("short_array", new ArrayStreamer<Short>(SHORT.streamer())),
    INTEGER_ARRAY("integer_array", new ArrayStreamer<Integer>(INTEGER.streamer())),
    LONG_ARRAY("long_array", new ArrayStreamer<Long>(LONG.streamer())),
    FLOAT_ARRAY("float_array", new ArrayStreamer<Float>(FLOAT.streamer())),
    DOUBLE_ARRAY("double_array", new ArrayStreamer<Double>(DOUBLE.streamer())),
    BOOLEAN_ARRAY("boolean_array", new ArrayStreamer<Boolean>(BOOLEAN.streamer())),
    STRING_ARRAY("string_array", new ArrayStreamer<BytesRef>(STRING.streamer())),
    IP_ARRAY("ip_array", new ArrayStreamer<BytesRef>(STRING.streamer())),
    TIMESTAMP_ARRAY("timestamp_array", new ArrayStreamer<Long>(TIMESTAMP.streamer())),
    OBJECT_ARRAY("object_array", new ArrayStreamer<Map<String, Object>>(OBJECT.streamer())),
    NULL_ARRAY("null_array", new ArrayStreamer<Void>(NULL.streamer()));

    public static final ImmutableList<DataType> ALL_TYPES = ImmutableList.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            OBJECT,
            IP
    );

    public static final BiMap<DataType, DataType> SET_TYPES_MAP = HashBiMap.create();
    static {
            SET_TYPES_MAP.put(BYTE, BYTE_SET);
            SET_TYPES_MAP.put(SHORT, SHORT_SET);
            SET_TYPES_MAP.put(INTEGER, INTEGER_SET);
            SET_TYPES_MAP.put(LONG, LONG_SET);
            SET_TYPES_MAP.put(FLOAT, FLOAT_SET);
            SET_TYPES_MAP.put(DOUBLE, DOUBLE_SET);
            SET_TYPES_MAP.put(BOOLEAN, BOOLEAN_SET);
            SET_TYPES_MAP.put(STRING, STRING_SET);
            SET_TYPES_MAP.put(TIMESTAMP, TIMESTAMP_SET);
            SET_TYPES_MAP.put(OBJECT, OBJECT_SET);
            SET_TYPES_MAP.put(IP, IP_SET);
            SET_TYPES_MAP.put(NULL, NULL_SET);
    }
    public static final ImmutableList<DataType> SET_TYPES = ImmutableList.copyOf(SET_TYPES_MAP.values());
    public static final Map<DataType, DataType> REVERSE_SET_TYPE_MAP = SET_TYPES_MAP.inverse();

    public static final BiMap<DataType, DataType> ARRAY_TYPES_MAP = HashBiMap.create();

    static {
        ARRAY_TYPES_MAP.put(BYTE, BYTE_ARRAY);
        ARRAY_TYPES_MAP.put(SHORT, SHORT_ARRAY);
        ARRAY_TYPES_MAP.put(INTEGER, INTEGER_ARRAY);
        ARRAY_TYPES_MAP.put(LONG, LONG_ARRAY);
        ARRAY_TYPES_MAP.put(FLOAT, FLOAT_ARRAY);
        ARRAY_TYPES_MAP.put(DOUBLE, DOUBLE_ARRAY);
        ARRAY_TYPES_MAP.put(BOOLEAN, BOOLEAN_ARRAY);
        ARRAY_TYPES_MAP.put(STRING, STRING_ARRAY);
        ARRAY_TYPES_MAP.put(TIMESTAMP, TIMESTAMP_ARRAY);
        ARRAY_TYPES_MAP.put(OBJECT, OBJECT_ARRAY);
        ARRAY_TYPES_MAP.put(IP, IP_ARRAY);
        ARRAY_TYPES_MAP.put(NULL, NULL_ARRAY);
    }

    public static final Map<DataType, DataType> REVERSE_ARRAY_TYPE_MAP = ARRAY_TYPES_MAP.inverse();


    public static final ImmutableList<DataType> ARRAY_TYPES = ImmutableList.copyOf(ARRAY_TYPES_MAP.values());

    public static final ImmutableList<DataType> ALL_TYPES_INC_NULL = ImmutableList.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            OBJECT,
            IP,
            NULL
    );

    private final Streamer streamer;

    private String name;

    private DataType(String name, Streamer streamer) {
        this.name = name;
        this.streamer = streamer;
    }

    public String getName() {
        return this.name;
    }

    public Streamer streamer() {
        return streamer;
    }

    @Override
    public String toString() {
        return name;
    }

    public static DataType fromStream(StreamInput in) throws IOException {
        return DataType.values()[in.readVInt()];
    }

    public static void toStream(DataType type, StreamOutput out) throws IOException {
        out.writeVInt(type.ordinal());
    }


    public static final ImmutableSet<DataType> NUMERIC_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE
    );

    public static final ImmutableSet<DataType> PRIMITIVE_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            IP
    );

    public static final ImmutableSet<DataType> SET_NUMERIC_TYPES = ImmutableSet.of(
            BYTE_SET,
            SHORT_SET,
            INTEGER_SET,
            LONG_SET,
            FLOAT_SET,
            DOUBLE_SET
    );

    public static final ImmutableSet<DataType> INTEGER_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG
    );

    public static final ImmutableSet<DataType> DECIMAL_TYPES = ImmutableSet.of(
            FLOAT,
            DOUBLE
    );

    private static final Map<Class<?>, DataType> typesMap = ImmutableMap.<Class<?>, DataType>builder()
            .put(Double.class, DataType.DOUBLE)
            .put(Float.class, DataType.FLOAT)
            .put(Integer.class, DataType.INTEGER)
            .put(Long.class, DataType.LONG)
            .put(Short.class, DataType.SHORT)
            .put(Byte.class, DataType.BYTE)
            .put(Boolean.class, DataType.BOOLEAN)
            .put(Map.class, DataType.OBJECT)
            .put(String.class, DataType.STRING)
            .put(BytesRef.class, DataType.STRING)
            .put(Character.class, DataType.STRING)
            .build();

    @Nullable
    public static DataType forValue(Object value) {
        return forValue(value, true);
    }

    public DataType setType() {
        if (this == DataType.NULL) {
            return this;
        } else {
            return SET_TYPES_MAP.get(this);
        }
    }

    public DataType arrayType() throws IllegalArgumentException {
        return ARRAY_TYPES_MAP.get(this);
    }

    public DataType elementType() {
        if (isArrayType()) {
            return REVERSE_ARRAY_TYPE_MAP.get(this);
        } else if (isSetType()) {
            return REVERSE_SET_TYPE_MAP.get(this);
        } else {
            return this;
        }
    }

    public boolean isPrimitiveType() {
        return PRIMITIVE_TYPES.contains(this);
    }

    public boolean isCompoundType() {
        return !isPrimitiveType();
    }

    public boolean isArrayType() {
        return ARRAY_TYPES.contains(this);
    }

    public boolean isSetType() {
        return SET_TYPES.contains(this);
    }

    public boolean isCollectionType() {
        return isArrayType() || isSetType();
    }

    @Nullable
    public static DataType guess(Object value) {
        return forValue(value, false);
    }

    /**
     * @param value  the value to get the type for
     * @param strict if false do not check for timestamp strings
     * @return the datatype for the given value or null if no datatype matches
     */
    @Nullable
    public static DataType forValue(Object value, boolean strict) {

        if (value == null) {
            return NULL;
        } else if (value instanceof Map) {
            return OBJECT;
        } else if (value instanceof List) {
            return valueFromList((List)value, strict);
        } else if (value.getClass().isArray()) {
            return valueFromList(Arrays.asList((Object[])value), strict);
        } else if (!strict && (value instanceof BytesRef || value instanceof String)) {
            // special treatment for timestamp strings
            if (TimestampFormat.isDateFormat(
                    (value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : (String) value))
                    ) {
                return TIMESTAMP;
            } else {
                return STRING;
            }
        }

        return typesMap.get(value.getClass());
    }

    private static DataType valueFromList(List<Object> value, boolean strict) {
        List<DataType> innerTypes = new ArrayList<>(value.size());

        if (value.isEmpty()) {
            return NULL_ARRAY;
        }

        DataType previous = null;
        DataType current = null;
        for (Object o : value) {
            if (o == null) {
                continue;
            }
            current = forValue(o, strict);
            if (previous != null && !current.equals(previous)) {
                throw new IllegalArgumentException("Mixed dataTypes inside a list are not supported");
            }
            innerTypes.add(current);
            previous = current;
        }

        if (innerTypes.size() > 0 && current == null) {
            return NULL_ARRAY;
        } else {
            return current.arrayType();
        }
    }
}
