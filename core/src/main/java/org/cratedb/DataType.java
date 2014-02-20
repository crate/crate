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

package org.cratedb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum DataType {

    BYTE("byte", new Streamer<Byte>() {
        @Override
        public Byte readFrom(StreamInput in) throws IOException {
            return in.readByte();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeByte((Byte) v);
        }
    }),
    SHORT("short", new Streamer<Short>() {
        @Override
        public Short readFrom(StreamInput in) throws IOException {
            return in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeShort((Short) v);
        }
    }),
    INTEGER("integer", new Streamer<Integer>() {
        @Override
        public Integer readFrom(StreamInput in) throws IOException {
            return in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeVInt((Integer) v);
        }
    }),
    LONG("long", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeVLong((Long) v);
        }
    }),
    FLOAT("float", new Streamer<Float>() {
        @Override
        public Float readFrom(StreamInput in) throws IOException {
            return in.readFloat();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeFloat((Float) v);
        }
    }),
    DOUBLE("double", new Streamer<Double>() {
        @Override
        public Double readFrom(StreamInput in) throws IOException {
            return in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeDouble((Double) v);
        }
    }),
    BOOLEAN("boolean", new Streamer<Boolean>() {
        @Override
        public Boolean readFrom(StreamInput in) throws IOException {
            return in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean((Boolean) v);
        }
    }),
    STRING("string", Streamer.BYTES_REF),
    TIMESTAMP("timestamp", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeLong((Long) v);
        }
    }),
    OBJECT("object", new Streamer<Map<String, Object>>() {
        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> readFrom(StreamInput in) throws IOException {
            return (Map<String, Object>)in.readGenericValue();
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
    NULL_SET("null_set", new SetStreamer<Void>(NULL.streamer()));

    /**
     * Keep the order of the following to Lists (ALL_TYPES, SET_TYPES) in sync
     * with the above 'enum' ordering.
     *
     * This gives you the advantage to get e.g. SET_TYPES more easily:
     *
     *      DataType.SET_TYPES.get(DataType.LONG.ordinal());
     */
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

    public static final ImmutableList<DataType> SET_TYPES = ImmutableList.of(
            BYTE_SET,
            SHORT_SET,
            INTEGER_SET,
            LONG_SET,
            FLOAT_SET,
            DOUBLE_SET,
            BOOLEAN_SET,
            STRING_SET,
            TIMESTAMP_SET,
            OBJECT_SET,
            IP_SET
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

    public interface Streamer<T> {

        public T readFrom(StreamInput in) throws IOException;

        public void writeTo(StreamOutput out, Object v) throws IOException;

        public static final Streamer<BytesRef> BYTES_REF = new Streamer<BytesRef>() {

            @Override
            public BytesRef readFrom(StreamInput in) throws IOException {
                int length = in.readVInt();
                if (length == 0) {
                    return null;
                }
                byte[] bytes = new byte[length - 1];
                in.readBytes(bytes, 0, bytes.length);
                return new BytesRef(bytes);
            }

            @Override
            public void writeTo(StreamOutput out, Object v) throws IOException {
                // .writeBytesRef isn't used here because it will convert null values to empty bytesRefs

                // to distinguish between null and an empty bytesRef
                // 1 is always added to the length so that
                // 0 is null
                // 1 is 0
                // ...
                if (v == null) {
                    out.writeVInt(0);
                } else {
                    BytesRef bytesRef = (BytesRef)v;
                    out.writeVInt(bytesRef.length + 1);
                    out.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                }
            }
        };

    }

    public static class SetStreamer<T> implements Streamer {

        private final Streamer<T> streamer;

        public SetStreamer(Streamer streamer) {
            this.streamer = streamer;
        }


        @Override
        public Set<T> readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            Set<T> s = new HashSet<>(size);
            for(int i = 0; i < size; i++) {
                s.add(streamer.readFrom(in));
            }
            if (in.readBoolean() == true) {
                s.add(null);
            }
            return s;
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            Set<T> s = (Set<T>) v;
            out.writeVInt(s.size());
            boolean containsNull = false;
            for (T e : s) {
                if (e == null) {
                    containsNull = true;
                    continue;
                }
                streamer.writeTo(out, e);
            }
            out.writeBoolean(containsNull);
        }


        public static final Streamer<Set<BytesRef>> BYTES_REF_SET = new SetStreamer<BytesRef>(BYTES_REF);

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
            .build();

    @Nullable
    public static DataType forValue(Object value) {
        return forValue(value, true);
    }

    @Nullable
    public static DataType guess(Object value) {
        return forValue(value, false);
    }

    /**
     *
     * @param value the value to get the type for
     * @param strict if false do not check for timestamp strings
     * @return the datatype for the given value or null if no datatype matches
     */
    @Nullable
    public static DataType forValue(Object value, boolean strict) {
        if (value == null) {
            return NULL;
        } else if (value instanceof Map) { // reflection class checks don't work 100%
            return OBJECT;
        } else if (!strict && (value instanceof BytesRef || value instanceof String)) {
            // special treatment for timestamp strings
            if (TimestampFormat.isDateFormat(
                    (value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : (String)value))
                    ) {
                return TIMESTAMP;
            } else {
                return STRING;
            }
        }

        return typesMap.get(value.getClass());
    }
}
