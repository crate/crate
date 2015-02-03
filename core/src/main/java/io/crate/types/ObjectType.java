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

import io.crate.Streamer;
import io.crate.core.collections.MapComparator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import java.io.IOException;
import java.util.*;

public class ObjectType extends DataType<Map<String,Object>>
        implements Streamer<Map<String, Object>>, DataTypeFactory {

    public static final ObjectType INSTANCE = new ObjectType();
    public static final int ID = 12;
    private final static MapStreamer MAP_STREAMER = new MapStreamer();

    private ObjectType() {}

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "object";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> value(Object value) {
        return (Map<String, Object>)value;
    }

    @Override
    public int compareValueTo(Map<String, Object> val1, Map<String, Object> val2) {
        return MapComparator.compareMaps(val1, val2);
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    // TODO: require type info from each child and then use typed streamer for contents of the map
    // ?

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        return (Map<String, Object>) MAP_STREAMER.readGenericValue(in);
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        MAP_STREAMER.writeGenericValue(out, v);
    }


    private static class MapStreamer {

        @SuppressWarnings({"unchecked"})
        @Nullable
        public Object readGenericValue(StreamInput in) throws IOException {
            byte type = in.readByte();
            switch (type) {
                case -1:
                    return null;
                case 0:
                    return in.readString();
                case 1:
                    return in.readInt();
                case 2:
                    return in.readLong();
                case 3:
                    return in.readFloat();
                case 4:
                    return in.readDouble();
                case 5:
                    return in.readBoolean();
                case 6:
                    int bytesSize = in.readVInt();
                    byte[] value = new byte[bytesSize];
                    in.readBytes(value, 0, bytesSize);
                    return value;
                case 7:
                    int size = in.readVInt();
                    List list = new ArrayList(size);
                    for (int i = 0; i < size; i++) {
                        list.add(readGenericValue(in));
                    }
                    return list;
                case 8:
                    int size8 = in.readVInt();
                    Object[] list8 = new Object[size8];
                    for (int i = 0; i < size8; i++) {
                        list8[i] = readGenericValue(in);
                    }
                    return list8;
                case 9:
                    int size9 = in.readVInt();
                    Map map9 = new LinkedHashMap(size9);
                    for (int i = 0; i < size9; i++) {
                        map9.put(in.readSharedString(), readGenericValue(in));
                    }
                    return map9;
                case 10:
                    int size10 = in.readVInt();
                    Map map10 = new HashMap(size10);
                    for (int i = 0; i < size10; i++) {
                        map10.put(in.readSharedString(), readGenericValue(in));
                    }
                    return map10;
                case 11:
                    return in.readByte();
                case 12:
                    return new Date(in.readLong());
                case 13:
                    return new DateTime(in.readLong());
                case 14:
                    return in.readBytesReference();
                case 15:
                    return in.readText();
                case 16:
                    return in.readShort();
                case 17:
                    return in.readIntArray();
                case 18:
                    return in.readLongArray();
                case 19:
                    return in.readFloatArray();
                case 20:
                    return in.readDoubleArray();
                case 21:
                    return in.readBytesRef();
                default:
                    throw new IOException("Can't read unknown type [" + type + "]");
            }
        }

        public void writeGenericValue(StreamOutput out, @Nullable Object value) throws IOException {
            if (value == null) {
                out.writeByte((byte) -1);
                return;
            }
            Class type = value.getClass();
            if (type == String.class) {
                out.writeByte((byte) 0);
                out.writeString((String) value);
            } else if (type == Integer.class) {
                out.writeByte((byte) 1);
                out.writeInt((Integer) value);
            } else if (type == Long.class) {
                out.writeByte((byte) 2);
                out.writeLong((Long) value);
            } else if (type == Float.class) {
                out.writeByte((byte) 3);
                out.writeFloat((Float) value);
            } else if (type == Double.class) {
                out.writeByte((byte) 4);
                out.writeDouble((Double) value);
            } else if (type == Boolean.class) {
                out.writeByte((byte) 5);
                out.writeBoolean((Boolean) value);
            } else if (type == byte[].class) {
                out.writeByte((byte) 6);
                out.writeVInt(((byte[]) value).length);
                out.writeBytes(((byte[]) value));
            } else if (value instanceof List) {
                out.writeByte((byte) 7);
                List list = (List) value;
                out.writeVInt(list.size());
                for (Object o : list) {
                    writeGenericValue(out, o);
                }
            } else if (value instanceof Object[]) {
                out.writeByte((byte) 8);
                Object[] list = (Object[]) value;
                out.writeVInt(list.length);
                for (Object o : list) {
                    writeGenericValue(out, o);
                }
            } else if (value instanceof Map) {
                if (value instanceof LinkedHashMap) {
                    out.writeByte((byte) 9);
                } else {
                    out.writeByte((byte) 10);
                }
                Map<String, Object> map = (Map<String, Object>) value;
                out.writeVInt(map.size());
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    out.writeSharedString(entry.getKey());
                    writeGenericValue(out, entry.getValue());
                }
            } else if (type == Byte.class) {
                out.writeByte((byte) 11);
                out.writeByte((Byte) value);
            } else if (type == Date.class) {
                out.writeByte((byte) 12);
                out.writeLong(((Date) value).getTime());
            } else if (value instanceof ReadableInstant) {
                out.writeByte((byte) 13);
                out.writeLong(((ReadableInstant) value).getMillis());
            } else if (value instanceof BytesReference) {
                out.writeByte((byte) 14);
                out.writeBytesReference((BytesReference) value);
            } else if (value instanceof Text) {
                out.writeByte((byte) 15);
                out.writeText((Text) value);
            } else if (type == Short.class) {
                out.writeByte((byte) 16);
                out.writeShort((Short) value);
            } else if (type == int[].class) {
                out.writeByte((byte) 17);
                out.writeIntArray((int[]) value);
            } else if (type == long[].class) {
                out.writeByte((byte) 18);
                out.writeLongArray((long[]) value);
            } else if (type == float[].class) {
                out.writeByte((byte) 19);
                out.writeFloatArray((float[]) value);
            } else if (type == double[].class) {
                out.writeByte((byte) 20);
                out.writeDoubleArray((double[]) value);
            } else if (type == BytesRef.class) {
                out.writeByte((byte) 21);
                out.writeBytesRef((BytesRef) value);
            } else {
                throw new IOException("Can't write type [" + type + "]");
            }
        }
    }
}
