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

package io.crate.protocols.postgres.types;

import com.google.common.primitives.Bytes;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.Tuple;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class PGArray extends PGType {

    private final PGType innerType;

    static final PGArray CHAR_ARRAY = new PGArray(1002, CharType.INSTANCE);
    static final PGArray INT2_ARRAY = new PGArray(1005, SmallIntType.INSTANCE);
    static final PGArray INT4_ARRAY = new PGArray(1007, IntegerType.INSTANCE);
    static final PGArray INT8_ARRAY = new PGArray(1016, BigIntType.INSTANCE);
    static final PGArray FLOAT4_ARRAY = new PGArray(1021, RealType.INSTANCE);
    static final PGArray FLOAT8_ARRAY = new PGArray(1022, DoubleType.INSTANCE);
    static final PGArray BOOL_ARRAY = new PGArray(1000, BooleanType.INSTANCE);
    static final PGArray TIMESTAMPZ_ARRAY = new PGArray(1185, TimestampType.INSTANCE);
    static final PGArray VARCHAR_ARRAY = new PGArray(1015, VarCharType.INSTANCE);
    static final PGArray JSON_ARRAY = new PGArray(199, JsonType.INSTANCE);

    private static final byte[] NULL_BYTES = new byte[] { 'N', 'U', 'L', 'L' };

    private PGArray(int oid, PGType innerType) {
        super(oid, -1, -1, "_" + innerType.typName());
        this.innerType = innerType;
    }

    @Override
    public int typElem() {
        return innerType.oid();
    }

    @Override
    public int writeAsBinary(ChannelBuffer buffer, @Nonnull Object value) {
        throw new UnsupportedOperationException("Binary array streaming not supported");
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        throw new UnsupportedOperationException("Binary array streaming not supported");
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull Object array) {
        Object[] values = (Object[]) array;
        List<Byte> encodedValues = new ArrayList<>();
        encodedValues.add((byte)'{');
        for (int i = 0; i < values.length; i++) {
            Object o = values[i];
            if (o instanceof Object[]) { // Nested Array -> recursive call
                byte[] bytes = encodeAsUTF8Text(o);
                for (byte b : bytes) {
                    encodedValues.add(b);
                }
                if (i == 0) {
                    encodedValues.add((byte) ',');
                }
            } else {
                if (i > 0) {
                    encodedValues.add((byte) ',');
                }
                byte[] bytes;
                if (o == null) {
                    bytes = NULL_BYTES;
                    for (byte aByte : bytes) {
                        encodedValues.add(aByte);
                    }
                } else {
                    bytes = innerType.encodeAsUTF8Text(o);
                    encodedValues.add((byte) '\"');
                    for (byte aByte : bytes) {
                        encodedValues.add(aByte);
                    }
                    encodedValues.add((byte) '\"');
                }
            }
        }
        encodedValues.add((byte) '}');
        return Bytes.toArray(encodedValues);
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        /*
         * text representation:
         *
         * 1-dimension integer array:
         *      {"10",NULL,NULL,"20","30"}
         * 2-dimension integer array:
         *      {{"10","20"},{"30",NULL,"40}}
         *
         * 1-dimension json array:
         *      {"{"x": 10}","{"y": 20}"}
         * 2-dimension json array:
         *      {{"{"x": 10}","{"y": 20}"},{"{"x": 30}","{"y": 40}"}}
         */

        List<Object> values = new ArrayList<>();
        decodeUTF8Text(bytes, 0, bytes.length - 1, values);
        return values.toArray();
    }

    private int decodeUTF8Text(byte[] bytes, int startIdx, int endIdx, List<Object> objects) {
        int valIdx = startIdx;
        boolean jsonObject = false;
        for (int i = startIdx; i <= endIdx; i++) {
            byte aByte = bytes[i];
            switch (aByte) {
                case '{':
                    if (i == 0 || bytes[i - 1] != '"') {
                        if (i > 0) {
                            // n-dimensions array -> call recursively
                            List<Object> nestedObjects = new ArrayList<>();
                            i = decodeUTF8Text(bytes, i + 1, endIdx, nestedObjects);
                            valIdx = i;
                            objects.add(nestedObjects.toArray());
                        } else {
                            // 1-dimension array -> call recursively
                            i = decodeUTF8Text(bytes, i + 1, endIdx, objects);
                        }
                    } else {
                        // Start of JSON object
                        valIdx = i - 1;
                        jsonObject = true;
                    }
                    break;
                case ',':
                    if (!jsonObject) {
                        addObject(bytes, valIdx, i - 1, objects);
                        valIdx = i + 1;
                    }
                    break;
                case '}':
                    if (i == endIdx || bytes[i + 1] != '"') {
                        addObject(bytes, valIdx, i - 1, objects);
                        return i;
                    } else {
                        //End of JSON object
                        addObject(bytes, valIdx, i + 1, objects);
                        jsonObject = false;
                        i ++;
                        valIdx = i;
                        if (bytes[i] == '}') { // end of array
                            return i + 2;
                        }
                    }
            }
        }
        return endIdx;
    }

    // Decode individual inner object
    private void addObject(byte[] bytes, int startIdx, int endIdx, List<Object> objects) {
        if (endIdx > startIdx) {
            byte firstValueByte = bytes[startIdx];
            if (firstValueByte == '"') {
                byte[] innerBytes = Arrays.copyOfRange(bytes, startIdx + 1, endIdx);
                String s = new String(innerBytes, StandardCharsets.UTF_8);
                s = s.replace("\\\"", "\"");
                s = s.replace("\\\\", "\\");
                objects.add(innerType.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8)));
            } else if (firstValueByte == 'N') {
                objects.add(null);
            }
        }
    }
}
