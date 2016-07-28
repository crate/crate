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
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
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
                    if (aByte == '"' || aByte == '\\') {
                        encodedValues.add((byte) '\\');
                    }
                    encodedValues.add(aByte);
                }
                encodedValues.add((byte) '\"');
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
         * integer array:
         *      {"10",NULL,NULL,"20","30"}
         *
         *
         * json array:
         *      {"{"x": 10}","{"y": 20}"}
         */

        int level = 0;
        int valIdx = 0;
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < bytes.length; i++) {
            byte aByte = bytes[i];
            switch (aByte) {
                case '{':
                    level++;
                    if (level == 1) {
                        /* {"10",NULL,"20"}
                         * ^
                         */
                        valIdx = i;
                    }
                    break;
                case '}':
                    level--;
                    if (level == 0) {
                        /* {"10",NULL,"20"}
                         *           |    |
                         *           |    i
                         *           valIdx
                         */
                        byte firstValueByte = bytes[valIdx + 1];
                        if (firstValueByte == '"') {
                            byte[] innerBytes = Arrays.copyOfRange(bytes, valIdx + 2 , i - 1);
                            String s = new String(innerBytes, StandardCharsets.UTF_8);
                            s = s.replace("\\\"", "\"");
                            s = s.replace("\\\\", "\\");
                            values.add(innerType.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8)));
                        } else if (firstValueByte == 'N') {
                            values.add(null);
                        }
                    }
                    break;
                case ',':
                    if (level == 1) {
                        /* {"10",NULL,"20"}
                         * ||   |
                         * ||   i
                         * |firstValueByte
                         * valIdx
                         */
                        byte firstValueByte = bytes[valIdx + 1];
                        if (firstValueByte == '"') {
                            byte[] innerBytes = Arrays.copyOfRange(bytes, valIdx + 2 , i - 1);
                            String s = new String(innerBytes, StandardCharsets.UTF_8);
                            s = s.replace("\\\"", "\"");
                            s = s.replace("\\\\", "\\");
                            values.add(innerType.decodeUTF8Text(s.getBytes(StandardCharsets.UTF_8)));
                        } else if (firstValueByte == 'N') {
                            values.add(null);
                        }
                        valIdx = i;
                    }
                    break;
            }
        }
        return values.toArray();
    }
}
