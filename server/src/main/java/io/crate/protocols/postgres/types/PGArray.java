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
import io.crate.protocols.postgres.parser.PgArrayParser;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class PGArray extends PGType<List<Object>> {

    private final PGType<?> innerType;

    static final PGArray CHAR_ARRAY = new PGArray(1002, CharType.INSTANCE);
    static final PGArray INT2_ARRAY = new PGArray(1005, SmallIntType.INSTANCE);
    static final PGArray INT4_ARRAY = new PGArray(1007, IntegerType.INSTANCE);
    static final PGArray INT8_ARRAY = new PGArray(1016, BigIntType.INSTANCE);
    static final PGArray FLOAT4_ARRAY = new PGArray(1021, RealType.INSTANCE);
    static final PGArray FLOAT8_ARRAY = new PGArray(1022, DoubleType.INSTANCE);
    static final PGArray BOOL_ARRAY = new PGArray(1000, BooleanType.INSTANCE);
    static final PGArray TIMESTAMPZ_ARRAY = new PGArray(1185, TimestampZType.INSTANCE);
    static final PGArray TIMESTAMP_ARRAY = new PGArray(1115, TimestampType.INSTANCE);
    static final PGArray TIMETZ_ARRAY = new PGArray(1270, TimeTZType.INSTANCE);
    static final PGArray VARCHAR_ARRAY = new PGArray(1015, VarCharType.INSTANCE);
    static final PGArray JSON_ARRAY = new PGArray(199, JsonType.INSTANCE);
    static final PGArray POINT_ARRAY = new PGArray(1017, PointType.INSTANCE);
    static final PGArray INTERVAL_ARRAY = new PGArray(1187, IntervalType.INSTANCE);
    static final PGArray EMPTY_RECORD_ARRAY = new PGArray(2287, RecordType.EMPTY_RECORD);
    public static final PGArray ANY_ARRAY = new PGArray(
        2277,
        AnyType.INSTANCE.typName() + "array",
        AnyType.INSTANCE) {

        @Override
        public String typeCategory() {
            return TypeCategory.PSEUDO.code();
        }
    };

    private static final byte[] NULL_BYTES = new byte[]{'N', 'U', 'L', 'L'};

    PGArray(int oid, String name, PGType<?> innerType) {
        super(oid, -1, -1, name);
        this.innerType = innerType;
    }

    PGArray(int oid, PGType<?> innerType) {
        this(oid, "_" + innerType.typName(), innerType);
    }

    @Override
    public int typArray() {
        return 0;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.ARRAY.code();
    }

    @Override
    public String type() {
        return innerType.type();
    }

    @Override
    public int typElem() {
        return innerType.oid();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull List<Object> value) {
        int dimensions = getDimensions(value);

        List<Integer> dimensionsList = new ArrayList<>();
        buildDimensions(value, dimensionsList, dimensions, 1);

        int bytesWritten = 4 + 4 + 4;
        final int lenIndex = buffer.writerIndex();
        buffer.writeInt(0);
        buffer.writeInt(dimensions);
        buffer.writeInt(1); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.writeInt(typElem());

        for (Integer dim : dimensionsList) {
            buffer.writeInt(dim); // upper bound
            buffer.writeInt(dim); // lower bound
            bytesWritten += 8;
        }
        int len = bytesWritten + writeArrayAsBinary(buffer, value, dimensionsList, 1);
        buffer.setInt(lenIndex, len);
        return INT32_BYTE_SIZE + len; // add also the size of the length itself
    }

    private int getDimensions(@Nonnull Object value) {
        int dimensions = 0;
        Object array = value;

        do {
            dimensions++;
            List arr = (List) array;
            if (arr.isEmpty()) {
                break;
            }
            array = null;
            for (Object o : arr) {
                if (o == null) {
                    continue;
                }
                array = o;
            }
        } while (array instanceof List);
        return dimensions;
    }

    @Override
    public List<Object> readBinaryValue(ByteBuf buffer, int valueLength) {
        int dimensions = buffer.readInt();
        buffer.readInt(); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.readInt(); // element oid
        if (dimensions == 0) {
            return List.of();
        }
        int[] dims = new int[dimensions];
        for (int d = 0; d < dimensions; ++d) {
            dims[d] = buffer.readInt();
            buffer.readInt(); // lowerBound ignored
        }
        List<Object> values = new ArrayList<>(dims[0]);
        readArrayAsBinary(buffer, values, dims, 0);
        return values;
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull List<Object> array) {
        boolean isJson = JsonType.OID == innerType.oid();
        List<Byte> encodedValues = new ArrayList<>();
        encodedValues.add((byte) '{');
        for (int i = 0; i < array.size(); i++) {
            Object o = array.get(i);
            if (o instanceof List) { // Nested Array -> recursive call
                byte[] bytes = encodeAsUTF8Text((List) o);
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
                    bytes = ((PGType) innerType).encodeAsUTF8Text(o);

                    encodedValues.add((byte) '"');
                    if (isJson) {
                        for (byte aByte : bytes) {
                            // Escape double quotes with backslash for json
                            if ((char) aByte == '"') {
                                encodedValues.add((byte) '\\');
                            }
                            encodedValues.add(aByte);
                        }
                    } else {
                        for (byte aByte : bytes) {
                            encodedValues.add(aByte);
                        }
                    }
                    encodedValues.add((byte) '"');
                }
            }
        }
        encodedValues.add((byte) '}');
        return Bytes.toArray(encodedValues);
    }

    @Override
    List<Object> decodeUTF8Text(byte[] bytes) {
        /*
         * text representation:
         *
         * 1-dimension integer array:
         *      {10,NULL,NULL,20,30}
         *      {"10",NULL,NULL,"20","30"}
         * 2-dimension integer array:
         *      {{"10","20"},{"30",NULL,"40}}
         *
         * 1-dimension json array:
         *      {"{"x": 10}","{"y": 20}"}
         * 2-dimension json array:
         *      {{"{"x": 10}","{"y": 20}"},{"{"x": 30}","{"y": 40}"}}
         */

        return (List<Object>) PgArrayParser.parse(bytes, innerType::decodeUTF8Text);
    }

    private int buildDimensions(List<Object> values, List<Integer> dimensionsList, int maxDimensions, int currentDimension) {
        if (values == null) {
            return 1;
        }
        // While elements of array are also arrays
        if (currentDimension < maxDimensions) {
            int max = 0;
            for (Object o : values) {
                max = Math.max(max, buildDimensions((List<Object>) o, dimensionsList, maxDimensions, currentDimension + 1));
            }

            if (currentDimension == maxDimensions - 1) {
                dimensionsList.add(max);
            } else {
                Integer current = dimensionsList.get(0);
                dimensionsList.set(0, Math.max(current, max));
            }
        }
        // Add the dimensions of 1st dimension
        if (currentDimension == 1) {
            dimensionsList.add(0, values.size());
        }
        return values.size();
    }

    private int writeArrayAsBinary(ByteBuf buffer, List<Object> array, List<Integer> dimensionsList, int currentDimension) {
        int bytesWritten = 0;

        if (array == null) {
            for (int i = 0; i < dimensionsList.get(currentDimension - 1); i++) {
                buffer.writeInt(-1);
                bytesWritten += 4;
            }
            return bytesWritten;
        }

        // 2nd to last level
        if (currentDimension == dimensionsList.size()) {
            int i = 0;
            for (Object o : array) {
                if (o == null) {
                    buffer.writeInt(-1);
                    bytesWritten += 4;
                } else {
                    bytesWritten += ((PGType) innerType).writeAsBinary(buffer, o);
                }
                i++;
            }
            // Fill in with -1 for up to max dimensions
            for (; i < dimensionsList.get(currentDimension - 1); i++) {
                buffer.writeInt(-1);
                bytesWritten += 4;
            }
        } else {
            for (Object o : array) {
                bytesWritten += writeArrayAsBinary(buffer, (List<Object>) o, dimensionsList, currentDimension + 1);
            }
        }
        return bytesWritten;
    }

    private void readArrayAsBinary(ByteBuf buffer,
                                   final List<Object> array,
                                   final int[] dims,
                                   final int thisDimension) {
        if (thisDimension == dims.length - 1) {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                int len = buffer.readInt();
                if (len == -1) {
                    array.add(null);
                } else {
                    array.add(innerType.readBinaryValue(buffer, len));
                }
            }
        } else {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                ArrayList<Object> list = new ArrayList<>(dims[thisDimension + 1]);
                array.add(list);
                readArrayAsBinary(buffer, list, dims, thisDimension + 1);
            }
        }
    }
}
