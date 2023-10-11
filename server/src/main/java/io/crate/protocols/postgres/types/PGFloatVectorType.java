/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.protocols.postgres.types;

import java.util.Arrays;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.carrotsearch.hppc.ByteArrayList;

import io.crate.protocols.postgres.parser.PgArrayParser;
import io.crate.types.FloatVectorType;
import io.netty.buffer.ByteBuf;

/**
 * Like {@link PGArray#FLOAT4_ARRAY} - including using the same oid - but specialized
 * to handle float[] used by {@link FloatVectorType}.
 * It's not registered in {@link io.crate.metadata.pgcatalog.PgTypeTable}.
 */
public class PGFloatVectorType extends PGType<float[]> {


    public static final PGFloatVectorType INSTANCE = new PGFloatVectorType();

    PGFloatVectorType() {
        super(PGArray.FLOAT4_ARRAY.oid(), -1, -1, PGArray.FLOAT4_ARRAY.typName());
    }

    @Override
    public int typArray() {
        return 0;
    }

    @Override
    public int typElem() {
        return PGArray.FLOAT4_ARRAY.typElem();
    }

    @Override
    public String typeCategory() {
        return PGArray.FLOAT4_ARRAY.typeCategory();
    }

    @Override
    public String type() {
        return PGArray.FLOAT4_ARRAY.type();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @NotNull float[] value) {
        int arrayLength = value.length;

        final int lenIndex = buffer.writerIndex();
        buffer.writeInt(0);
        buffer.writeInt(1); // one dimension
        buffer.writeInt(0); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.writeInt(typElem());

        buffer.writeInt(arrayLength); // upper bound
        buffer.writeInt(arrayLength); // lower bound
        int bytesWritten = 4 + 4 + 4 + 8;

        int len = bytesWritten + writeArrayAsBinary(buffer, value);
        buffer.setInt(lenIndex, len);
        return INT32_BYTE_SIZE + len; // add also the size of the length itself
    }

    @Override
    public float[] readBinaryValue(ByteBuf buffer, int valueLength) {
        int dimensions = buffer.readInt();
        assert dimensions == 1 : "float_vector should have only 1 dimension";
        buffer.readInt(); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.readInt(); // element oid
        int dimension = buffer.readInt();
        buffer.readInt();  // lowerBound ignored
        return readArrayAsBinary(buffer, dimension);
    }

    @Override
    byte[] encodeAsUTF8Text(float[] value) {
        ByteArrayList encodedValues = new ByteArrayList();
        encodedValues.add((byte) '{');
        for (int i = 0; i < value.length; i++) {
            var f = value[i];
            if (i > 0) {
                encodedValues.add((byte) ',');
            }
            byte[] bytes = RealType.INSTANCE.encodeAsUTF8Text(f);
            encodedValues.add((byte) '"');
            encodedValues.add(bytes);
            encodedValues.add((byte) '"');
        }
        encodedValues.add((byte) '}');
        return Arrays.copyOfRange(encodedValues.buffer, 0, encodedValues.elementsCount);
    }

    @SuppressWarnings("unchecked")
    @Override
    float[] decodeUTF8Text(byte[] bytes) {
        var list = (List<Object>) PgArrayParser.parse(bytes, RealType.INSTANCE::decodeUTF8Text);
        float[] vector = new float[list.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) list.get(i);
        }
        return vector;
    }

    private int writeArrayAsBinary(ByteBuf buffer, @NotNull float[] array) {
        int bytesWritten = 0;
        for (float f : array) {
            bytesWritten += RealType.INSTANCE.writeAsBinary(buffer, f);
        }
        return bytesWritten;
    }

    static float[] readArrayAsBinary(ByteBuf buffer, final int dimension) {
        float[] array = new float[dimension];
        for (int i = 0; i < dimension; ++i) {
            int len = buffer.readInt();
            array[i] = RealType.INSTANCE.readBinaryValue(buffer, len);
        }
        return array;
    }
}
