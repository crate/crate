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

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;

class NumericType extends PGType<BigDecimal> {

    static final int OID = 1700;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final NumericType INSTANCE = new NumericType();

    private NumericType() {
        super(OID, TYPE_LEN, TYPE_MOD, "numeric");
    }

    @Override
    public int typArray() {
        return PGArray.NUMERIC_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.NUMERIC.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull BigDecimal value) {
        buffer.writeInt(TYPE_LEN);

        buffer.writeInt(value.scale());
        buffer.writeInt(value.precision());

        var unscaled = value.unscaledValue();
        byte[] bytes = unscaled.toByteArray();
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);

        // unscaled (length) + unscaled (bytes) + precision and scale
        return INT32_BYTE_SIZE + bytes.length + INT32_BYTE_SIZE * 2;
    }

    @Override
    protected byte[] encodeAsUTF8Text(@Nonnull BigDecimal value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public BigDecimal readBinaryValue(ByteBuf buffer, int valueLength) {
        int scale = buffer.readInt();
        int precision = buffer.readInt();

        int length = buffer.readInt();
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);

        return new BigDecimal(
            new BigInteger(bytes),
            scale,
            new MathContext(precision)
        );
    }

    @Override
    BigDecimal decodeUTF8Text(byte[] bytes) {
        return new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
    }
}
