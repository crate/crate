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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.postgresql.util.ByteConverter;

import io.crate.metadata.RelationLookup;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

class NumericType extends PGType<BigDecimal> {

    static final int OID = 1700;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    private static final short NUMERIC_POS = 0x0000;
    private static final short NUMERIC_NEG = 0x4000;
    private static final BigInteger[] BI_TEN_POWERS = new BigInteger[32];
    private static final BigInteger BI_TEN_THOUSAND = BigInteger.valueOf(10000);
    private static final BigInteger BI_MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);

    static {
        for (int i = 0; i < BI_TEN_POWERS.length; i++) {
            BI_TEN_POWERS[i] = BigInteger.TEN.pow(i);
        }
    }


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
    public Regproc typSend() {
        return Regproc.of("numeric_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of("numeric_recv");
    }

    /**
     * Copied from {@link ByteConverter#numeric(BigDecimal)} but use a {@link ByteBuf}
     */
    @Override
    public int writeAsBinary(ByteBuf buffer, BigDecimal value) {
        final PositiveShorts shorts = new PositiveShorts();
        BigInteger unscaled = value.unscaledValue().abs();
        int scale = value.scale();
        if (unscaled.equals(BigInteger.ZERO)) {
            final byte[] bytes = new byte[]{0, 0, -1, -1, 0, 0, 0, 0};
            ByteConverter.int2(bytes, 6, Math.max(0, scale));
            buffer.writeInt(bytes.length);
            buffer.writeBytes(bytes);
            return INT32_BYTE_SIZE + bytes.length;
        }
        int weight = -1;
        if (scale <= 0) {
            //this means we have an integer
            //adjust unscaled and weight
            if (scale < 0) {
                scale = Math.abs(scale);
                //weight value covers 4 digits
                weight += scale / 4;
                //whatever remains needs to be incorporated to the unscaled value
                int mod = scale % 4;
                unscaled = unscaled.multiply(tenPower(mod));
                scale = 0;
            }

            while (unscaled.compareTo(BI_MAX_LONG) > 0) {
                final BigInteger[] pair = unscaled.divideAndRemainder(BI_TEN_THOUSAND);
                unscaled = pair[0];
                final short shortValue = pair[1].shortValue();
                if (shortValue != 0 || shorts.isNotEmpty()) {
                    shorts.push(shortValue);
                }
                ++weight;
            }
            long unscaledLong = unscaled.longValueExact();
            do {
                final short shortValue = (short) (unscaledLong % 10000);
                if (shortValue != 0 || shorts.isNotEmpty()) {
                    shorts.push(shortValue);
                }
                unscaledLong = unscaledLong / 10000L;
                ++weight;
            } while (unscaledLong != 0);
        } else {
            final BigInteger[] split = unscaled.divideAndRemainder(tenPower(scale));
            BigInteger decimal = split[1];
            BigInteger wholes = split[0];
            if (!BigInteger.ZERO.equals(decimal)) {
                int mod = scale % 4;
                int segments = scale / 4;
                if (mod != 0) {
                    decimal = decimal.multiply(tenPower(4 - mod));
                    ++segments;
                }
                do {
                    final BigInteger[] pair = decimal.divideAndRemainder(BI_TEN_THOUSAND);
                    decimal = pair[0];
                    final short shortValue = pair[1].shortValue();
                    if (shortValue != 0 || shorts.isNotEmpty()) {
                        shorts.push(shortValue);
                    }
                    --segments;
                } while (!BigInteger.ZERO.equals(decimal));

                //for the leading 0 shorts we either adjust weight (if no wholes)
                // or push shorts
                if (BigInteger.ZERO.equals(wholes)) {
                    weight -= segments;
                } else {
                    //now add leading 0 shorts
                    for (int i = 0; i < segments; i++) {
                        shorts.push((short) 0);
                    }
                }
            }

            while (!BigInteger.ZERO.equals(wholes)) {
                ++weight;
                final BigInteger[] pair = wholes.divideAndRemainder(BI_TEN_THOUSAND);
                wholes = pair[0];
                final short shortValue = pair[1].shortValue();
                if (shortValue != 0 || shorts.isNotEmpty()) {
                    shorts.push(shortValue);
                }
            }
        }

        //8 bytes for "header" and then 2 for each short
        final byte[] bytes = new byte[8 + (2 * shorts.size())];
        int idx = 0;

        //number of 2-byte shorts representing 4 decimal digits
        ByteConverter.int2(bytes, idx, shorts.size());
        idx += 2;
        //0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
        ByteConverter.int2(bytes, idx, weight);
        idx += 2;
        //indicates positive, negative or NaN
        ByteConverter.int2(bytes, idx, value.signum() == -1 ? NUMERIC_NEG : NUMERIC_POS);
        idx += 2;
        //number of digits after the decimal
        ByteConverter.int2(bytes, idx, scale);
        idx += 2;

        short s;
        while ((s = shorts.pop()) != -1) {
            ByteConverter.int2(bytes, idx, s);
            idx += 2;
        }

        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    /**
     * Simplified version of {@link org.postgresql.util.ByteConverter#numeric(byte[], int, int)}
     *
     * <p>The unscaled value is reconstructed arithmetically:
     * <pre>
     *   N = groups[0]*10000^(nDigits-1) + ... + groups[nDigits-1]
     *   E = 4*(weight - nDigits + 1) + scale
     *   unscaled = N * 10^E   (or N / 10^|E| when E < 0)
     * </pre>
     */
    @Override
    public BigDecimal readBinaryValue(ByteBuf buffer, int valueLength) {
        int nDigits = buffer.readShort() & 0xFFFF;
        short weight = buffer.readShort();
        short sign = buffer.readShort();
        short scale = buffer.readShort();

        if (nDigits == 0) {
            return new BigDecimal(BigInteger.ZERO, scale);
        }

        BigInteger bigInt = BigInteger.ZERO;
        for (int i = 0; i < nDigits; i++) {
            bigInt = bigInt.multiply(BI_TEN_THOUSAND).add(BigInteger.valueOf(buffer.readShort() & 0xFFFF));
        }

        int e = 4 * (weight - nDigits + 1) + scale;
        BigInteger unscaled = e >= 0 ? bigInt.multiply(tenPower(e)) : bigInt.divide(tenPower(-e));

        BigDecimal bd = new BigDecimal(unscaled, scale);
        return sign == NUMERIC_NEG ? bd.negate() : bd;
    }

    @Override
    protected byte[] encodeAsUTF8Text(BigDecimal value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    BigDecimal decodeUTF8Text(byte[] bytes, RelationLookup relationLookup) {
        return new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
    }

    private static BigInteger tenPower(int exponent) {
        return BI_TEN_POWERS.length > exponent ? BI_TEN_POWERS[exponent] : BigInteger.TEN.pow(exponent);
    }

    /**
     * Simple stack structure for non-negative {@code short} values.
     */
    private static final class PositiveShorts {
        private short[] shorts = new short[8];
        private int idx;

        PositiveShorts() {
        }

        void push(short s) {
            if (s < 0) {
                throw new IllegalArgumentException("only non-negative values accepted: " + s);
            }
            if (idx == shorts.length) {
                grow();
            }
            shorts[idx++] = s;
        }

        int size() {
            return idx;
        }

        boolean isNotEmpty() {
            return idx != 0;
        }

        short pop() {
            return idx > 0 ? shorts[--idx] : -1;
        }

        private void grow() {
            final int newSize = shorts.length <= 1024 ? shorts.length << 1 : (int) (shorts.length * 1.5);
            shorts = Arrays.copyOf(shorts, newSize);
        }
    }
}
