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
import java.math.MathContext;
import java.nio.charset.StandardCharsets;

import org.jetbrains.annotations.NotNull;

import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

class NumericType extends PGType<BigDecimal> {

    static final int OID = 1700;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    private static final short DEC_DIGITS = 4;
    private static final short NUMERIC_POS = 0x0000;
    private static final short NUMERIC_NEG = 0x4000;

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

    @Override
    public int writeAsBinary(ByteBuf buffer, @NotNull BigDecimal value) {
        // Taken from https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/pgwire/types.go#L336
        // and https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c#L6760.
        // The number is split into chunks of DEC_DIGITS short values while leading and trailing 0's are omitted.
        // Examples:
        //  * 01234       -> [1234]
        //  * 1234567     -> [0123, 4567], scale 0
        //  * 1.234500    -> [0001, 2345], scale 4
        //  * 1234567.12  -> [0123, 4567, 1200], scale 1
        //  * 1234.0      -> [1234], scale 1
        //  * 0123.45     -> [0123, 4500], scale 2
        var digits = value.unscaledValue().toString().toCharArray();
        int start = 0;
        int end = digits.length;
        while (start < end && (digits[start] == '0' || digits[start] == '-')) {
            start++;
        }
        int dWeight = end - start - value.scale() - 1;
        while (start < end && digits[end - 1] == '0') {
            end--;
        }

        int len = end - start;
        short weight = 0;       // Max DEC_DIGIT block index before decimal point
        int offset = 0;         // Offset inside the first block, e.g. 234.23 has and offset of 1
        short nDigits = 0;      // Number of DEC_DIGIT blocks
        if (len != 0) {
            if (dWeight >= 0) {
                weight = (short) ((dWeight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1);
            } else {
                weight = (short) (-((-dWeight - 1) / DEC_DIGITS + 1));
            }
            offset = (weight + 1) * DEC_DIGITS - (dWeight + 1);
            nDigits = (short)((len + offset + DEC_DIGITS - 1) / DEC_DIGITS);
        }
        int typeLen = 2 * (4 + nDigits);

        buffer.writeInt(typeLen);
        buffer.writeShort(nDigits);
        buffer.writeShort(weight);
        if (value.signum() == -1) {
            buffer.writeShort(NUMERIC_NEG);
        } else {
            buffer.writeShort(NUMERIC_POS);
        }
        buffer.writeShort(value.scale());

        int digitIdx = -offset + start;
        while (nDigits-- > 0) {
            short ndigit = 0;
            // Encode 4 digits into a 16 bit short value
            for (var nextDigitIdx = digitIdx + DEC_DIGITS; digitIdx < nextDigitIdx; digitIdx++) {
                ndigit *= 10;
                if (digitIdx >= start && digitIdx < end) {
                    ndigit += (short) (digits[digitIdx] - '0');
                }
            }
            buffer.writeShort(ndigit);
        }

        return INT32_BYTE_SIZE + typeLen;
    }

    @Override
    public BigDecimal readBinaryValue(ByteBuf buffer, int valueLength) {
        // Number of DEC_DIGIT blocks
        short nDigits = buffer.readShort();
        // DEC_DIGIT blocks before decimal point
        short weight = buffer.readShort();
        short sign = buffer.readShort();
        short scale = buffer.readShort();

        if (nDigits == 0) {
            return BigDecimal.ZERO;
        }

        boolean has_dp = scale > 0;
        int sizeOfBytes = (nDigits * DEC_DIGITS) + (has_dp ? 1 : 0);
        char[] decDigits = new char[sizeOfBytes];

        int decDigitsIdx = 0;
        for (int i = 0; i < nDigits; i++) {
            int decDigit = buffer.readShort();
            if (decDigit > 0) {
                // Decode 4 digits from a 16 bit short
                for (int j = 1000; j > 0 && decDigitsIdx < sizeOfBytes; j /= 10) {
                    int d1 = (decDigit / j);
                    decDigit -= d1 * j;
                    decDigits[decDigitsIdx++] = (char) (d1 + '0');
                }
            }
            if (has_dp && i == weight) {
                decDigits[decDigitsIdx++] = '.';
            }
        }

        var bd = new BigDecimal(decDigits)
            .setScale(scale, MathContext.UNLIMITED.getRoundingMode());
        return sign == NUMERIC_NEG ? bd.negate() : bd;
    }

    @Override
    protected byte[] encodeAsUTF8Text(@NotNull BigDecimal value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    BigDecimal decodeUTF8Text(byte[] bytes) {
        return new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
    }
}
