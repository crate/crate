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
import java.math.MathContext;
import java.nio.charset.StandardCharsets;

import io.crate.metadata.RelationLookup;
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
    public int writeAsBinary(ByteBuf buffer, BigDecimal value) {
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
        int weight = 0;         // Max DEC_DIGIT block index before decimal point
        int offset = 0;         // Offset inside the first block, e.g. 234.23 has and offset of 1
        short nDigits = 0;      // Number of DEC_DIGIT blocks
        if (len != 0) {
            if (dWeight >= 0) {
                weight = (dWeight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
            } else {
                weight = -((-dWeight - 1) / DEC_DIGITS + 1);
            }
            offset = (weight + 1) * DEC_DIGITS - (dWeight + 1);
            nDigits = (short)((len + offset + DEC_DIGITS - 1) / DEC_DIGITS);
        }
        // Header: nDigits(2) + weight(4) + sign(2) + scale(4) = 12 bytes, plus 2 per digit group.
        // weight and scale use int32 to support values outside the int16 range.
        int typeLen = 12 + 2 * nDigits;

        buffer.writeInt(typeLen);
        buffer.writeShort(nDigits);
        buffer.writeInt(weight);
        if (value.signum() == -1) {
            buffer.writeShort(NUMERIC_NEG);
        } else {
            buffer.writeShort(NUMERIC_POS);
        }
        buffer.writeInt(value.scale());

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
        // DEC_DIGIT blocks before decimal point (int32 to support values outside int16 range)
        int weight = buffer.readInt();
        short sign = buffer.readShort();
        int scale = buffer.readInt();

        if (nDigits == 0) {
            return BigDecimal.ZERO.setScale(scale, MathContext.UNLIMITED.getRoundingMode());
        }

        // The decimal point sits after (weight + 1) groups, i.e. at char index dotAt.
        // Each group is decoded into exactly DEC_DIGITS characters (zero-padded).
        // The final char array is constructed directly per case.
        int dotAt = (weight + 1) * DEC_DIGITS;

        String digits;
        if (dotAt <= 0) {
            // All digits are fractional: "0." + "0"*(-dotAt) + groups
            int prefixLen = 2 + (-dotAt);
            char[] out = new char[prefixLen + nDigits * DEC_DIGITS];
            out[0] = '0';
            out[1] = '.';
            for (int z = 0; z < -dotAt; z++) {
                out[2 + z] = '0';
            }
            for (int i = 0; i < nDigits; i++) {
                int decDigit = buffer.readShort() & 0xFFFF;
                for (int j = DEC_DIGITS - 1; j >= 0; j--) {
                    out[prefixLen + i * DEC_DIGITS + j] = (char) ('0' + decDigit % 10);
                    decDigit /= 10;
                }
            }
            digits = new String(out);
        } else if (dotAt >= nDigits * DEC_DIGITS) {
            // All digit groups are integer. setScale cannot be used for negative scales
            // (e.g. 1234567E+1234567 has scale=-1234567 and setScale would round to zero).
            // Instead, strip the trailing padding zeros introduced by group encoding and
            // construct the result directly via new BigDecimal(BigInteger unscaled, int scale).
            char[] out = new char[nDigits * DEC_DIGITS];
            for (int i = 0; i < nDigits; i++) {
                int decDigit = buffer.readShort() & 0xFFFF;
                for (int j = DEC_DIGITS - 1; j >= 0; j--) {
                    out[i * DEC_DIGITS + j] = (char) ('0' + decDigit % 10);
                    decDigit /= 10;
                }
            }
            // E = zeros to append (E>0) or strip (E<0) from the digit string to get the unscaled value
            int E = dotAt - nDigits * DEC_DIGITS + scale;
            int leadStart = 0;
            while (leadStart < out.length && out[leadStart] == '0') leadStart++;
            int trailEnd = out.length + (Math.min(E, 0));
            char[] sig;
            if (leadStart >= trailEnd) {
                sig = new char[]{'0'};
            } else {
                int sigLen = trailEnd - leadStart;
                sig = new char[sigLen + (Math.max(E, 0))];
                System.arraycopy(out, leadStart, sig, 0, sigLen);
                for (int z = sigLen; z < sig.length; z++) sig[z] = '0';
            }
            var bd = new BigDecimal(new BigInteger(new String(sig)), scale);
            return sign == NUMERIC_NEG ? bd.negate() : bd;
        } else {
            // Decimal point sits in the middle of the digit string
            char[] out = new char[nDigits * DEC_DIGITS + 1];
            int pos = 0;
            for (int i = 0; i < nDigits; i++) {
                if (pos == dotAt) {
                    out[pos++] = '.';
                }
                int decDigit = buffer.readShort() & 0xFFFF;
                for (int j = DEC_DIGITS - 1; j >= 0; j--) {
                    out[pos + j] = (char) ('0' + decDigit % 10);
                    decDigit /= 10;
                }
                pos += DEC_DIGITS;
            }
            digits = new String(out);
        }

        var bd = new BigDecimal(digits)
            .setScale(scale, MathContext.UNLIMITED.getRoundingMode());
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
}
