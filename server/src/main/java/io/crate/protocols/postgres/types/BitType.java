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

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import io.crate.sql.tree.BitString;
import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public class BitType extends PGType<BitString> {

    public static final int OID = 1560;
    public static final BitType INSTANCE = new BitType();
    private int length;

    public BitType() {
        super(OID, -1, -1, "bit");
    }

    public BitType(int length) {
        super(OID, -1, length, "bit");
        this.length = length;
    }

    @Override
    public int typArray() {
        return PGArray.BIT_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.BIT_STRING.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public Regproc typSend() {
        return Regproc.of("bit_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of("bit_recv");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, BitString value) {
        BitSet bitSet = value.bitSet();
        int bitLen = value.length();
        int byteLen = (bitLen + 7) / 8;
        // PostgreSQL uses a different encoding than BitSet, so can't use toByteArray()
        // This was cobbled together reading PostgreSQL/asyncpg sources
        byte[] bytes = new byte[byteLen];
        for (int i = 0; i < bitLen; ++i) {
            bytes[i / 8] |= ((0x80 >> (i % 8)) & (bitSet.get(i) ? 0xff : 0x00));
        }
        buffer.writeInt(INT32_BYTE_SIZE + bytes.length);
        buffer.writeInt(bitLen);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + INT32_BYTE_SIZE + byteLen;
    }

    @Override
    public BitString readBinaryValue(ByteBuf buffer, int valueLength) {
        int bitLen = buffer.readInt();
        int byteLen = (bitLen + 7) / 8;
        byte[] bytes = new byte[byteLen];
        buffer.readBytes(bytes);
        BitSet bitSet = new BitSet();
        for (int i = 0; i < bitLen; i++) {
            int b = bytes[i / 8];
            int shift = 8 - i % 8 - 1;
            int result = (b >> shift) & 0x1;
            bitSet.set(i, result == 1);
        }
        return new BitString(bitSet, length);
    }

    @Override
    byte[] encodeAsUTF8Text(BitString value) {
        assert length >= 0 : "BitType length must be set";
        // See `varbit_out` in src/backend/utils/adt/varbit.c of PostgreSQL
        return value.asRawBitString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    BitString decodeUTF8Text(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        // See `varbit_in` in src/backend/utils/adt/varbit.c of PostgreSQL
        // PostgreSQL also supports hex notation, that's currently unsupported.

        // bit-or 32 normalizes ascii-upper-case:
        // "Bar".charAt(0) | 32 == 'b'
        // "bar".charAt(0) | 32 == 'b'
        if (!text.isEmpty() && (text.charAt(0) | 32) == 'b') {
            return BitString.ofBitString(text);
        } else {
            if (length > 0) {
                return BitString.ofRawBits(text, length);
            } else {
                return BitString.ofRawBits(text);
            }
        }
    }
}
