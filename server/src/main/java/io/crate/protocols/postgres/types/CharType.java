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

import io.crate.metadata.RelationLookup;
import io.netty.buffer.ByteBuf;

/**
 * PostgreSQL's special single-byte "char" type (OID 18). CrateDB has no dedicated
 * data type for it; "char" maps character(1), so values are single-character strings.
 * The wire representation is one byte.
 */
class CharType extends PGType<Character> {

    public static final CharType INSTANCE = new CharType();
    static final int OID = 18;

    private static final int TYPE_LEN = 1;
    private static final int TYPE_MOD = -1;

    private CharType() {
        super(OID, TYPE_LEN, TYPE_MOD, "char");
    }

    @Override
    public int typArray() {
        return PGArray.CHAR_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.STRING.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, Character value) {
        byte byteValue = (byte) value.charValue();
        buffer.writeInt(TYPE_LEN);
        buffer.writeByte(byteValue);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Character readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == TYPE_LEN : "\"char\" must have 1 byte";
        return (char) buffer.readByte();
    }

    @Override
    byte[] encodeAsUTF8Text(Character value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Character decodeUTF8Text(byte[] bytes, RelationLookup relationLookup) {
        return (char) bytes[0];
    }
}
