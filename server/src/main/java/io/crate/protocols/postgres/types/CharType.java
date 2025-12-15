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

import io.netty.buffer.ByteBuf;

class CharType extends PGType<Byte> {

    public static final CharType INSTANCE = new CharType();
    static final int OID = 18;


    private CharType() {
        super(OID, 1, -1, "char");
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
    public int writeAsBinary(ByteBuf buffer, Byte value) {
        buffer.writeInt(1);
        buffer.writeByte(value);
        return 5;
    }

    @Override
    public Byte readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == 1 : "char must have 1 byte";
        return buffer.readByte();
    }

    @Override
    byte[] encodeAsUTF8Text(Byte value) {
        return Byte.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Byte decodeUTF8Text(byte[] bytes) {
        return Byte.parseByte(new String(bytes, StandardCharsets.UTF_8));
    }
}
