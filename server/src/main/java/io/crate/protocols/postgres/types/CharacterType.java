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

import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;

class CharacterType extends PGType<String> {

    static final int OID = 1042;
    private static final int ARRAY_OID = 1014;
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final CharacterType INSTANCE = new CharacterType(ARRAY_OID);

    private final int typArray;

    private CharacterType(int typArray) {
        super(OID, TYPE_LEN, TYPE_MOD, "bpchar");
        this.typArray = typArray;
    }

    @Override
    public int typArray() {
        return typArray;
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.STRING.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, String value) {
        int writerIndex = buffer.writerIndex();
        buffer.writeInt(0);
        int bytesWritten = buffer.writeCharSequence(value, StandardCharsets.UTF_8);
        buffer.setInt(writerIndex, bytesWritten);
        return INT32_BYTE_SIZE + bytesWritten;
    }

    @Override
    public int writeAsText(ByteBuf buffer, String value) {
        return writeAsBinary(buffer, value);
    }

    @Override
    protected byte[] encodeAsUTF8Text(String value) {
        return DataTypes.CHARACTER.implicitCast(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String readTextValue(ByteBuf buffer, int valueLength) {
        return readBinaryValue(buffer, valueLength);
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength) {
        int readerIndex = buffer.readerIndex();
        buffer.readerIndex(readerIndex + valueLength);
        return buffer.toString(readerIndex, valueLength, StandardCharsets.UTF_8);
    }

    @Override
    String decodeUTF8Text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
