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

class VarCharType extends PGType<Object> {

    static final int OID = 1043;
    private static final int ARRAY_OID = 1015;
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final VarCharType INSTANCE = new VarCharType(ARRAY_OID);

    private final int typArray;

    private VarCharType(int typArray) {
        super(OID, TYPE_LEN, TYPE_MOD, "varchar");
        this.typArray = typArray;
    }

    private VarCharType(int oid, int typArray, int maxLength, String aliasName) {
        super(oid, maxLength, TYPE_MOD, aliasName);
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
    public int writeAsBinary(ByteBuf buffer, Object value) {
        String string = DataTypes.STRING.implicitCast(value);
        int writerIndex = buffer.writerIndex();
        buffer.writeInt(0);
        int bytesWritten = buffer.writeCharSequence(string, StandardCharsets.UTF_8);
        buffer.setInt(writerIndex, bytesWritten);
        return INT32_BYTE_SIZE + bytesWritten;
    }

    @Override
    public int writeAsText(ByteBuf buffer, Object value) {
        return writeAsBinary(buffer, value);
    }

    @Override
    protected byte[] encodeAsUTF8Text(Object value) {
        return DataTypes.STRING.implicitCast(value).getBytes(StandardCharsets.UTF_8);
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

    static class NameType {
        static final int OID = 19;
        private static final int ARRAY_OID = -1;
        private static final int TYPE_LEN = 64;

        static final VarCharType INSTANCE = new VarCharType(OID, ARRAY_OID, TYPE_LEN, "name");
    }

    static class TextType {
        static final int OID = 25;
        static final int TEXT_ARRAY_OID = 1009;
        static final VarCharType INSTANCE = new VarCharType(OID, TEXT_ARRAY_OID, -1, "text");
    }
}
