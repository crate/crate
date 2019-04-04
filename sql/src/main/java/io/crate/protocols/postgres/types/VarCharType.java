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
import java.nio.charset.StandardCharsets;

class VarCharType extends PGType {

    static final int OID = 1043;
    private static final int ARRAY_OID = 1015;
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final PGType INSTANCE = new VarCharType(ARRAY_OID);

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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value) {
        assert value instanceof String : "value must be a string, got: " + value;
        byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    @Override
    public int writeAsText(ByteBuf buffer, @Nonnull Object value) {
        return writeAsBinary(buffer, value);
    }

    @Override
    protected byte[] encodeAsUTF8Text(@Nonnull Object value) {
        assert value instanceof String : "value must be a string";
        return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength) {
        byte[] utf8 = new byte[valueLength];
        buffer.readBytes(utf8);
        return new String(utf8, StandardCharsets.UTF_8);
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static class NameType {
        static final int OID = 19;
        private static final int ARRAY_OID = -1;
        private static final int TYPE_LEN = 64;

        static final PGType INSTANCE = new VarCharType(OID, ARRAY_OID, TYPE_LEN, "name");
    }
}
