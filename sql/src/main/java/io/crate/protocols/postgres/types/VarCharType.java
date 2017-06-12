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
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class VarCharType extends PGType {

    public static final PGType INSTANCE = new VarCharType();
    static final int OID = 1043;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    private VarCharType() {
        super(OID, TYPE_LEN, TYPE_MOD, "varchar");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value) {
        BytesRef bytesRef = (BytesRef) value;
        buffer.writeInt(bytesRef.length);
        buffer.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        return INT32_BYTE_SIZE + bytesRef.length;
    }

    @Override
    public int writeAsText(ByteBuf buffer, @Nonnull Object value) {
        return writeAsBinary(buffer, value);
    }

    @Override
    protected byte[] encodeAsUTF8Text(@Nonnull Object value) {
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        BytesRef bytesRef = (BytesRef) value;
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.length + bytesRef.offset);
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength) {
        BytesRef bytesRef = new BytesRef(valueLength);
        bytesRef.length = valueLength;
        buffer.readBytes(bytesRef.bytes);
        return bytesRef;
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        return new BytesRef(bytes);
    }
}
