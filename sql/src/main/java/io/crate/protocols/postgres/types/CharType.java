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

import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

class CharType extends PGType {

    public static final PGType INSTANCE = new CharType();
    static final int OID = 18;


    private CharType() {
        super(OID, 1, -1, "char");
    }

    @Override
    public int writeAsBinary(ChannelBuffer buffer, @Nonnull Object value) {
        buffer.writeInt(1);
        buffer.writeByte(((byte) value));
        return 5;
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        assert valueLength == 1: "char must have 1 byte";
        return buffer.readByte();
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull Object value) {
        return Byte.toString((byte) value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        return Byte.parseByte(new String(bytes, StandardCharsets.UTF_8));
    }
}
