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

class BigIntType extends PGType {

    static final int OID = 20;

    private static final int TYPE_LEN = 8;
    private static final int TYPE_MOD = -1;

    BigIntType() {
        super(OID, TYPE_LEN, TYPE_MOD);
    }

    @Override
    public int writeAsBinary(ChannelBuffer buffer, @Nonnull Object value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeLong(((long) value));
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    protected byte[] encodeAsUTF8Text(@Nonnull Object value) {
        return Long.toString(((long) value)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        assert valueLength == TYPE_LEN: "length should be 8 because long is int64. Actual length: " + valueLength;
        return buffer.readLong();
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        return Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
    }
}
