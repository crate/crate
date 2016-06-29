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

import com.google.common.collect.ImmutableSet;
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Collection;

class BooleanType extends PGType {

    static final int OID = 16;

    private static final int TYPE_LEN = 1;
    private static final int TYPE_MOD = -1;

    private final static byte[] TEXT_TRUE = new byte[] { 't' };
    private final static byte[] TEXT_FALSE = new byte[] { 'f' };

    private static final Collection<ByteBuffer> TRUTH_VALUES = ImmutableSet.of(
        ByteBuffer.wrap(new byte[] { '1' }),
        ByteBuffer.wrap(new byte[] { 't' }),
        ByteBuffer.wrap(new byte[] { 'T' }),
        ByteBuffer.wrap(new byte[] { 't', 'r', 'u', 'e'}),
        ByteBuffer.wrap(new byte[] { 'T', 'R', 'U', 'E'})
    );

    BooleanType() {
        super(OID, TYPE_LEN, TYPE_MOD);
    }

    @Override
    public int writeAsBytes(ChannelBuffer buffer, @Nonnull Object value) {
        byte byteValue = (byte) ((boolean) value ? 1 : 0);
        buffer.writeInt(TYPE_LEN);
        buffer.writeByte(byteValue);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    byte[] asUTF8StringBytes(@Nonnull Object value) {
        if ((boolean) value) {
            return TEXT_TRUE;
        }
        return TEXT_FALSE;
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        assert valueLength == 1 : "length should be 1 because boolean is just a byte. Actual length: " + valueLength;
        byte value = buffer.readByte();
        switch (value) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw new IllegalArgumentException("Unsupported binary bool: " + value);
        }
    }

    @Override
    Object valueFromUTF8Bytes(byte[] bytes) {
        return TRUTH_VALUES.contains(ByteBuffer.wrap(bytes));
    }
}
