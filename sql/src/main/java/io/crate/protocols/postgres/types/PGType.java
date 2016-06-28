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

public abstract class PGType {

    public static class FormatCode {
        public static final short TEXT = 0;
        public static final short BINARY = 1;
    }

    static final int INT32_BYTE_SIZE = Integer.SIZE / 8;

    private final int oid;
    private final int typeLen;
    private final int typeMod;
    private final short formatCode;

    PGType(int oid, int typeLen, int typeMod, short formatCode) {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
        this.formatCode = formatCode;
    }

    public int oid() {
        return oid;
    }

    public int typeLen() {
        return typeLen;
    }

    public int typeMod() {
        return typeMod;
    }

    public short formatCode() {
        return formatCode;
    }

    /**
     * write | int32 len | byteN value onto the buffer
     * @return the number of bytes written. (4 + N)
     */
    public abstract int writeValue(ChannelBuffer buffer, @Nonnull Object value);

    public abstract Object readBinaryValue(ChannelBuffer buffer, int valueLength);

    abstract Object valueFromUTF8Bytes(byte[] bytes);

    public Object readTextValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return valueFromUTF8Bytes(bytes);
    }
}
