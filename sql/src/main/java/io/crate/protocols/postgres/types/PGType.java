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

    static final int INT32_BYTE_SIZE = Integer.SIZE / 8;

    private final int oid;
    private final int typeLen;
    private final int typeMod;

    PGType(int oid, int typeLen, int typeMod) {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
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


    /**
     * Write the value as text into the buffer.
     *
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public final int writeAsText(ChannelBuffer buffer, @Nonnull Object value) {
        byte[] bytes = asUTF8StringBytes(value);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    public Object readTextValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return valueFromUTF8Bytes(bytes);
    }

    /**
     * Write the value as binary into the buffer.
     *
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public abstract int writeAsBytes(ChannelBuffer buffer, @Nonnull Object value);

    public abstract Object readBinaryValue(ChannelBuffer buffer, int valueLength);


    /**
     * Return the UTF8 encoded text representation of the value
     */
    abstract byte[] asUTF8StringBytes(@Nonnull Object value);

    /**
     * Convert a UTF8 encoded text representation into the actual value
     */
    abstract Object valueFromUTF8Bytes(byte[] bytes);
}
