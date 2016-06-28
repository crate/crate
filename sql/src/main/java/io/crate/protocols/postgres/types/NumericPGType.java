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
import java.nio.ByteBuffer;
import java.util.Locale;

abstract class NumericPGType<T extends Number> extends PGType {

    public NumericPGType(int oid, int typeLen, int typeMod, short formatCode) {
        super(oid, typeLen, typeMod, formatCode);
    }

    abstract void writeTo(ByteBuffer byteBuffer, Object value);

    abstract T readFrom(ByteBuffer byteBuffer);

    abstract T fromString(String s);

    @Override
    public int writeValue(ChannelBuffer buffer, @Nonnull Object value) {
        buffer.writeInt(typeLen());

        ByteBuffer byteBuffer = buffer.toByteBuffer(buffer.writerIndex(), typeLen());
        writeTo(byteBuffer, value);

        return INT32_BYTE_SIZE + typeLen();
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        return readFrom(wrapped);
    }

    @Override
    public Object readTextValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        String value = new String(bytes).toLowerCase(Locale.ENGLISH);
        return fromString(value);
    }
}
