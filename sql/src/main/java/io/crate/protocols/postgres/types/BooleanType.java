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
import java.util.Collection;
import java.util.Locale;

class BooleanType extends PGType {

    static final int OID = 16;

    private static final int TYPE_LEN = 1;
    private static final int TYPE_MOD = -1;
    private static final short DEFAULT_FORMAT_CODE = FormatCode.TEXT;
    private static final Collection<String> TRUTH_VALUES = ImmutableSet.of(
        "t", "true", "o", "on", "1");

    BooleanType() {
        super(OID, TYPE_LEN, TYPE_MOD, DEFAULT_FORMAT_CODE);
    }

    @Override
    public int writeValue(ChannelBuffer buffer, @Nonnull Object value) {
        byte byteValue = (byte) ((boolean) value ? 't' : 'f');
        buffer.writeInt(TYPE_LEN);
        buffer.writeByte(byteValue);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Object readTextValue(ChannelBuffer buffer, int valueLength) {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        String value = new String(bytes).toLowerCase(Locale.ENGLISH);
        return TRUTH_VALUES.contains(value);
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        byte value = buffer.readByte();
        return value != 0;
    }
}
