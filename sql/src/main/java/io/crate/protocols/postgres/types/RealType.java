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

class RealType extends PGType {

    static final int OID = 700;

    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    RealType() {
        super(OID, TYPE_LEN, TYPE_MOD);
    }

    @Override
    public int writeAsBinary(ChannelBuffer buffer, @Nonnull Object value) {
        buffer.writeInt(4);
        buffer.writeFloat(((float) value));
        return 8;
    }

    @Override
    protected byte[] encodeAsUTF8Text(@Nonnull Object value) {
        return Float.toString(((float) value)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object readBinaryValue(ChannelBuffer buffer, int valueLength) {
        assert valueLength == 4 : "length should be 4 because float is int32. Actual length: " + valueLength;
        return buffer.readFloat();
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        return Float.parseFloat(new String(bytes, StandardCharsets.UTF_8));
    }
}
