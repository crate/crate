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

import io.crate.types.TimeTZ;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static io.crate.types.TimeTZParser.formatTime;
import static io.crate.types.TimeTZParser.parse;
import static io.crate.types.TimeTZType.NAME;
import static io.crate.types.TimeTZType.TYPE_LEN;


final class TimeTZType extends PGType<TimeTZ> {

    public static final PGType<TimeTZ> INSTANCE = new TimeTZType();
    private static final int OID = 1266;
    private static final int TYPE_MOD = -1;


    TimeTZType() {
        super(OID, TYPE_LEN, TYPE_MOD, NAME);
    }

    @Override
    public int typArray() {
        return PGArray.TIMETZ_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.DATETIME.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull TimeTZ value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeLong(value.getMicrosFromMidnight());
        buffer.writeInt(value.getSecondsFromUTC());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public TimeTZ readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == TYPE_LEN : String.format(
            Locale.ENGLISH,
            "valueLength must be %d because timetz is a 12 byte structure. Actual length: %d",
            TYPE_LEN, valueLength);
        return new TimeTZ(buffer.readLong(), buffer.readInt());
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull TimeTZ time) {
        return formatTime(time).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    TimeTZ decodeUTF8Text(byte[] bytes) {
        return parse(new String(bytes, StandardCharsets.UTF_8));
    }
}
