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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

class TimestampType extends PGType {

    public static final PGType INSTANCE = new TimestampType();

    /**
     * this oid is TIMESTAMPZ (with timezone) instead of TIMESTAMP
     * the timezone is always GMT
     * <p>
     * If TIMESTAMP was used resultSet.getTimestamp() would convert the timestamp to a local time.
     */
    private static final int OID = 1184;
    private static final int TYPE_LEN = 8;
    private static final int TYPE_MOD = -1;

    // amount of seconds between 1970-01-01 and 2000-01-01
    private static final int EPOCH_DIFF = 946684800;


    // ISO is the default - postgres allows changing the format but that's currently not supported
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS +00 G").withZoneUTC().withLocale(Locale.ENGLISH);


    private TimestampType() {
        super(OID, TYPE_LEN, TYPE_MOD, "timestampz");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeDouble(toPgTimestamp((long) value));
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    /**
     * Convert a crate timestamp (unix timestamp in ms) into a postgres timestamp (double seconds since 2000-01-01)
     */
    private static double toPgTimestamp(long value) {
        double seconds = value / 1000.0;
        return seconds - EPOCH_DIFF;
    }

    /**
     * Convert a postgres timestamp (seconds since 2000-01-01) into a crate timestamp (unix timestamp in ms)
     */
    private static long toCrateTimestamp(double v) {
        return (long) ((v + EPOCH_DIFF) * 1000.0);
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == TYPE_LEN : "valueLength must be " + TYPE_LEN +
                                         " because timestamp is a 64 bit double. Actual length: " + valueLength;
        return toCrateTimestamp(buffer.readDouble());
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull Object value) {
        return ISO_FORMATTER.print((long) value).getBytes(StandardCharsets.UTF_8);
    }


    @Override
    Object decodeUTF8Text(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);
        return ISO_FORMATTER.parseMillis(s);
    }
}
