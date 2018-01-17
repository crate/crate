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

    // 1st msec where BC date becomes AD date
    private static final long FIRST_MSEC_AFTER_CHRIST = -62135596800000L;

    // ISO is the default - postgres allows changing the format but that's currently not supported

    // For Golang if date is AD (after Christ), era abbreviation is not parsed.
    private static final DateTimeFormatter ISO_FORMATTER =
        DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS+00").withZoneUTC().withLocale(Locale.ENGLISH);
    // For Golang if date is BC (before Christ), era abbreviation needs to be appended.
    private static final DateTimeFormatter ISO_FORMATTER_WITH_ERA =
        DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS+00 G").withZoneUTC().withLocale(Locale.ENGLISH);


    private static final DateTimeFormatter[] PARSERS_WITHOUT_ERA = generateParseFormatters(false);
    private static final DateTimeFormatter[] PARSERS_WITH_ERA = generateParseFormatters(true);

    private static DateTimeFormatter[] generateParseFormatters(boolean withEra) {
        DateTimeFormatter[] formatters = new DateTimeFormatter[10];
        String prefix = "YYYY-MM-dd HH:mm:ss";
        String suffix = "ZZ";
        if (withEra) {
            suffix = "ZZ G";
        }

        formatters[0] = DateTimeFormat.forPattern(prefix + suffix).withLocale(Locale.ENGLISH);
        for (int i = 1; i < 10; i++) { // 1-9 digits for fraction of second
            StringBuilder pattern = new StringBuilder(prefix);
            pattern.append('.');
            for (int j = 1; j <= i; j++) {
                pattern.append('S');
            }
            pattern.append(suffix);
            formatters[i] = DateTimeFormat.forPattern(pattern.toString()).withLocale(Locale.ENGLISH);
        }
        return formatters;
    }

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
        long msecs = (long) value;
        if (msecs >= FIRST_MSEC_AFTER_CHRIST) {
            return ISO_FORMATTER.print(msecs).getBytes(StandardCharsets.UTF_8);
        } else {
            return ISO_FORMATTER_WITH_ERA.print(msecs).getBytes(StandardCharsets.UTF_8);
        }
    }


    @Override
    Object decodeUTF8Text(byte[] bytes) {
        // Currently seems that only GoLang prepared statements are sent as TimestampType
        // Other PostgreSQL clients send the parameter as Bigint or Varchar
        String s = new String(bytes, StandardCharsets.UTF_8);

        int endOfSeconds  = s.indexOf(".");
        int idx = endOfSeconds;
        if (endOfSeconds > 0) {
            idx++;
            while (s.charAt(idx) != '+' && s.charAt(idx) != '-') { // start of timezone
                idx++;
            }
        }

        int fractionDigits = idx - endOfSeconds - 1;
        fractionDigits = fractionDigits < 0 ? 0 : fractionDigits;
        if (fractionDigits > 9) {
            throw new IllegalArgumentException("Cannot parse more than 9 digits for fraction of a second");
        }

        boolean withEra = s.endsWith("BC") || s.endsWith("AD");
        if (withEra) {
            return PARSERS_WITH_ERA[fractionDigits].parseMillis(s);
        }
        return PARSERS_WITHOUT_ERA[fractionDigits].parseMillis(s);
    }
}
