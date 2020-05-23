/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Represents time as milliseconds from Jan 1st 1970 (EPOCH), ignoring
 * the date portion, the time zone, and storing the value as UTC long.
 * <p>
 * Accepts two kinds of literal:
 * <ol>
 *    <li><b>numeric:</b>
 *      <ul>
 *        <li>short, integer and long values are taken at face value
 *        and range checked.</li>
 *        <li>double and float values are interpreted as seconds.millis
 *        and are range checked. float values loose some precision (milliseconds).
 *        </li>
 *      </ul>
 *    </li>
 *
 *    <li>text:
 *      <ul>
 *        <li>hhmmss: e.g. 23:12:21</li>
 *        <li>hhmm: e.g. 23:12:00</li>
 *        <li>hh: e.g. 23:00:00</li>
 *        <li>hhmmss.ffffff: e.g. 23:12:21.999</li>
 *        <li>hhmm.ffffff: e.g. 23:12:00.999</li>
 *        <li>hh.ffffff: e.g. 23:00:00.999</li>
 *        <li>any ISO-8601 extended local time format</li>
 *      </ul>
 *    </li>
 * </ol>
 *
 * Precision is milli seconds (10e3 in a second, unlike postgres which is
 * micro seconds 10e6) see TimestampType.
 * <p>
 * Accepted range for time values is 0 .. 86400000 (max number of millis in a day).
 */
public final class TimeType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final int ID = 19;
    public static final String NAME = "time without time zone";
    public static final TimeType INSTANCE = new TimeType();
    private static final long MAX_MILLIS = 24 * 60 * 60 * 1000L;


    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Precedence precedence() {
        return Precedence.TIME;
    }

    @Override
    public Streamer<Long> streamer() {
        return this;
    }

    @Override
    public int compare(Long val1, Long val2) {
        return Long.compare(val1, val2);
    }

    @Override
    public Long readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readLong();
    }

    @Override
    public void writeValueTo(StreamOutput out, Long val) throws IOException {
        out.writeBoolean(val == null);
        if (val != null) {
            out.writeLong(val);
        }
    }

    @Override
    public int fixedSize() {
        return LongType.LONG_SIZE;
    }

    @Override
    public Long value(Object value) throws ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return checkRange((long) Math.floor(((Number) value).doubleValue() * 1000L));
        }
        if (value instanceof Float) {
            return checkRange((long) Math.floor(((Number) value).floatValue() * 1000L));
        }
        if (value instanceof Long || value instanceof Number) {
            return checkRange(((Number) value).longValue());
        }
        if (value instanceof String) {
            return parseTime((String) value);
        }
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "unexpected value [%s] is not a valid literal for TimeType",
            value));
    }

    public static long parseTime(@Nonnull String time) {
        try {
            long epochMilli = Long.parseLong(time);
            return toEpochMilli(time, 000, () -> epochMilli);
        } catch (NumberFormatException e0) {
            try {
                long epochMilli = (long) Math.floor(Double.parseDouble(time) * 1000L);
                return toEpochMilli(
                    time.substring(0, time.indexOf(".")),
                    Math.floorMod(epochMilli, 1000),
                    () -> epochMilli);
            } catch (NumberFormatException e1) {
                try {
                    // the time zone is ignored if present
                    LocalTime lt = LocalTime.parse(time, TIME_PARSER);
                    return LocalDateTime
                        .of(ZERO_DATE, lt)
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();
                } catch (DateTimeParseException e2) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH, "value [%s] is not a valid literal for TimeType", time));
                }
            }
        }
    }

    private static long toEpochMilli(@Nonnull String time, int millis, Supplier<Long> defaultSupplier) {
        switch (time.length()) {
            case 6:
                // hhmmss
                int hh = Integer.parseInt(time.substring(0, 2));
                int mm = Integer.parseInt(time.substring(2, 4));
                int ss = Integer.parseInt(time.substring(4));
                return toEpochMilli(hh, mm, ss, millis);

            case 4:
                // hhmm
                hh = Integer.parseInt(time.substring(0, 2));
                mm = Integer.parseInt(time.substring(2, 4));
                return toEpochMilli(hh, mm, 0, millis);

            case 2:
                // hh
                hh = Integer.parseInt(time.substring(0, 2));
                return toEpochMilli(hh, 0, 0, millis);

            default:
                return checkRange(defaultSupplier.get());
        }
    }

    private static long toEpochMilli(int hh, int mm, int ss, int millis) {
        return checkRange((((hh * 60 + mm) * 60) + ss) * 1000L + millis);
    }

    private static long checkRange(long epochMilli) {
        if (epochMilli < 0 || epochMilli >= MAX_MILLIS) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value [%d] is out of range for TimeType [0, %d]",
                epochMilli, MAX_MILLIS));
        }
        return epochMilli;
    }

    public static String formatTime(@Nonnull Long time) {
        return LocalDateTime
            .ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    private static final DateTimeFormatter TIME_PARSER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalStart()
            .appendPattern("[Z][VV][x][xx][xxx]")
        .toFormatter(Locale.ENGLISH)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final LocalDate ZERO_DATE = LocalDate.of(1970, 1, 1);
}
