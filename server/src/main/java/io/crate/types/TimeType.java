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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Represents time as microseconds from midnight, ignoring the time zone
 * and storing the value as UTC long.
 * <p>
 * Accepts two kinds of literal:
 * <ol>
 *    <li><b>numeric:</b>
 *      <ul>
 *        <li>short, integer and long values are taken at face value
 *        and range checked.</li>
 *        <li>double and float values are converted to text and parsed
 *        as defined bellow.
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
 * Precision is microseconds (10e6 in a second, unlike TimestampType which is
 * milli seconds 10e3).
 * <p>
 * Accepted range for time values is 0 .. 86400000000 (max number of micros in a day),
 * where both extremes are equivalent ('00:00:00:000000', '24:00:00.000000').
 */
public final class TimeType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final int ID = 19;
    public static final String NAME = "time without time zone";
    public static final TimeType INSTANCE = new TimeType();
    public static final long MAX_MICROS = 24 * 60 * 60 * 1000_000L;


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
            return parseTimeFromFloatingPoint(String.valueOf(((Number) value).doubleValue()));
        }
        if (value instanceof Float) {
            return parseTimeFromFloatingPoint(String.valueOf(((Number) value).floatValue()));
        }
        if (value instanceof Long || value instanceof Number) {
            long v = ((Number) value).longValue();
            return parseFormattedTime(String.valueOf(v), 0L, () -> v);
        }
        if (value instanceof String) {
            return parseTime((String) value);
        }
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "unexpected value [%s] is not a valid literal for TimeType",
            value));
    }

    private static long parseTimeFromFloatingPoint(@Nonnull String time) {
        int dot = time.indexOf(".");
        String format = time.substring(0, dot);
        String microsStr = time.substring(dot + 1);
        int padding = 6 - microsStr.length();
        if (padding > 0) {
            StringBuilder sb = new StringBuilder(6);
            sb.append(microsStr);
            for (int i = 0; i < padding; i++) {
                sb.append("0");
            }
            microsStr = sb.toString();
        }
        long micros = Integer.valueOf(microsStr);
        return parseFormattedTime(format, micros, () -> {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,"value [%s] is not a valid literal for TimeType", time));
        });
    }

    private static long parseFormattedTime(@Nonnull String time, long micros, Supplier<Long> defaultSupplier) {
        switch (time.length()) {
            case 6:
                // hhmmss
                int hh = Integer.parseInt(time.substring(0, 2));
                int mm = Integer.parseInt(time.substring(2, 4));
                int ss = Integer.parseInt(time.substring(4));
                return toEpochMicro(hh, mm, ss, micros);

            case 4:
                // hhmm
                hh = Integer.parseInt(time.substring(0, 2));
                mm = Integer.parseInt(time.substring(2, 4));
                return toEpochMicro(hh, mm, 0, micros);

            case 2:
                // hh
                hh = Integer.parseInt(time.substring(0, 2));
                return toEpochMicro(hh, 0, 0, micros);

            default:
                return checkRange(defaultSupplier.get());
        }
    }

    private static long toEpochMicro(int hh, int mm, int ss, long micros) {
        checkRange("hh", hh, 0, 24);
        checkRange("mm", mm, 0, 59);
        checkRange("ss", ss, 0, 59);
        checkRange("micros", micros, 0, 999999L);
        return checkRange(((((hh * 60 + mm) * 60) + ss) * 1000_000L + micros));
    }

    private static long checkRange(long epochMicro) {
        return checkRange(TimeType.class.getSimpleName(), epochMicro, 0, MAX_MICROS);
    }

    private static long checkRange(String name, long value, long min, long max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value [%d] is out of range for '%s' [0, %d]",
                value, name, max));
        }
        return value;
    }

    public static long parseTime(@Nonnull String format) {
        try {
            long v = Long.parseLong(format);
            return parseFormattedTime(format, 0L, () -> v);
        } catch (NumberFormatException e0) {
            try {
                Double.parseDouble(format);
                return parseTimeFromFloatingPoint(format);
            } catch (NumberFormatException e1) {
                try {
                    // the time zone is ignored if present
                    return LocalTime
                        .parse(format, TIME_PARSER)
                        .getLong(ChronoField.MICRO_OF_DAY);
                } catch (DateTimeParseException e2) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH, "value [%s] is not a valid literal for TimeType", format));
                }
            }
        }
    }

    public static String formatTime(@Nonnull Long time) {
        return LocalTime
            .ofNanoOfDay(time * 1000L)
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
