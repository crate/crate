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
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Represents time as microseconds from midnight, ignoring the time
 * zone and storing the value as UTC <b>long</b>.
 *
 * <p>
 *
 * There are 1000_000 microseconds in one second:
 *
 * <pre>
 *     (24 * 3600 + 59 * 60 + 59) * 1000_000L > Integer.MAX_VALUE
 * </pre>
 *
 * Thus the range for time values is 0 .. 86400000000 (max number
 * of micros in a day), where both extremes are equivalent to
 * '00:00:00:000000' and '24:00:00.000000' respectively.
 *
 * <p>
 *
 * Accepts four kinds of literal:
 * <ol>
 *    <li>text:
 *      <ul>
 *        <li>'hhmmss': e.g. '232121', equivalent to '23:12:21'</li>
 *        <li>'hhmm': e.g. '2312', equivalent to '23:12:00'</li>
 *        <li>'hh': e.g. '23', equivalent to '23:00:00'</li>
 *      </ul>
 *    </li>
 *
 *    <li>numeric:
 *      <ul>
 *        <li>integer and long values are first interpreted as
 *        'text'. Failing this they are kept as is, representing
 *        microseconds from midnight, ignoring the time zone
 *        and storing the value as UTC.
 *    </li>
 *
 *    <li>text high precision:
 *      <p>
 *      Expects up to six digits after the floating point (number of
 *      micro seconds), and it will right pad with zeroes if this is
 *      not the case. For instance the examples below are all padded
 *      to 999000 micro seconds.
 *      <ul>
 *        <li>'hhmmss.ffffff': e.g. '231221.999', equivalent to '23:12:21.999'</li>
 *        <li>'hhmm.ffffff': e.g. '2312.999', equivalent to '23:12:00.999'</li>
 *        <li>'hh.ffffff': e.g. '23.999', equivalent to '23:00:00.999'</li>
 *      </ul>
 *    </li>
 *
 *    <li>numeric high precision:
 *      <ul>
 *        <li>double and float values are interpreted as
 *        'text high precision'. Failing this, the number is not
 *        accepted as a valid literal for time.
 *        </li>
 *      </ul>
 *    </li>
 *
 *    <li>All ISO-8601 extended local time format.</li>
 * </ol>
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
            "unexpected value [%s] is not a valid literal for type %s",
            value, TimeType.class.getSimpleName()));
    }

    private static long parseTimeFromFloatingPoint(@Nonnull String time) {
        int dotIdx = time.indexOf(".");
        String format = time.substring(0, dotIdx);
        String micros = time.substring(dotIdx + 1);
        int padding = 6 - micros.length();
        if (padding > 0) {
            StringBuilder sb = new StringBuilder(6);
            sb.append(micros);
            for (int i = 0; i < padding; i++) {
                sb.append("0");
            }
            micros = sb.toString();
        }
        return parseFormattedTime(format, Integer.valueOf(micros), () -> {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value [%s] is not a valid literal for type %s",
                time, TimeType.class.getSimpleName()));
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
}
