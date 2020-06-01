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
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;

import static io.crate.types.TimeTZParser.timeTZOf;
import static io.crate.types.TimeTZParser.exceptionForInvalidLiteral;

/**
 * Represents time as microseconds from midnight, ignoring the time
 * zone and storing the value as UTC <b>long</b>.
 * <p>
 * There are 1000_000 microseconds in one second:
 *
 * <pre>
 *     (24 * 3600 + 59 * 60 + 59) * 1000_000L > Integer.MAX_VALUE
 * </pre>
 * <p>
 * Thus the range for time values is 0 .. 86400000000 (max number
 * of micros in a day), where both extremes are equivalent to
 * '00:00:00.000000' and '24:00:00.000000' respectively.
 *
 * <p>
 * <p>
 * Accepts three kinds of literal:
 * <ol>
 *    <li>text:
 *      <ul>
 *        <li>'hh[:]mm[:]ss': e.g. '232121', equivalent to '23:12:21'</li>
 *        <li>'hh[:]mm': e.g. '2312', equivalent to '23:12:00'</li>
 *        <li>'hh': e.g. '23', equivalent to '23:00:00'</li>
 *      </ul>
 *    </li>
 *
 *    <li>text high precision:
 * <p>
 *      Expects up to six digits after the floating point (number of
 *      micro seconds), and it will right pad with zeroes, or truncate,
 *      if this is not the case. For instance the examples below are
 *      all padded to 999000 micro seconds.
 *      <ul>
 *        <li>'hh[:]mm[:]ss.ffffff': e.g. '231221.999', equivalent to '23:12:21.999000'</li>
 *        <li>'hh[:]mm.ffffff': e.g. '2312.999', equivalent to '23:12:00.999000'</li>
 *        <li>'hh.ffffff': e.g. '23.999', equivalent to '23:00:00.999000'</li>
 *      </ul>
 *    </li>
 *
 *    <li>All ISO-8601 extended local time format.</li>
 * </ol>
 */
public final class TimeTZType extends DataType<TimeTZ> implements FixedWidthType, Streamer<TimeTZ> {

    public static final int ID = 19;
    public static final int TYPE_LEN = 12;
    public static final String NAME = "time with time zone";
    public static final TimeTZType INSTANCE = new TimeTZType();


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
        return Precedence.TIMETZ;
    }

    @Override
    public Streamer<TimeTZ> streamer() {
        return this;
    }

    @Override
    public int compare(TimeTZ val1, TimeTZ val2) {
        return val1.compareTo(val2);
    }

    @Override
    public TimeTZ readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return null;
        }
        return new TimeTZ(in.readLong(), in.readInt());
    }

    @Override
    public void writeValueTo(StreamOutput out, TimeTZ tz) throws IOException {
        out.writeBoolean(tz == null);
        if (tz != null) {
            out.writeLong(tz.getTime());
            out.writeInt(tz.getSecondsFromUTC());
        }
    }

    @Override
    public int fixedSize() {
        return TYPE_LEN;
    }

    @Override
    public TimeTZ value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof TimeTZ) {
            return (TimeTZ) value;
        }
        if (value instanceof Long || value instanceof Integer) {
            return timeTZOf(
                TimeTZType.class.getSimpleName(),
                ((Number) value).longValue());
        }
        if (value instanceof String) {
            try {
                try {
                    return parseTime((String) value);
                } catch (IllegalArgumentException e0) {
                    return timeTZOf(
                        TimeTZType.class.getSimpleName(),
                        Long.valueOf((String) value));
                }
            } catch (NumberFormatException e1) {



                return new TimeTZ(
                    LocalTime
                        .parse((String) value, TIME_PARSER)
                        .getLong(ChronoField.MICRO_OF_DAY),
                    0);
            }
        }
        throw exceptionForInvalidLiteral(value);
    }

    public static String formatTime(@Nonnull TimeTZ time) {
        return TimeTZParser.formatTime(time);
    }

    public static TimeTZ parseTime(@Nonnull String time) {
        return TimeTZParser.parseTime(time);
    }

    private static final DateTimeFormatter TIME_PARSER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalStart()
        .appendPattern("[Z][VV][x][xx][xxx]")
        .toFormatter(Locale.ENGLISH)
        .withResolverStyle(ResolverStyle.STRICT);
}
