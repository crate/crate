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
import java.util.EnumMap;
import java.util.Locale;

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
        if (value instanceof Long || value instanceof Integer) {
            return checkRange(
                TimeType.class.getSimpleName(),
                ((Number) value).longValue(),
                MAX_MICROS);
        }
        if (value instanceof String) {
            try {
                try {
                    return parseTime((String) value);
                } catch (IllegalArgumentException e0) {
                    return Long.valueOf((String) value);
                }
            } catch (NumberFormatException e1) {
                return LocalTime
                    .parse((String) value, TIME_PARSER)
                    .getLong(ChronoField.MICRO_OF_DAY);
            }
        }
        throw exceptionForInvalidLiteral(value);
    }

    public static String formatTime(@Nonnull Long time) {
        return LocalTime
            .ofNanoOfDay(time * 1000L)
            .format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    private enum State {
        HH(24),
        MM(59),
        SS(59),
        MICRO(999999) {
            @Override
            long validate (String value, int start, int end) {
                String s = value.substring(start, Math.min(start + 6, end));
                try {
                    int v = Integer.parseInt(s);
                    for (int i = 0; i < 6 - s.length(); i++) {
                        v *= 10;
                    }
                    return checkRange(name(), v, 999999);
                } catch (NumberFormatException e) {
                    throw exceptionForInvalidLiteral(value);
                }
            }
        };

        private int max;

        State(int max) {
            this.max = max;
        }

        State next() {
            State[] st = values();
            return st[(ordinal() + 1) % st.length];
        }

        long validate (String value, int start, int end) {
            try {
                return checkRange(name(), Integer.valueOf(value.substring(start, end)), max);
            } catch (NumberFormatException e) {
                throw exceptionForInvalidLiteral(value);
            }
        }
    }

    public static long parseTime(@Nonnull String format) {
        EnumMap<State, Long> values = new EnumMap<>(State.class);
        int i = 0;
        int start = 0;
        int colonCount = 0;
        State state = State.HH;
        for (; i < format.length(); i++) {
            char c = format.charAt(i);
            if (Character.isDigit(c)) {
                if (i - start != 2) {
                    continue;
                } else {
                    values.put(state, state.validate(format, start, i));
                    state = state.next();
                    start = i;
                }
            } else if (c == ':') {
                values.put(state, state.validate(format, start, i));
                state = state.next();
                start = i + 1;
                colonCount++;
            } else if (c == '.') {
                if (i - start != 2) {
                    throw exceptionForInvalidLiteral(format);
                }
                values.put(state, state.validate(format, start, i));
                state = State.MICRO;
                start = i + 1;
                break;
            } else {
                throw exceptionForInvalidLiteral(format);
            }
        }
        if (state != State.MICRO && format.length() - start != 2) {
            throw exceptionForInvalidLiteral(format);
        }
        if (colonCount == 1 && values.get(State.SS) != null) {
            throw exceptionForInvalidLiteral(format);
        }
        values.put(state, state.validate(format, start, format.length()));
        long hh = values.get(State.HH);
        long mm = values.getOrDefault(State.MM, 0L);
        long ss = values.getOrDefault(State.SS, 0L);
        long micros = values.getOrDefault(State.MICRO, 0L);
        return checkRange(
            format,
            (((hh * 60 + mm) * 60) + ss) * 1000_000L + micros,
            MAX_MICROS);
    }

    private static IllegalArgumentException exceptionForInvalidLiteral(Object literal) {
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "value [%s] is not a valid literal for %s",
            literal, TimeType.class.getSimpleName()));
    }

    static long checkRange(String name, long value, long max) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value [%d] is out of range for '%s' [0, %d]",
                value, name, max));
        }
        return value;
    }

    private static final DateTimeFormatter TIME_PARSER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalStart()
        .appendPattern("[Z][VV][x][xx][xxx]")
        .toFormatter(Locale.ENGLISH)
        .withResolverStyle(ResolverStyle.STRICT);
}
