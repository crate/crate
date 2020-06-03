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

import javax.annotation.Nonnull;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.Locale;

/**
 * Represents time with time zone as microseconds from midnight,
 * and zone as signed seconds from UTC.
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
 * Accepts three kinds of literal (with time zone offset suffix):
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
 *
 * Time zone offset suffix:
 * <p>
 * The above formats accept a suffix (-|+)HH[[:]MM[[:]SS]] (and all ISO-8601
 * compliant) to define the time zone. If the time zone is not defined,
 * then <b>UTC is implicit</>. Time zone values are limited to 14:59:59 in
 * either direction as per postgres specification.
 */
public final class TimeTZParser {

    public static final long MAX_MICROS = 24 * 60 * 60 * 1000_000L;
    static final int LOCAL_TZ_SECONDS_FROM_UTC = 0;


    public static TimeTZ timeTZOf(String source, long value) {
        return new TimeTZ(checkRange(source, value, MAX_MICROS), LOCAL_TZ_SECONDS_FROM_UTC);
    }

    private enum State {
        HH(24),
        MM(59),
        SS(59),
        MICRO(999999, 6),
        ZID_HH(14),
        ZID_MM(59),
        ZID_SS(59);

        private int maxValue;
        private int maxDigits;

        State(int maxValue) {
            this(maxValue, 2);
        }

        State(int maxValue, int maxDigits) {
            this.maxValue = maxValue;
            this.maxDigits = maxDigits;
        }

        State next() {
            State[] st = values();
            return st[(ordinal() + 1) % st.length];
        }

        long validate(String value, int start, int end) {
            String s = value.substring(start, Math.min(start + maxDigits, end));
            try {
                int v = Integer.parseInt(s);
                for (int i = 0; i < maxDigits - s.length(); i++) {
                    v *= 10;
                }
                return checkRange(name(), v, maxValue);
            } catch (NumberFormatException e) {
                throw exceptionForInvalidLiteral(value);
            }
        }
    }

    private static EnumMap<State, Long> parse(@Nonnull String format,
                                              State initialState,
                                              int startOffset,
                                              boolean microsEnabled) {
        EnumMap<State, Long> values = new EnumMap<>(State.class);
        int colonCount = 0;
        State state = initialState;
        int i = startOffset;
        int start = startOffset;
        for (; i < format.length(); i++) {
            char c = format.charAt(i);
            if (Character.isDigit(c)) {
                if (i - start != 2 || state == State.MICRO) {
                    continue;
                } else {
                    values.put(state, state.validate(format, start, i));
                    state = state.next();
                    if (state == State.MICRO) {
                        // missing compulsory '.'
                        throw exceptionForInvalidLiteral(format);
                    }
                    start = i;
                }
            } else if (c == ':') {
                colonCount++;
                if (state == State.SS && colonCount == 1) {
                    throw exceptionForInvalidLiteral(format);
                }
                values.put(state, state.validate(format, start, i));
                state = state.next();
                start = i + 1;
            } else if (c == '.') {
                if (i - start != 2 || !microsEnabled) {
                    throw exceptionForInvalidLiteral(format);
                }
                values.put(state, state.validate(format, start, i));
                state = State.MICRO;
                start = i + 1;
            } else if (c == '+' || c == '-' || c == ' ' || Character.isLetter(c)) {
                if (!microsEnabled) {
                    throw exceptionForInvalidLiteral(format);
                }
                break;
            } else {
                throw exceptionForInvalidLiteral(format);
            }
        }
        if (state != State.MICRO && i - start != 2) {
            throw exceptionForInvalidLiteral(format);
        }
        if (state == State.MICRO && !microsEnabled) {
            throw exceptionForInvalidLiteral(format);
        }
        values.put(state, state.validate(format, start, i));
        return values;
    }

    private static int findZoneStart(@Nonnull String format) {
        int startOffset = 0;
        while (startOffset < format.length()) {
            char c = format.charAt(startOffset);
            if (c == '+' || c == '-' || Character.isLetter(c)) {
                // will skip spaces in between time and zone
                return startOffset;
            }
            startOffset++;
        }
        return -1;
    }

    private static long parseJustTime(@Nonnull String format) {
        EnumMap<State, Long> time = parse(format, State.HH, 0, true);
        long hh = time.get(State.HH);
        long mm = time.getOrDefault(State.MM, 0L);
        long ss = time.getOrDefault(State.SS, 0L);
        long micros = time.getOrDefault(State.MICRO, 0L);
        return checkRange(
            format,
            (((hh * 60 + mm) * 60) + ss) * 1000_000L + micros,
            MAX_MICROS);
    }

    private static int parseJustZone(@Nonnull String format) {
        int zoneSecondsFromUTC = LOCAL_TZ_SECONDS_FROM_UTC;
        int zoneStart = findZoneStart(format);
        if (-1 != zoneStart) {
            char c = format.charAt(zoneStart);
            if (Character.isLetter(c)) {
                try {
                    String zoneId = format.substring(zoneStart).strip();
                    zoneSecondsFromUTC = ZoneId
                        .of(zoneId)
                        .getRules()
                        .getOffset(Instant.EPOCH)
                        .getTotalSeconds();
                } catch (DateTimeException e) {
                    throw exceptionForInvalidLiteral(format);
                }
            } else {
                long zoneSign = '+' == c ? 1L : -1L;
                EnumMap<State, Long> zone = parse(format, State.ZID_HH, zoneStart + 1, false);
                long zoneHH = zone.getOrDefault(State.ZID_HH, 0L);
                long zoneMM = zone.getOrDefault(State.ZID_MM, 0L);
                long zoneSS = zone.getOrDefault(State.ZID_SS, 0L);
                zoneSecondsFromUTC = (int) (zoneSign * (zoneHH * 60 + zoneMM) * 60 + zoneSS);
            }
        }
        return zoneSecondsFromUTC;
    }

    static IllegalArgumentException exceptionForInvalidLiteral(Object literal) {
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "value [%s] is not a valid literal for %s",
            literal, TimeTZType.class.getSimpleName()));
    }

    private static long checkRange(String name, long value, long max) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value [%d] is out of range for '%s' [0, %d]",
                value, name, max));
        }
        return value;
    }

    public static TimeTZ parse(@Nonnull String format) {
        return new TimeTZ(parseJustTime(format), parseJustZone(format));
    }

    public static String formatTime(@Nonnull TimeTZ time) {
        String localTime = LocalTime
            .ofNanoOfDay(time.getMicrosFromMidnight() * 1000L)
            .format(DateTimeFormatter.ISO_TIME);
        int secondsFromUTC = time.getSecondsFromUTC();
        if (secondsFromUTC != 0) {
            char sign = secondsFromUTC >= 0 ? '+' : '-';
            secondsFromUTC = Math.abs(secondsFromUTC);
            int mm = secondsFromUTC / 60;
            int hh = mm / 60;
            mm = mm % 60;
            int ss = secondsFromUTC % 60;
            return mm != 0 ?
                ss != 0 ?
                    String.format(
                        Locale.ENGLISH,"%s%c%02d:%02d:%02d",
                        localTime, sign, hh, mm, ss)
                    :
                    String.format(
                        Locale.ENGLISH,"%s%c%02d:%02d",
                        localTime, sign, hh, mm)
                :
                String.format(
                    Locale.ENGLISH,"%s%c%02d",
                    localTime, sign, hh);
        }
        return localTime;
    }
}
