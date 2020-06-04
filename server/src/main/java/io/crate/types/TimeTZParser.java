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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Locale;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

/**
 * Represents time with time zone as microseconds from midnight,
 * and zone as signed seconds from UTC.
 * <p>
 * There are 1000_000 microseconds in one second:
 * <pre>
 *     (24 * 3600 + 59 * 60 + 59) * 1000_000L > Integer.MAX_VALUE
 * </pre>
 * <p>
 * Thus the range for time values is 0 .. 86400000000 (max number
 * of micros in a day).
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

    private static final long MAX_MICROS = 24 * 60 * 60 * 1000_000L - 1;

    public static TimeTZ timeTZOf(String source, long value) {
        return new TimeTZ(checkRange(source, value, MAX_MICROS), 0);
    }

    public static TimeTZ parse(@Nonnull String format) {
        TemporalAccessor dt;
        try {
            dt = TIMETZ_PARSER.parse(format.replaceAll("\\s+", ""));
            int hh = access(dt, HOUR_OF_DAY);
            int mm = access(dt, MINUTE_OF_HOUR);
            int ss = access(dt, SECOND_OF_MINUTE);
            int micros = access(dt, NANO_OF_SECOND) / 1000;
            long microsFromMidnight = checkRange(
                format,
                (((hh * 60 + mm) * 60) + ss) * 1000_000L + micros,
                MAX_MICROS);
            int zoneSecondsFromUTC = 0;
            ZoneOffset zoneOffset = dt.query(TemporalQueries.offset());
            if (zoneOffset != null) {
                zoneSecondsFromUTC = zoneOffset.getTotalSeconds();
            } else {
                ZoneId zoneId = dt.query(TemporalQueries.zone());
                if (zoneId != null) {
                    zoneOffset = zoneId.getRules().getOffset(Instant.now());
                    zoneSecondsFromUTC = zoneOffset.getTotalSeconds();
                }
            }
            return new TimeTZ(microsFromMidnight, zoneSecondsFromUTC);
        } catch (DateTimeException e0 ) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,"%s", e0.getMessage()));
        }
    }

    private static int access(TemporalAccessor time, ChronoField field) {
        return time.isSupported(field) ? time.get(field) : 0;
    }

    public static String formatTime(@Nonnull TimeTZ time) {
        String localTime = LocalTime
            .ofNanoOfDay(time.getMicrosFromMidnight() * 1000L)
            .format(DateTimeFormatter.ISO_TIME);
        int secondsFromUTC = time.getSecondsFromUTC();
        if (secondsFromUTC != 0) {
            return  String.format(
                    Locale.ENGLISH,"%s%s",
                    localTime,
                    ZoneOffset.ofTotalSeconds(time.getSecondsFromUTC()));
        }
        return localTime;
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

    private static final DateTimeFormatter TIMETZ_PARSER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 2)
        .optionalStart()
        .optionalStart().appendLiteral(':').optionalEnd()
        .appendValue(MINUTE_OF_HOUR, 2)
        .optionalStart()
        .optionalStart().appendLiteral(':').optionalEnd()
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalEnd()
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendPattern("[Z][VV][x][xx][xxx]")
        .optionalEnd()
        .toFormatter(Locale.ENGLISH)
        .withResolverStyle(ResolverStyle.STRICT);
}
