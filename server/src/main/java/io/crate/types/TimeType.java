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
import java.time.format.ResolverStyle;
import java.util.Locale;

/**
 * All literal formats are interpreted as UTC, ignoring time zone if present.
 * Internally stored as a long (milli seconds from epoch, ignoring date).
 * Precision is milli seconds (10e3 in a second, unlike postgres which is
 * micro seconds 10e6) see TimestampType.
 */
public final class TimeType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final int ID = 19;
    public static final String NAME = "time without time zone";
    public static final TimeType INSTANCE = new TimeType();


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
        if (value instanceof Double || value instanceof Float) {
            // we treat float and double values as seconds with milliseconds as fractions
            // 123.456789 -> 123456
            return (long) Math.floor(((Number) value).doubleValue() * 1000);
        }
        if (value instanceof Long || value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return parseTime((String) value);
        }
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "unexpected value [%s], does not fit TimeType's literal syntax",
            value));
    }

    public static long parseTime(@Nonnull String time) {
        try {
            return Long.parseLong(time);
        } catch (NumberFormatException eLong) {
            try {
                return (long) Math.floor(Double.parseDouble(time) * 1000);
            } catch (NumberFormatException eDouble) {
                // the time zone is ignored if present
                LocalTime lt = LocalTime.parse(time, TIME_PARSER);
                return LocalDateTime
                    .of(ZERO_DATE, lt)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
            }
        }
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
