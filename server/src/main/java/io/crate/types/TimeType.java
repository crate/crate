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

public final class TimeType extends DataType<Integer> implements FixedWidthType, Streamer<Integer> {

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
    public Streamer<Integer> streamer() {
        return this;
    }

    @Override
    public int compare(Integer val1, Integer val2) {
        return Integer.compare(val1, val2);
    }

    @Override
    public Integer readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readInt();
    }

    @Override
    public void writeValueTo(StreamOutput out, Integer v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeInt(v);
        }
    }

    @Override
    public int fixedSize() {
        return IntegerType.INTEGER_SIZE;
    }

    @Override
    public Integer value(Object value) throws ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return parseTime((String) value);
        }
        // float values are treated as "seconds.milliseconds"
        if (value instanceof Double) {
            Double n = (Double) value;
            if (n.doubleValue() < Float.MAX_VALUE) {
                return translateFrom(n.floatValue());
            }
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "value too large [%f] if does not fit in a float",
                value));
        }
        if (value instanceof Float) {
            return translateFrom((Float) value);
        }
        return value instanceof Integer ? (Integer) value : ((Number) value).intValue();
    }

    public static int translateFrom(@Nonnull Float number) {
        // number is: seconds.milliseconds
        return (int) (Instant
            .ofEpochMilli((long) Math.floor(number.floatValue() * 1000))
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli() - Instant.EPOCH.toEpochMilli());
    }

    public static int parseTime(@Nonnull String time) {
        try {
            return Integer.parseInt(time);
        } catch (NumberFormatException e) {
            // the time zone is ignored if present
            LocalTime lt = LocalTime.parse(time, TIME_PARSER);
            return (int) LocalDateTime
                .of(ZERO_DATE, lt)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
        }
    }

    public static String formatTime(@Nonnull Integer time) {
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
