/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public final class TimestampType extends DataType<Long>
    implements FixedWidthType, Streamer<Long> {

    public static final int ID_WITH_TZ = 11;
    public static final int ID_WITHOUT_TZ = 15;

    public static final TimestampType INSTANCE_WITH_TZ = new TimestampType(
        ID_WITH_TZ,
        "timestamp with time zone",
        TimestampType::parseTimestamp,
        Precedence.TIMESTAMP_WITH_TIME_ZONE);

    public static final TimestampType INSTANCE_WITHOUT_TZ = new TimestampType(
        ID_WITHOUT_TZ,
        "timestamp without time zone",
        TimestampType::parseTimestampIgnoreTimeZone,
        Precedence.TIMESTAMP);

    private static final StorageSupport<Long> STORAGE = new StorageSupport<>(true, true, new LongEqQuery());

    private final int id;
    private final String name;
    private final Function<String, Long> parse;
    private final Precedence precedence;

    private TimestampType(int id, String name, Function<String, Long> parse, Precedence precedence) {
        this.id = id;
        this.name = name;
        this.parse = parse;
        this.precedence = precedence;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Precedence precedence() {
        return precedence;
    }

    @Override
    public Streamer<Long> streamer() {
        return this;
    }

    @Override
    public Long implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return parse.apply((String) value);
        } else if (value instanceof Double) {
            // we treat float and double values as seconds with milliseconds as fractions
            // see timestamp documentation
            return ((Number) (((Double) value) * 1000)).longValue();
        } else if (value instanceof Float) {
            return ((Number) (((Float) value) * 1000)).longValue();
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Long sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        } else {
            return ((Number) value).longValue();
        }
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
    public void writeValueTo(StreamOutput out, Long v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeLong(v);
        }
    }

    @Override
    public int fixedSize() {
        return LongType.LONG_SIZE;
    }

    static long parseTimestamp(String timestamp) {
        try {
            return Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            TemporalAccessor dt;
            try {
                dt = TIMESTAMP_PARSER.parseBest(
                    timestamp, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
            } catch (DateTimeParseException e1) {
                throw new IllegalArgumentException(e1.getMessage());
            }

            if (dt instanceof LocalDateTime) {
                LocalDateTime localDateTime = LocalDateTime.from(dt);
                return localDateTime.toInstant(UTC).toEpochMilli();
            } else if (dt instanceof LocalDate) {
                LocalDate localDate = LocalDate.from(dt);
                return localDate.atStartOfDay(UTC).toInstant().toEpochMilli();
            }

            OffsetDateTime offsetDateTime = OffsetDateTime.from(dt);
            return offsetDateTime.toInstant().toEpochMilli();
        }
    }

    public static long parseTimestampIgnoreTimeZone(String timestamp) {
        try {
            return Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            TemporalAccessor dt;
            try {
                dt = TIMESTAMP_PARSER.parseBest(
                    timestamp, LocalDateTime::from, LocalDate::from);
            } catch (DateTimeParseException e1) {
                throw new IllegalArgumentException(e1.getMessage());
            }

            if (dt instanceof LocalDate) {
                LocalDate localDate = LocalDate.from(dt);
                return localDate.atStartOfDay(UTC).toInstant().toEpochMilli();
            }

            LocalDateTime localDateTime = LocalDateTime.from(dt);
            return localDateTime.toInstant(UTC).toEpochMilli();
        }
    }

    private static final DateTimeFormatter TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .optionalStart()
            .padNext(1)
                .optionalStart()
                    .appendLiteral('T')
                .optionalEnd()
            .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendPattern("[Z][VV][x][xx][xxx]")
        .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);


    @Override
    public StorageSupport<Long> storageSupport() {
        return STORAGE;
    }
}
