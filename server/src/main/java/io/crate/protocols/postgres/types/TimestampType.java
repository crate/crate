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

package io.crate.protocols.postgres.types;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;

import org.jetbrains.annotations.NotNull;

import io.crate.types.Regproc;

final class TimestampType extends BaseTimestampType {

    public static final TimestampType INSTANCE = new TimestampType();

    private static final int OID = 1114;
    private static final String NAME = "timestamp";

    private static final DateTimeFormatter PARSER_WITH_OPTIONAL_ERA = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .optionalStart()
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .optionalStart()
                .appendPattern("[VV][x][xx][xxx]")
            .optionalStart()
                .appendLiteral(' ')
                .appendPattern("G")
        .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern("yyyy-MM-dd HH:mm:ss.SSS+00")
        .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ISO_FORMATTER_WITH_ERA = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_FORMATTER)
        .appendLiteral(' ')
        .appendPattern("G")
        .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    private TimestampType() {
        super(OID, TYPE_LEN, TYPE_MOD, NAME);
    }

    @Override
    public int typArray() {
        return PGArray.TIMESTAMP_ARRAY.oid();
    }

    @Override
    public Regproc typSend() {
        return Regproc.of(NAME + "_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of(NAME + "_recv");
    }

    @Override
    byte[] encodeAsUTF8Text(@NotNull Long value) {
        long millis = (long) value;
        LocalDateTime ts =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        if (millis >= FIRST_MSEC_AFTER_CHRIST) {
            return ts.format(ISO_FORMATTER).getBytes(StandardCharsets.UTF_8);
        } else {
            return ts.format(ISO_FORMATTER_WITH_ERA).getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    Long decodeUTF8Text(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);

        try {
            LocalDateTime dt = LocalDateTime.parse(s, PARSER_WITH_OPTIONAL_ERA);
            return dt.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return io.crate.types.TimestampType.parseTimestampIgnoreTimeZone(s);
        }
    }
}
