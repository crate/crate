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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class DateType extends DataType<Long>
    implements FixedWidthType, Streamer<Long> {

    public static final int ID = 24;
    public static final DateType INSTANCE = new DateType();
    public static final int DATE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);

    private static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.DATE;
    }

    @Override
    public String getName() {
        return "date";
    }

    @Override
    public Streamer<Long> streamer() {
        return this;
    }

    @Override
    public Long implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {

            var stringVal = (String) value;

            try {
                LocalDate dt = LocalDate.parse(stringVal, ISO_FORMATTER);
                return dt.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (DateTimeParseException ex) {
                try {
                    return Long.parseLong(stringVal);
                } catch (NumberFormatException e) {
                    throw new ClassCastException("Can't cast '" + value + "' to " + getName());
                }
            }

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
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            return (Long) value;
        }
    }

    @Override
    public int compare(Long o1, Long o2) {
        return o1.compareTo(o2);
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
        return DATE_SIZE;
    }
}
