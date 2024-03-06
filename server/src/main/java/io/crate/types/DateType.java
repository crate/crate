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

import static io.crate.types.TimestampType.TIMESTAMP_PARSER;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.statistics.ColumnSketch;
import io.crate.statistics.ColumnSketchBuilder;
import io.crate.statistics.ColumnStatsSupport;

public class DateType extends DataType<Long>
    implements FixedWidthType, Streamer<Long> {

    public static final int ID = 24;
    public static final DateType INSTANCE = new DateType();
    public static final int TYPE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);

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
        long longVal;
        if (value == null) {
            return null;
        } else if (value instanceof String stringVal) {
            try {
                TemporalAccessor dt = TIMESTAMP_PARSER.parseBest(stringVal, LocalDateTime::from, LocalDate::from);
                LocalDate localDate = LocalDate.from(dt);
                return localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (DateTimeParseException ex) {
                try {
                    longVal = Long.parseLong(stringVal);
                } catch (NumberFormatException e) {
                    throw new ClassCastException("Can't cast '" + value + "' to " + getName());
                }
            }
        } else if (value instanceof Double d) {
            // we treat float and double values as seconds with milliseconds as fractions
            // see timestamp documentation
            longVal = ((Number) (d * 1000)).longValue();
        } else if (value instanceof Float f) {
            longVal = ((Number) (f * 1000)).longValue();
        } else if (value instanceof Number number) {
            longVal = number.longValue();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }

        var epochDay = longVal / 1000 / 86400;
        var localDate = LocalDate.ofEpochDay(epochDay);
        return localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    @Override
    public Long sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number number) {
            return number.longValue();
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
        return TYPE_SIZE;
    }

    @Override
    public long valueBytes(Long value) {
        return TYPE_SIZE;
    }

    @Override
    public ColumnStatsSupport<Long> columnStatsSupport() {
        return new ColumnStatsSupport<>() {
            @Override
            public ColumnSketchBuilder<Long> sketchBuilder() {
                return new ColumnSketchBuilder<>(Long.class, DateType.this);
            }

            @Override
            public ColumnSketch<Long> readSketchFrom(StreamInput in) throws IOException {
                return new ColumnSketch<>(Long.class, DateType.this, in);
            }
        };
    }
}
