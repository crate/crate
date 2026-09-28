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
import io.crate.statistics.ColumnStatsSupport;

public class DateType extends DataType<Long>
    implements FixedWidthType, Streamer<Long> {

    public static final int ID = 24;
    public static final String NAME = "date";
    public static final DateType INSTANCE = new DateType();
    public static final int TYPE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);
    public static final int DAY_TO_MS = 86400000;

    // Date values are streamed as timestamp (in ms since epoch) to clients via HTTP
    // So our max/min values are smaller than LocalDate.MAX/MIN
    public static final LocalDate MAX_NULL_SENTINEL = ofTimestamp(Long.MAX_VALUE);
    public static final LocalDate MIN_NULL_SENTINEL = ofTimestamp(Long.MIN_VALUE);
    public static final LocalDate MAX = MAX_NULL_SENTINEL.minusDays(1);
    public static final LocalDate MIN = MIN_NULL_SENTINEL.plusDays(1);

    public static LocalDate ofTimestamp(long msValue) {
        return msValue >= 0
            ? LocalDate.ofEpochDay(msValue / DAY_TO_MS)
            : LocalDate.ofEpochDay(Math.floorDiv(msValue, DAY_TO_MS));
    }

    public static long toTimestamp(LocalDate date) {
        return date.toEpochDay() * DAY_TO_MS;
    }

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
        return NAME;
    }

    @Override
    public TypeSignature getTypeSignature() {
        return TypeSignature.DATE;
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

        LocalDate localDate = ofTimestamp(longVal);
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
        return ColumnStatsSupport.singleValued(Long.class, DateType.this);
    }
}
