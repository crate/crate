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
import io.crate.interval.IntervalParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.IOException;

public class IntervalType extends DataType<Period> implements FixedWidthType, Streamer<Period> {

    public static final int ID = 17;
    public static final IntervalType INSTANCE = new IntervalType();
    public static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder()
        .appendYears()
        .appendSuffix(" year", " years")
        .appendSeparator(" ")
        .appendMonths()
        .appendSuffix(" mon", " mons")
        .appendSeparator(" ")
        .appendWeeks()
        .appendSuffix(" weeks")
        .appendSeparator(" ")
        .appendDays()
        .printZeroAlways()
        .minimumPrintedDigits(2)
        .appendSuffix(" day", " days")
        .appendSeparator(" ")
        .appendHours()
        .minimumPrintedDigits(2)
        .printZeroAlways()
        .appendSeparator(":")
        .appendMinutes()
        .minimumPrintedDigits(2)
        .printZeroAlways()
        .appendSeparator(":")
        .appendSecondsWithOptionalMillis()
        .toFormatter();

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.INTERVAL;
    }

    @Override
    public String getName() {
        return "interval";
    }

    @Override
    public Streamer<Period> streamer() {
        return this;
    }

    @Override
    public Period implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return IntervalParser.apply((String) value);
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Period sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else {
            return (Period) value;
        }
    }

    @Override
    public int compare(Period p1, Period p2) {
        return p1.toStandardDuration().compareTo(p2.toStandardDuration());

    }

    @Override
    public Period readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new Period(
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt()
            );
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Period p) throws IOException {
        if (p == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(p.getYears());
            out.writeVInt(p.getMonths());
            out.writeVInt(p.getWeeks());
            out.writeVInt(p.getDays());
            out.writeVInt(p.getHours());
            out.writeVInt(p.getMinutes());
            out.writeVInt(p.getSeconds());
            out.writeVInt(p.getMillis());
        }
    }

    @Override
    public int fixedSize() {
        return 32;
    }

    @Override
    public long valueBytes(Period value) {
        return 32;
    }

    /**
     * returns Period in yearMonthDayTime which corresponds to Postgres default Interval output format.
     * See https://www.postgresql.org/docs/14/datatype-datetime.html#DATATYPE-INTERVAL-OUTPUT
     */
    public static Period subtractTimestamps(long timestamp1, long timestamp2) {
        /*
         PeriodType is important as it affects the internal representation of the Period object.
         PeriodType.yearMonthDayTime() is needed to return 8 days but not 1 week 1 day.
         Streamer of the IntervalType will simply put 0 in 'out.writeVInt(p.getWeeks())' as getWeeks() returns zero for unused fields.
         */

        if (timestamp1 < timestamp2) {
            /*
            In Postgres second argument is subtracted from the first.
            Interval's first argument must be smaller than second and thus we swap params and negate.

            We need to pass UTC timezone to be sure that Interval doesn't end up using system default time zone.
            Currently, timestamps are in UTC (see https://github.com/crate/crate/issues/10037 and
            https://github.com/crate/crate/issues/12064) but if https://github.com/crate/crate/issues/7196 ever gets
            implemented, we need to pass here not UTC but time zone set by SET TIMEZONE.
            */
            return new Interval(timestamp1, timestamp2, DateTimeZone.UTC).toPeriod(PeriodType.yearMonthDayTime()).negated();
        } else {
            return new Interval(timestamp2, timestamp1, DateTimeZone.UTC).toPeriod(PeriodType.yearMonthDayTime());
        }
    }
}
