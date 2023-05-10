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

import java.io.IOException;
import java.math.BigInteger;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.DateTimeConstants;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import io.crate.Streamer;
import io.crate.interval.IntervalParser;

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
        } else if (value instanceof String strValue) {
            return IntervalParser.apply(strValue);
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
        var period1 = normalize(p1);
        var period2 = normalize(p2);
        var cmp = Integer.compare(period1.getYears(), period2.getYears());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(period1.getMonths(), period2.getMonths());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(period1.getWeeks(), period2.getWeeks());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(period1.getHours(), period2.getHours());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(period1.getMinutes(), period2.getMinutes());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(period1.getSeconds(), period2.getSeconds());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(period1.getMillis(), period2.getMillis());
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
     * Copy of {@link Period#toStandardDuration()}, but also includes 30 days per month and
     * 12 months per year calculation. Uses BigInteger to avoid long overflow in case of large
     * numbers.
     */
    public static BigInteger toStandardDuration(Period p) {
        // no overflow can happen up to WEEKS, even with Integer.MAX_VALUEs,
        // so use a long to avoid BigInteger object allocations.
        long millis = p.getMillis();
        millis += (((long) p.getSeconds()) * ((long) DateTimeConstants.MILLIS_PER_SECOND));
        millis += (((long) p.getMinutes()) * ((long) DateTimeConstants.MILLIS_PER_MINUTE));
        millis += (((long) p.getHours()) * ((long) DateTimeConstants.MILLIS_PER_HOUR));
        millis += (((long) p.getDays()) * ((long) DateTimeConstants.MILLIS_PER_DAY));
        millis += (((long) p.getWeeks()) * ((long) DateTimeConstants.MILLIS_PER_WEEK));

        BigInteger result = BigInteger.valueOf(millis);
        result = result.add(BigInteger.valueOf(p.getMonths() * 30 * (long) DateTimeConstants.MILLIS_PER_DAY));
        result = result.add(BigInteger.valueOf(p.getYears() * 12 * 30 * (long) DateTimeConstants.MILLIS_PER_DAY));
        return result;
    }

    /**
     * It's half-copied from {@link Period#normalizedStandard(PeriodType)} to normalize first the time/days
     * units and then normalize the days/months/years. It's used to only create one {@link Period} result object
     * and make all necessary calculations in one go.
     */
    public static Period normalize(Period p) {
        long millis = p.getMillis();  // no overflow can happen, even with Integer.MAX_VALUEs
        millis += (((long) p.getSeconds()) * ((long) DateTimeConstants.MILLIS_PER_SECOND));
        millis += (((long) p.getMinutes()) * ((long) DateTimeConstants.MILLIS_PER_MINUTE));
        millis += (((long) p.getHours()) * ((long) DateTimeConstants.MILLIS_PER_HOUR));
        millis += (((long) p.getDays()) * ((long) DateTimeConstants.MILLIS_PER_DAY));
        millis += (((long) p.getWeeks()) * ((long) DateTimeConstants.MILLIS_PER_WEEK));
        Period result = new Period(millis, PeriodType.yearMonthDayTime(), ISOChronology.getInstanceUTC());

        // Normalize days/months/years
        int days = result.getDays();
        int years = p.getYears();
        if (days > 365) {
            var oldDays = days;
            days %= 365;
            years += (oldDays / 365);
        }
        int months = p.getMonths();
        if (days > 30) {
            var oldDays = days;
            days %= 30;
            months += oldDays / 30;
        }
        if (months > 12) {
            var oldMonths = months;
            months %= 12;
            years += (oldMonths / 12);
        }
        return result.withDays(days).withMonths(months).withYears(years);
    }
}
