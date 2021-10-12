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
import org.joda.time.Period;
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
}
