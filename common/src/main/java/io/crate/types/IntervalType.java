/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.Period;

import java.io.IOException;
import java.util.Locale;

public class IntervalType extends DataType<Interval> implements FixedWidthType, Streamer<Interval> {

    public static final int ID = 17;
    public static final IntervalType INSTANCE = new IntervalType();

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.IntervalType;
    }

    @Override
    public String getName() {
        return "interval";
    }

    @Override
    public Streamer<Interval> streamer() {
        return this;
    }

    @Override
    public Interval value(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }

        if(value instanceof Interval) {
            return (Interval) value;
        }

        if (value instanceof Period) {
            return new Interval((Period) value);
        }

        if(value instanceof String) {
            return IntervalParser.apply((String) value);
        }

        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot convert %s to interval", value));
    }

    public int compareValueTo(Interval i1, Interval i2) {
        return nullSafeCompareValueTo(i1, i2, Interval::compareTo);
    }

    @Override
    public Interval readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            long seconds = in.readLong();
            int days = in.readInt();
            int months = in.readInt();
            Period period = new Period().withSeconds(Math.toIntExact(seconds)).withDays(days).withMonths(months);
            return new Interval(period);
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Interval i) throws IOException {
        if (i == null) {
            out.writeBoolean(false);
        } else {
            out.writeDouble(i.getPeriod().getSeconds());
            out.writeInt(i.getPeriod().getDays());
            out.writeInt(i.getPeriod().getMonths());
        }
    }

    @Override
    public int fixedSize() {
        return 24; //TODO estimate size of interval
    }

}
