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

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Period;

import java.util.Objects;
import java.util.Optional;


public class Interval implements Comparable<Interval> {

    private Period period;
    private Interval.Format format;

    public enum Precision {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    public enum Format {
        UNDEFINED,
        NUMERICAL,
        PSQL,
        SQL_STANDARD,
        IS0_8601
    }

    public Interval(Period period) {
        this.format = Format.UNDEFINED;
        this.period = period;
    }

    public Interval(Period period, Interval.Format format) {
        this.format = format;
        this.period = period;
    }

    public Period getPeriod() {
        return period;
    }

    public Interval withPeriod(Period period) {
        return new Interval(period, format);
    }

    @Override
    public int compareTo(Interval o) {
        return 0;
    }

    public static int compare(Interval i1, Interval i2) {
        int formatCmp = i1.compareTo(i2);
        if (formatCmp != 0) {
            return formatCmp;
        }

        //TODO format + precision

        Duration duration1 = i1.period.toDurationFrom(Instant.EPOCH);
        Duration duration2 = i2.period.toDurationFrom(Instant.EPOCH);
        return duration1.compareTo(duration2);
    }

    @Override
    public String toString() {
        return "Interval{" +
               "period=" + period +
               ", format=" + format +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Interval interval = (Interval) o;
        return Objects.equals(period, interval.period);

    }

    @Override
    public int hashCode() {
        return Objects.hash(period, format);
    }
}
