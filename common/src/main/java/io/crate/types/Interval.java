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

import java.util.Objects;

public class Interval implements Comparable<Interval> {

    private final long ms;
    private final int days;
    private final int months;

    public Interval(long ms, int days, int months) {
        this.ms = ms;
        this.days = days;
        this.months = months;
    }

    public long getMs() {
        return ms;
    }

    public int getDays() {
        return days;
    }

    public int getMonths() {
        return months;
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
        return ms == interval.ms &&
               days == interval.days &&
               months == interval.months;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ms, days, months);
    }

    @Override
    public int compareTo(Interval other) {
        return compare(this, other);
    }

    public static int compare(Interval i1, Interval i2) {
        int msCmp = Long.compare(i1.ms, i2.ms);
        if (msCmp != 0) return msCmp;

        int daysCmp = Integer.compare(i1.days, i2.days);
        if (daysCmp != 0) return daysCmp;

        return Integer.compare(i1.months, i2.months);
    }
}
