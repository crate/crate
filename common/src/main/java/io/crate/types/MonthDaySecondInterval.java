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

public class MonthDaySecondInterval implements Comparable<MonthDaySecondInterval> {

    private final double seconds;
    private final int days;
    private final int months;

    public MonthDaySecondInterval(double seconds, int days, int months) {
        this.seconds = seconds;
        this.days = days;
        this.months = months;
    }

    public double getSeconds() {
        return seconds;
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
        MonthDaySecondInterval MOnthDaySecondInterval = (MonthDaySecondInterval) o;
        return Double.compare(MOnthDaySecondInterval.seconds, seconds) == 0 &&
               days == MOnthDaySecondInterval.days &&
               months == MOnthDaySecondInterval.months;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seconds, days, months);
    }

    @Override
    public int compareTo(MonthDaySecondInterval other) {
        return compare(this, other);
    }

    public static int compare(MonthDaySecondInterval i1, MonthDaySecondInterval i2) {
        int msCmp = Double.compare(i1.seconds, i2.seconds);
        if (msCmp != 0) return msCmp;

        int daysCmp = Integer.compare(i1.days, i2.days);
        if (daysCmp != 0) return daysCmp;

        return Integer.compare(i1.months, i2.months);
    }
}
