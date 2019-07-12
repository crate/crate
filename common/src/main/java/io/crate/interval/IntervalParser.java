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

package io.crate.interval;

import org.joda.time.Period;

import java.math.BigDecimal;

public class IntervalParser {

    public enum Precision {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    private IntervalParser() { }

    public static Period apply(String value) {
        return apply(value, null, null);
    }

    public static Period apply(String value, Precision start, Precision end) {
        Period result;
        try {
            result = NumericalIntervalParser.apply(value, start, end);
        } catch (IllegalArgumentException e1) {
            try {
                result = IsoIntervalParser.apply(value, start, end);
            } catch (IllegalArgumentException e2) {
                try {
                    result = SQLStandardIntervalParser.apply(value, start, end);
                } catch (IllegalArgumentException e3) {
                    result = PGIntervalParser.apply(value, start, end);
                }
            }
        }
        return result.normalizedStandard();
    }

    static Period roundToPrecision(Period period, Precision start, Precision end) {
        if (start == null && end == null) {
            return period;
        }
        if (start == Precision.YEAR && end == null) {
            return Period.years(period.getYears());
        }
        if (start == Precision.YEAR && end == Precision.MONTH) {
            return Period.years(period.getYears()).withMonths(period.getMonths());
        }
        if (start == Precision.MONTH && end == null) {
            return Period.years(period.getYears()).withMonths(period.getMonths());
        }
        if (start == Precision.DAY && end == null) {
            return new Period(period).withHours(0).withMinutes(0).withSeconds(0).withMillis(0);
        }
        if (start == Precision.DAY && end == Precision.HOUR) {
            return new Period(period).withMinutes(0).withSeconds(0).withMillis(0);
        }
        if (start == Precision.DAY && end == Precision.MINUTE) {
            return new Period(period).withSeconds(0).withMillis(0);
        }
        if (start == Precision.DAY && end == Precision.SECOND) {
            return new Period(period).withMillis(0);
        }
        if (start == Precision.HOUR && end == null) {
            return new Period(period).withMinutes(0).withSeconds(0).withMillis(0);
        }
        if (start == Precision.HOUR && end == Precision.MINUTE) {
            return new Period(period).withSeconds(0).withMillis(0);
        }
        if (start == Precision.HOUR && end == Precision.SECOND) {
            return new Period(period).withMillis(0);
        }
        if (start == Precision.MINUTE && end == null) {
            return new Period(period).withSeconds(0).withMillis(0);
        }
        if (start == Precision.MINUTE && end == Precision.SECOND) {
            return new Period(period).withMillis(0);
        }
        if (start == Precision.SECOND && end == null) {
            return new Period(period).withMillis(0);
        }
        throw new IllegalArgumentException(String.format("Invalid start and end combination", start, end));
    }

    static int parseMilliSeconds(String value) throws NumberFormatException {
        BigDecimal decimal = new BigDecimal(value);
        return decimal
            .subtract(new BigDecimal(decimal.intValue()))
            .multiply(new BigDecimal(1000)).intValue();
    }

    static int parseInteger(String value) {
        return new BigDecimal(value).intValue();
    }

    static int nullSafeIntGet(String value) {
        return (value == null) ? 0 : Integer.parseInt(value);
    }

}
