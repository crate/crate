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

package io.crate.interval;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.ISOPeriodFormat;

import org.jetbrains.annotations.Nullable;
import java.math.BigDecimal;

public final class IntervalParser {

    public enum Precision {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    private IntervalParser() {
    }

    /**
     * Parses a period from the given text, returning a new Period.
     * The following text formats are supported:
     * <p>
     * SQL Standard 'Y-M D H:M:S'
     * ISO 8601 'P1Y2M3DT4H5M6S'
     * PostgreSQL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'
     * Abbreviated PostgreSQL '1 yr 2 mons 3 d 4 hrs 5 mins 6 secs'
     *
     * @param value  text to parse
     * @return parsed value in a Period object
     * @throws IllegalArgumentException if the text does not fulfill any of the format
     */
    public static Period apply(String value) {
        return apply(value, null, null);
    }

    /**
     * Parses a period from the given text, returning a new Period.
     * The following text formats are supported:
     * <p>
     * SQL Standard 'Y-M D H:M:S'
     * ISO 8601 'P1Y2M3DT4H5M6S'
     * PostgreSQL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'
     * Abbreviated PostgreSQL '1 yr 2 mons 3 d 4 hrs 5 mins 6 secs'
     *
     * @param value  text to parse
     * @param start  start of the precision
     * @param end    end of the precision
     * @return parsed value in a Period object
     * @throws IllegalArgumentException if the text does not fulfill any of the format
     */
    public static Period apply(String value, @Nullable Precision start, @Nullable Precision end) {
        if (value == null || value.isEmpty() || value.isBlank()) {
            throw new IllegalArgumentException("Invalid interval format:  " + value);
        }

        Period result;
        try {
            result = NumericalIntervalParser.apply(value, start, end);
        } catch (IllegalArgumentException e1) {
            try {
                result = roundToPrecision(ISOPeriodFormat.standard().parsePeriod(value), start, end);
            } catch (IllegalArgumentException e2) {
                try {
                    result = SQLStandardIntervalParser.apply(value, start, end);
                } catch (IllegalArgumentException e3) {
                    result = PGIntervalParser.apply(value, start, end);
                }
            }
        }
        return result.normalizedStandard(PeriodType.yearMonthDayTime());
    }

    static Period roundToPrecision(Period period, Precision start, Precision end) {
        if (start == null && end == null) {
            return period;
        }
        if (start == Precision.YEAR) {
            if (end == null) {
                return Period.years(period.getYears());
            }
            if (end == Precision.MONTH) {
                return Period.years(period.getYears()).withMonths(period.getMonths());
            }
        }
        if (start == Precision.MONTH && end == null) {
            return Period.years(period.getYears()).withMonths(period.getMonths());
        }
        if (start == Precision.DAY) {
            if (end == null) {
                return period.withHours(0).withMinutes(0).withSeconds(0).withMillis(0);
            }
            if (end == Precision.HOUR) {
                return period.withMinutes(0).withSeconds(0).withMillis(0);
            }
            if (end == Precision.MINUTE) {
                return period.withSeconds(0).withMillis(0);
            }
            if (end == Precision.SECOND) {
                return period.withMillis(0);
            }
        }
        if (start == Precision.HOUR) {
            if (end == null) {
                return period.withMinutes(0).withSeconds(0).withMillis(0);
            }
            if (end == Precision.MINUTE) {
                return period.withSeconds(0).withMillis(0);
            }
            if (end == Precision.SECOND) {
                return period.withMillis(0);
            }
        }
        if (start == Precision.MINUTE) {
            if (end == null) {
                return period.withSeconds(0).withMillis(0);
            }
            if (end == Precision.SECOND) {
                return period.withMillis(0);
            }
        }
        if (start == Precision.SECOND && end == null) {
            return period.withMillis(0);
        }
        throw new IllegalArgumentException("Invalid start and end combination");
    }

    static int parseMilliSeconds(String value) throws NumberFormatException {
        BigDecimal decimal = new BigDecimal(value);
        return decimal
            .subtract(new BigDecimal(decimal.intValue()))
            .multiply(new BigDecimal(1000)).intValue();
    }

    static int nullSafeIntGet(String value) {
        return (value == null) ? null : Integer.parseInt(value);
    }

}
