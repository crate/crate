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

import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.crate.interval.IntervalParser.parseMilliSeconds;


final class NumericalIntervalParser {

    private NumericalIntervalParser() {
    }

    static Period apply(String value,
                        @Nullable IntervalParser.Precision start,
                        @Nullable IntervalParser.Precision end) {
        try {
            return roundToPrecision(parseInteger(value), parseMilliSeconds(value), start, end);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval format: " + value);
        }
    }

    private static Period roundToPrecision(int value,
                                           int millis,
                                           @Nullable IntervalParser.Precision start,
                                           @Nullable IntervalParser.Precision end) {
        if (start == null && end == null) {
            return buildSecondsWithMillisPeriod(value, millis);
        }
        if (start == IntervalParser.Precision.YEAR) {
            if (end == null) {
                return Period.years(value);
            }
            if (end == IntervalParser.Precision.MONTH) {
                return Period.months(value);
            }
        }
        if (start == IntervalParser.Precision.MONTH && end == null) {
            return Period.months(value);
        }
        if (start == IntervalParser.Precision.DAY) {
            if (end == null) {
                return Period.days(value);
            }
            if (end == IntervalParser.Precision.HOUR) {
                return Period.hours(value);
            }
            if (end == IntervalParser.Precision.MINUTE) {
                return Period.minutes(value);
            }
            if (end == IntervalParser.Precision.SECOND) {
                return buildSecondsWithMillisPeriod(value, millis);
            }
        }
        if (start == IntervalParser.Precision.HOUR) {
            if (end == null) {
                return Period.hours(value);
            }
            if (end == IntervalParser.Precision.MINUTE) {
                return Period.minutes(value);
            }
            if (end == IntervalParser.Precision.SECOND) {
                return buildSecondsWithMillisPeriod(value, millis);
            }
        }
        if (start == IntervalParser.Precision.MINUTE) {
            if (end == null) {
                return Period.minutes(value);
            }
            if (end == IntervalParser.Precision.SECOND) {
                return Period.seconds(value);
            }
        }
        if (start == IntervalParser.Precision.SECOND && end == null) {
            return buildSecondsWithMillisPeriod(value, millis);
        }
        throw new IllegalArgumentException("Invalid start and end combination");
    }

    private static int parseInteger(String value) {
        BigInteger result = new BigDecimal(value).toBigInteger();
        if (result.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0 ||
            result.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0) {
            throw new ArithmeticException("Interval field value out of range " + value);
        }
        return result.intValue();
    }

    private static Period buildSecondsWithMillisPeriod(int seconds, int millis) {
        Period period = Period.seconds(seconds);
        if (millis != 0) {
            period = period.withMillis(millis);
        }
        return period;
    }
}
