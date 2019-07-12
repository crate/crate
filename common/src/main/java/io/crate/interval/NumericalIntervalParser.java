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

import static io.crate.interval.IntervalParser.parseInteger;
import static io.crate.interval.IntervalParser.parseMilliSeconds;


class NumericalIntervalParser {

    private NumericalIntervalParser() { }

    static Period apply(String value) {
        return apply(value, null, null);
    }

    static Period apply(String value, IntervalParser.Precision start, IntervalParser.Precision end) {
        try {
            return roundToPrecision(parseInteger(value), parseMilliSeconds(value), start, end);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval format " + value);
        }
    }

    private static Period roundToPrecision(int value,
                                   int millis,
                                   IntervalParser.Precision start,
                                   IntervalParser.Precision end) {
        if (start == null && end == null) {
            return roundToSecondsWithMillis(value, millis);
        }
        if (start == IntervalParser.Precision.YEAR && end == null) {
            return Period.years(value);
        }
        if (start == IntervalParser.Precision.YEAR && end == IntervalParser.Precision.MONTH) {
            return Period.months(value);
        }
        if (start == IntervalParser.Precision.MONTH && end == null) {
            return Period.months(value);
        }
        if (start == IntervalParser.Precision.DAY && end == null) {
            return Period.days(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.HOUR) {
            return Period.hours(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.MINUTE) {
            return Period.minutes(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.SECOND) {
            return roundToSecondsWithMillis(value, millis);
        }
        if (start == IntervalParser.Precision.HOUR && end == null) {
            return Period.hours(value);
        }
        if (start == IntervalParser.Precision.HOUR && end == IntervalParser.Precision.MINUTE) {
            return Period.minutes(value);
        }
        if (start == IntervalParser.Precision.HOUR && end == IntervalParser.Precision.SECOND) {
            return roundToSecondsWithMillis(value, millis);
        }
        if (start == IntervalParser.Precision.MINUTE && end == null) {
            return Period.minutes(value);
        }
        if (start == IntervalParser.Precision.MINUTE && end == IntervalParser.Precision.SECOND) {
            return Period.seconds(value);
        }
        if (start == IntervalParser.Precision.SECOND && end == null) {
            return roundToSecondsWithMillis(value, millis);
        }
        throw new IllegalArgumentException(String.format("Invalid start %s and end %s combination", start, end));
    }

    private static Period roundToSecondsWithMillis(int seconds, int millis) {
        Period period = Period.seconds(seconds);
        if (millis != 0) {
            period = period.withMillis(millis);
        }
        return period;
    }
}
