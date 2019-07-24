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

class NumericalIntervalParser {

    private NumericalIntervalParser() { }

    static Period apply(String value) {
        return roundToPrecision(parseInteger(value), null, null);
    }

    public static Period apply(String value, IntervalParser.Precision start, IntervalParser.Precision end) {
        return roundToPrecision(parseInteger(value), start, end);
    }

    static int parseInteger(String value) {
        try {
            return new BigDecimal(value).intValue();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

    static Period roundToPrecision(int value, IntervalParser.Precision start, IntervalParser.Precision end) {
        if (start == null && end == null) {
            return new Period().withSeconds(value);
        }
        if (start == IntervalParser.Precision.YEAR && end == null) {
            return new Period().withYears(value);
        }
        if (start == IntervalParser.Precision.YEAR && end == IntervalParser.Precision.MONTH) {
            return new Period().withMonths(value);
        }
        if (start == IntervalParser.Precision.MONTH && end == null) {
            return new Period().withMonths(value);
        }
        if (start == IntervalParser.Precision.DAY && end == null) {
            return new Period().withDays(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.HOUR) {
            return new Period().withHours(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.MINUTE) {
            return new Period().withMinutes(value);
        }
        if (start == IntervalParser.Precision.DAY && end == IntervalParser.Precision.SECOND) {
            return new Period().withSeconds(value);
        }
        if (start == IntervalParser.Precision.HOUR && end == null) {
            return new Period().withHours(value);
        }
        if (start == IntervalParser.Precision.HOUR && end == IntervalParser.Precision.MINUTE) {
            return new Period().withMinutes(value);
        }
        if (start == IntervalParser.Precision.HOUR && end == IntervalParser.Precision.SECOND) {
            return new Period().withSeconds(value);
        }
        if (start == IntervalParser.Precision.MINUTE && end == null) {
            return new Period().withMinutes(value);
        }
        if (start == IntervalParser.Precision.MINUTE && end == IntervalParser.Precision.SECOND) {
            return new Period().withSeconds(value);
        }
        if (start == IntervalParser.Precision.SECOND && end == null) {
            return new Period().withSeconds(value);
        }
        throw new IllegalArgumentException(String.format("Invalid start %s and end %s combination", start, end));
    }
}
