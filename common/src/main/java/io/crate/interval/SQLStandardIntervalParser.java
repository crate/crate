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

import javax.annotation.Nullable;

import static io.crate.interval.IntervalParser.nullSafeIntGet;
import static io.crate.interval.IntervalParser.roundToPrecision;

final class SQLStandardIntervalParser {

    private enum ParserState {
        NOTHING_PARSED,
        HMS_PARSED,
        DAY_PARSED,
        YEAR_MONTH_PARSED
    }

    static Period apply(String value,
                        @Nullable IntervalParser.Precision start,
                        @Nullable IntervalParser.Precision end) {
        return roundToPrecision(apply(value), start, end);
    }

    static Period apply(String value) {
        String[] values = value.split(" ");

        if (values.length > 3 || values.length == 0) {
            throw new IllegalArgumentException("Invalid interval format " + value);
        }

        ParserState state = ParserState.NOTHING_PARSED;
        boolean negative = false;
        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;

        try {
            String part;
            // Parse from backwards
            for (int i = values.length; i > 0; i--) {
                part = values[i - 1];
                if (part.startsWith("-")) {
                    negative = true;
                    part = part.substring(1);
                }
                if (part.startsWith("+")) {
                    part = part.substring(1);
                }
                if (part.contains(":")) {
                    // H:M:S: Using a single value defines seconds only
                    // Using two values defines hours and minutes.
                    String[] hms = part.split(":");
                    if (hms.length == 3) {
                        hours = nullSafeIntGet(hms[0]);
                        minutes = nullSafeIntGet(hms[1]);
                        seconds = nullSafeIntGet(hms[2]);
                    } else if (hms.length == 2) {
                        hours = nullSafeIntGet(hms[0]);
                        minutes = nullSafeIntGet(hms[1]);
                    } else if (hms.length == 1) {
                        seconds = nullSafeIntGet(hms[0]);
                    }
                    if (negative) {
                        hours = -hours;
                        minutes = -minutes;
                        seconds = -seconds;
                        negative = false;
                    }
                    state = ParserState.HMS_PARSED;
                } else if (part.contains("-")) {
                    if (state == ParserState.YEAR_MONTH_PARSED) {
                        throw new IllegalArgumentException("Invalid interval format " + value);
                    }
                    //YEAR-MONTH
                    String[] ym = part.split("-");
                    if (ym.length == 2) {
                        years = nullSafeIntGet(ym[0]);
                        months = nullSafeIntGet(ym[1]);
                    } else {
                        throw new IllegalArgumentException("Invalid interval format " + value);
                    }
                    if (negative) {
                        years = -years;
                        months = -months;
                        negative = false;
                    }
                    state = ParserState.YEAR_MONTH_PARSED;
                } else {
                    // Try to parse as day or second
                    if (state == ParserState.HMS_PARSED) {
                        days = nullSafeIntGet(part);
                        if (negative) {
                            days = -days;
                            negative = false;
                        }
                        state = ParserState.DAY_PARSED;
                    } else if (state == ParserState.NOTHING_PARSED) {
                        seconds = nullSafeIntGet(part);
                        if (negative) {
                            seconds = -seconds;
                            negative = false;
                        }
                        state = ParserState.HMS_PARSED;
                    }

                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval fornat " + value);
        }
        return new Period(years, months, 0, days, hours, minutes, seconds, 0);
    }
}
