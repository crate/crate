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

import java.util.regex.Pattern;

import static io.crate.interval.IntervalParser.nullSafeIntGet;
import static io.crate.interval.IntervalParser.roundToPrecision;

final class SQLStandardIntervalParser {

    private enum ParserState {
        NOTHING_PARSED,
        HMS_PARSED,
        SECOND_PARSED,
        DAYS_PARSED,
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
            throw new IllegalArgumentException("Invalid interval format: " + value);
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
            for (int i = values.length - 1; i >= 0; i--) {
                part = values[i];
                if (part.isBlank()) {
                    continue;
                }
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
                    } else {
                        throw new IllegalArgumentException("Invalid interval format: " + value);
                    }
                    if (negative) {
                        hours = -hours;
                        minutes = -minutes;
                        seconds = -seconds;
                        negative = false;
                    }
                    state = ParserState.HMS_PARSED;
                } else if (part.contains("-")) {
                    //YEAR-MONTH
                    String[] ym = part.split("-");
                    if (ym.length == 2) {
                        years = nullSafeIntGet(ym[0]);
                        months = nullSafeIntGet(ym[1]);
                    } else {
                        throw new IllegalArgumentException("Invalid interval format: " + value);
                    }
                    if (negative) {
                        years = -years;
                        months = -months;
                        negative = false;
                    }
                    state = ParserState.YEAR_MONTH_PARSED;
                } else if (state == ParserState.NOTHING_PARSED) {
                    // Trying to parse days or second by looking ahead
                    // and trying to guess what the next value is
                    int number = nullSafeIntGet(part);
                    if (i - 1 >= 0) {
                        String next = values[i - 1];
                        if (YEAR_MONTH_PATTERN.matcher(next).matches()) {
                            days = number;
                            if (negative) {
                                days = -days;
                                negative = false;
                            }
                            state = ParserState.DAYS_PARSED;
                        } else {
                            //Invalid day/second only combination
                            throw new IllegalArgumentException("Invalid interval format: " + value);
                        }
                    } else {
                        seconds = number;
                        if (negative) {
                            seconds = -seconds;
                            negative = false;
                        }
                        state = ParserState.SECOND_PARSED;
                    }
                } else if (state == ParserState.HMS_PARSED) {
                    days = nullSafeIntGet(part);
                    if (negative) {
                        days = -days;
                        negative = false;
                    }
                    state = ParserState.DAYS_PARSED;
                } else if (state == ParserState.SECOND_PARSED) {
                    //Invalid day second combination
                    throw new IllegalArgumentException("Invalid interval format: " + value);
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval format: " + value);
        }

        if (ParserState.NOTHING_PARSED == state) {
            throw new IllegalArgumentException("Invalid interval format: " + value);
        }

        return new Period(years, months, 0, days, hours, minutes, seconds, 0);
    }

    private static final Pattern YEAR_MONTH_PATTERN = Pattern.compile("-?\\d{1,9}-\\d{1,9}");
}
