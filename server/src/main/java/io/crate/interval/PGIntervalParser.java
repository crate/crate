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

import static io.crate.interval.IntervalParser.nullSafeIntGet;
import static io.crate.interval.IntervalParser.parseMilliSeconds;
import static io.crate.interval.IntervalParser.roundToPrecision;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.StringTokenizer;

import org.jetbrains.annotations.Nullable;
import org.joda.time.Period;

final class PGIntervalParser {

    private PGIntervalParser() {}

    static Period apply(String value,
                        @Nullable IntervalParser.Precision start,
                        @Nullable IntervalParser.Precision end) {
        return roundToPrecision(apply(value), start, end);
    }

    static Period apply(String value) {
        String strInterval = value.trim().toLowerCase(Locale.ENGLISH);
        final boolean ISOFormat = !strInterval.startsWith("@");
        final boolean hasAgo = strInterval.endsWith("ago");
        strInterval = strInterval
            .replace("+", "")
            .replace("@", "")
            .replace("ago", "")
            .trim();

        // Just a simple '0'
        if (!ISOFormat && value.length() == 3 && value.charAt(2) == '0') {
            return new Period();
        }
        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int milliSeconds = 0;
        boolean weeksParsed = false;
        boolean daysParsed = false;

        try {
            String unitToken = null;
            String valueToken;
            boolean timeParsed = false;
            final StringTokenizer st = new StringTokenizer(strInterval);
            while (st.hasMoreTokens()) {
                String token = st.nextToken();

                int firstCharIdx = firstCharacterInStr(token);
                if (firstCharIdx > 0) { // value next to unit, e.g.: '1year'
                    valueToken = token.substring(0, firstCharIdx);
                    unitToken = token.substring(firstCharIdx);
                } else { // value and unit separated with whitespace, e.g.: '1  year'
                    valueToken = token;
                    if (st.hasMoreTokens()) {
                        unitToken = st.nextToken();
                    }
                }

                int endHours = token.indexOf(':');
                if (endHours > 0) {
                    if (timeParsed) {
                        throw new IllegalArgumentException("Invalid interval format: " + value);
                    }

                    // This handles hours, minutes, seconds and microseconds for
                    // ISO intervals
                    int offset = (token.charAt(0) == '-') ? 1 : 0;

                    hours = nullSafeIntGet(token.substring(offset, endHours));
                    minutes = nullSafeIntGet(token.substring(endHours + 1, endHours + 3));

                    int endMinutes = token.indexOf(':', endHours + 1);
                    seconds = parseInteger(token.substring(endMinutes + 1));
                    milliSeconds = parseMilliSeconds(token.substring(endMinutes + 1));

                    if (offset == 1) {
                        hours = -hours;
                        minutes = -minutes;
                        seconds = -seconds;
                        milliSeconds = -milliSeconds;
                    }
                    timeParsed = true;
                } else {
                    if (unitToken == null) {
                        throw new IllegalArgumentException("Invalid interval format: " + value);
                    }
                }

                // This handles years, months, days for both, ISO and
                // Non-ISO intervals. Hours, minutes, seconds and microseconds
                // are handled for Non-ISO intervals here.
                if (unitToken != null) {
                    switch (unitToken) {
                        case "year", "years" -> {
                            if (years > 0) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            years = nullSafeIntGet(valueToken);
                        }
                        case "month", "months", "mon", "mons" -> {
                            if (months > 0) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            months = nullSafeIntGet(valueToken);
                        }
                        case "day", "days" -> {
                            if (daysParsed) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            days += nullSafeIntGet(valueToken);
                            daysParsed = true;
                        }
                        case "week", "weeks" -> {
                            if (weeksParsed) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            days += nullSafeIntGet(valueToken) * 7;
                            weeksParsed = true;
                        }
                        case "hour", "hours" -> {
                            if (hours > 0) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            hours = nullSafeIntGet(valueToken);
                            timeParsed = true;
                        }
                        case "min", "mins", "minute", "minutes" -> {
                            if (minutes > 0) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            minutes = nullSafeIntGet(valueToken);
                            timeParsed = true;
                        }
                        case "sec", "secs", "second", "seconds" -> {
                            if (seconds > 0 || milliSeconds > 0) {
                                throw new IllegalArgumentException("Invalid interval format: " + value);
                            }
                            seconds = parseInteger(valueToken);
                            milliSeconds = parseMilliSeconds(valueToken);
                            timeParsed = true;
                        }
                        default -> throw new IllegalArgumentException("Invalid interval format: " + value);
                    }
                }
                unitToken = null;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval format: " + value);
        }

        Period period = new Period(years, months, 0, days, hours, minutes, seconds, milliSeconds);

        if (!ISOFormat && hasAgo) {
            // Inverse the leading sign
            period = period.negated();
        }
        return period;
    }

    private static int firstCharacterInStr(String token) {
        for (int i = 0; i < token.length(); i++) {
            if (Character.isLetter(token.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static int parseInteger(String value) {
        return new BigDecimal(value).intValue();
    }
}
