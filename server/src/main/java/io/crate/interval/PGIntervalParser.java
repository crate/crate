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
import java.util.StringTokenizer;

import static io.crate.interval.IntervalParser.nullSafeIntGet;
import static io.crate.interval.IntervalParser.parseMilliSeconds;
import static io.crate.interval.IntervalParser.roundToPrecision;

final class PGIntervalParser {

    static Period apply(String value,
                        @Nullable IntervalParser.Precision start,
                        @Nullable IntervalParser.Precision end) {
        return roundToPrecision(apply(value), start, end);
    }

    static Period apply(String value) {
        final boolean ISOFormat = !value.startsWith("@");

        // Just a simple '0'
        if (!ISOFormat && value.length() == 3 && value.charAt(2) == '0') {
            return new Period();
        }
        boolean dataParsed = false;
        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int milliSeconds = 0;

        try {
            String valueToken = null;

            value = value.replace('+', ' ').replace('@', ' ');
            final StringTokenizer st = new StringTokenizer(value);
            for (int i = 1; st.hasMoreTokens(); i++) {
                String token = st.nextToken();

                if ((i & 1) == 1) {
                    int endHours = token.indexOf(':');
                    if (endHours == -1) {
                        valueToken = token;
                        continue;
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
                    valueToken = null;
                } else {
                    // This handles years, months, days for both, ISO and
                    // Non-ISO intervals. Hours, minutes, seconds and microseconds
                    // are handled for Non-ISO intervals here.
                    if (token.startsWith("year")) {
                        years = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("mon")) {
                        months = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("day")) {
                        days = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("week")) {
                        days = nullSafeIntGet(valueToken) * 7;
                    } else if (token.startsWith("hour")) {
                        hours = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("min")) {
                        minutes = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("sec")) {
                        seconds = parseInteger(valueToken);
                        milliSeconds = parseMilliSeconds(valueToken);
                    } else {
                        throw new IllegalArgumentException("Invalid interval format " + value);
                    }
                    dataParsed = true;
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid interval format " + value);
        }

        if (!dataParsed) {
            throw new IllegalArgumentException("Invalid interval format " + value);
        }

        Period period = new Period(years, months, 0, days, hours, minutes, seconds, milliSeconds);

        if (!ISOFormat && value.endsWith("ago")) {
            // Inverse the leading sign
            period = period.negated();
        }
        return period;
    }


    private static int parseInteger(String value) {
        return new BigDecimal(value).intValue();
    }
}
