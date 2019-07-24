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

import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormat;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.StringTokenizer;

public class IntervalParser {

    private IntervalParser() {
    }

    public static Interval apply(String value) {
        Period period = null;
        Interval.Format format = null;
        try {
            period = parseSeconds(value);
            format = Interval.Format.NUMERICAL;
        } catch (IllegalArgumentException e1) {
            try {
                period = ISOPeriodFormat.standard().parsePeriod(value);
                format = Interval.Format.IS0_8601;
            } catch (IllegalArgumentException e2) {
                try {
                    period = ISOPeriodFormat.standard().parsePeriod(value);
                    format = Interval.Format.IS0_8601;
                } catch (IllegalArgumentException e3) {
                    period = parsePsqlFormat(value);
                    format = Interval.Format.PSQL;
                }
            }
        }
        return new Interval(period, format);
    }

    public static Period parsePsqlFormat(String value) {
        final boolean ISOFormat = !value.startsWith("@");

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

                    hours = nullSafeIntGet(token.substring(offset + 0, endHours));
                    minutes = nullSafeIntGet(token.substring(endHours + 1, endHours + 3));

                    // Pre 7.4 servers do not put second information into the results
                    // unless it is non-zero.
                    int endMinutes = token.indexOf(':', endHours + 1);
                    if (endMinutes != -1) {
                        seconds = parseInteger(token.substring(endMinutes + 1));
                        milliSeconds = parseMiliSeconds(token.substring(endMinutes + 1));
                    }

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
                    } else if (token.startsWith("hour")) {
                        hours = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("min")) {
                        minutes = nullSafeIntGet(valueToken);
                    } else if (token.startsWith("sec")) {
                        seconds = parseInteger(valueToken);
                        milliSeconds = parseMiliSeconds(valueToken);
                    }
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Conversion of interval failed", e);

        }

        if (!ISOFormat && value.endsWith("ago")) {
            // Inverse the leading sign
            return new Period(-years, -months, 0, -days, -hours, -minutes, -seconds, -milliSeconds);
        } else {
            return new Period(years, months, 0, days, hours, minutes, seconds, milliSeconds);
        }
    }

    private static int parseMiliSeconds(String value) throws NumberFormatException {
        return new BigDecimal(value).subtract(new BigDecimal(parseInteger(value))).multiply(new BigDecimal(1000)).intValue();
    }

    private static Period parseNumerical(String value) throws NumberFormatException {
        try {
            int seconds = parseInteger(value);
            int milliSeconds = parseDecimal(value) * 1000;
            return new Period().withSeconds(seconds).withMillis(milliSeconds);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

    private static Period parseSeconds(String value) throws NumberFormatException {
        try {
            int seconds = parseInteger(value);
            int milliSeconds = parseDecimal(value) * 1000;
            return new Period().withSeconds(seconds).withMillis(milliSeconds);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

    private static int nullSafeIntGet(String value) {
        try {
            return (value == null) ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

    public static int parseInteger(String value) {
        try {
            return new BigDecimal(value).intValue();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

    public static int parseDecimal(String value) {
        try {
            BigDecimal subtract = new BigDecimal(value).subtract(new BigDecimal(parseInteger(value)));
            return subtract.intValue();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid format " + value, e);
        }
    }

}
