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

package io.crate.expression.scalar;

import com.carrotsearch.hppc.CharObjectHashMap;
import com.carrotsearch.hppc.CharObjectMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;

/**
 * Formatting DateTime instances using the MySQL date_format format:
 * http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format
 */
public class TimestampFormatter {

    private interface FormatTimestampPartFunction {
        String format(DateTime timestamp);
    }

    private static final Locale LOCALE = Locale.ENGLISH;
    private static final CharObjectMap<FormatTimestampPartFunction> PART_FORMATTERS = new CharObjectHashMap<>();

    private static void addFormatter(char character, FormatTimestampPartFunction fun) {
        PART_FORMATTERS.put(character, fun);
    }

    static {
        // %a Abbreviated weekday name (Sun..Sat)
        addFormatter('a', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.dayOfWeek().getAsShortText(LOCALE);
            }
        });
        // %b Abbreviated month name (Jan..Dec)
        addFormatter('b', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.monthOfYear().getAsShortText(LOCALE);
            }
        });
        // %c Month, numeric (0..12)
        addFormatter('c', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return String.valueOf(timestamp.monthOfYear().get());
            }
        });
        // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
        addFormatter('D', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                int n = timestamp.dayOfMonth().get();
                StringBuilder builder = new StringBuilder(n > 9 ? 4 : 3);
                builder.append(n);
                if (n >= 11 && n <= 13) {
                    builder.append("th");
                } else {
                    switch (n % 10) {
                        case 1:
                            builder.append("st");
                            break;
                        case 2:
                            builder.append("nd");
                            break;
                        case 3:
                            builder.append("rd");
                            break;
                        default:
                            builder.append("th");
                    }
                }
                return builder.toString();
            }
        });
        //  %d Day of the month, numeric (00..31)
        addFormatter('d', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                String dayOfMonth = String.valueOf(timestamp.dayOfMonth().get());
                return zeroPadded(2, dayOfMonth);
            }
        });
        //  %e Day of the month, numeric (0..31)
        addFormatter('e', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return String.valueOf(timestamp.dayOfMonth().get());
            }
        });
        // %f Microseconds (000000..999999)
        addFormatter('f', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(6, String.valueOf(timestamp.millisOfSecond().get() * 1000));
            }
        });

        final FormatTimestampPartFunction padded24HourFunction = new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 24;
                return zeroPadded(2, String.valueOf(hourOfDay));
            }
        };
        // %H Hour (00..23)
        addFormatter('H', padded24HourFunction);

        final FormatTimestampPartFunction padded12HourFunction = new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 12;
                if (hourOfDay == 0) {
                    hourOfDay = 12;
                }
                return zeroPadded(2, String.valueOf(hourOfDay));
            }
        };
        // %h Hour (01..12)
        // %I Hour (01..12)
        addFormatter('h', padded12HourFunction);
        addFormatter('I', padded12HourFunction);

        // %i Minutes, numeric (00..59)
        addFormatter('i', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(2, timestamp.minuteOfHour().getAsShortText(LOCALE));
            }
        });

        // %j Day of year (001..366)
        addFormatter('j', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(3, timestamp.dayOfYear().getAsShortText(LOCALE));
            }
        });
        // %k Hour (0..23)
        addFormatter('k', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.hourOfDay().getAsShortText(LOCALE);
            }
        });
        // %l Hour (1..12)
        addFormatter('l', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 12;
                if (hourOfDay == 0) {
                    hourOfDay = 12;
                }
                return String.valueOf(hourOfDay);
            }
        });
        // %M Month name (January..December)
        addFormatter('M', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.monthOfYear().getAsText(LOCALE);
            }
        });
        // %m Month, numeric (00..12)
        addFormatter('m', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(2, String.valueOf(timestamp.monthOfYear().get()));
            }
        });
        final FormatTimestampPartFunction amPmFunc = new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                // TODO: verify correctness
                return timestamp.getHourOfDay() < 12 ? "AM" : "PM";
            }
        };
        // %p AM or PM
        addFormatter('p', amPmFunc);


        final FormatTimestampPartFunction paddedMinuteFunction = new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(2, timestamp.minuteOfHour().getAsShortText(LOCALE));
            }
        };
        final FormatTimestampPartFunction paddedSecondFunction = new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return zeroPadded(2, timestamp.secondOfMinute().getAsShortText(LOCALE));
            }
        };

        // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
        addFormatter('r', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return padded12HourFunction.format(timestamp) + ':'
                       + paddedMinuteFunction.format(timestamp) + ':'
                       + paddedSecondFunction.format(timestamp) + ' '
                       + amPmFunc.format(timestamp);
            }
        });

        // %S Seconds (00..59)
        // %s Seconds (00..59)
        addFormatter('s', paddedSecondFunction);
        addFormatter('S', paddedSecondFunction);

        //  %T Time, 24-hour (hh:mm:ss)
        addFormatter('T', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return padded24HourFunction.format(timestamp) + ':'
                       + paddedMinuteFunction.format(timestamp) + ':'
                       + paddedSecondFunction.format(timestamp);
            }
        });

        // %U Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
        // with respect to the year that contains the first day of the week for the given date
        // if first week
        addFormatter('U', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                // if week starts in last year, return 00
                // range 00 - 53
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.SUNDAY);
                c.setMinimalDaysInFirstWeek(7);
                c.setTimeInMillis(timestamp.getMillis());
                int week = c.get(Calendar.WEEK_OF_YEAR);
                int weekYear = c.getWeekYear();
                int year = c.get(Calendar.YEAR);
                if (weekYear < year) {
                    week = 0;
                } else if (weekYear > year) {
                    week = c.getWeeksInWeekYear();
                }
                return zeroPadded(2, String.valueOf(week));
            }
        });
        // %u Week (00..53), where Monday is the first day of the week; WEEK() mode 1
        // weeks are numbered according to ISO 8601:1988
        addFormatter('u', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.MONDAY);
                c.setMinimalDaysInFirstWeek(4);
                c.setTimeInMillis(timestamp.getMillis());
                int week = c.get(Calendar.WEEK_OF_YEAR);
                int weekYear = c.getWeekYear();
                int year = c.get(Calendar.YEAR);
                if (weekYear < year) {
                    week = 0;
                } else if (weekYear > year) {
                    week = c.getWeeksInWeekYear();
                }
                return zeroPadded(2, String.valueOf(week));
            }
        });
        // %V Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
        // with respect to the year that contains the first day of the week for the given date
        addFormatter('V', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.SUNDAY);
                c.setMinimalDaysInFirstWeek(7);
                c.setTimeInMillis(timestamp.getMillis());

                int week = c.get(Calendar.WEEK_OF_YEAR);
                int weekYear = c.getWeekYear();
                int year = c.get(Calendar.YEAR);
                if (weekYear < year) {
                    // get weeks from last year
                    c.add(Calendar.DAY_OF_MONTH, -7);
                    week = c.getWeeksInWeekYear();
                }
                return zeroPadded(2, String.valueOf(week));
            }
        });
        // %v Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
        // weeks are numbered according to ISO 8601:1988
        addFormatter('v', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.MONDAY);
                c.setMinimalDaysInFirstWeek(4);
                c.setTimeInMillis(timestamp.getMillis());

                int week = c.get(Calendar.WEEK_OF_YEAR);
                int weekYear = c.getWeekYear();
                int year = c.get(Calendar.YEAR);
                if (weekYear < year) {
                    // get weeks from last year
                    c.add(Calendar.DAY_OF_MONTH, -7);
                    week = c.getWeeksInWeekYear();
                }
                return zeroPadded(2, String.valueOf(week));
            }
        });

        // %W Weekday name (Sunday..Saturday)
        addFormatter('W', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.dayOfWeek().getAsText(LOCALE);
            }
        });
        // %w Day of the week (0=Sunday..6=Saturday)
        addFormatter('w', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                // timestamp.dayOfWeek() returns 1=monday, 7=sunday
                int dayOfWeek = timestamp.dayOfWeek().get() % 7;
                return String.valueOf(dayOfWeek);
            }
        });

        // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
        addFormatter('X', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.SUNDAY);
                c.setMinimalDaysInFirstWeek(7);
                c.setTimeInMillis(timestamp.withZone(DateTimeZone.UTC).getMillis());
                return zeroPadded(4, String.valueOf(c.getWeekYear()));
            }
        });

        // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
        addFormatter('x', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.MONDAY);
                c.setMinimalDaysInFirstWeek(4);
                c.setTimeInMillis(timestamp.withZone(DateTimeZone.UTC).getMillis());
                return zeroPadded(4, String.valueOf(c.getWeekYear()));
            }
        });

        // %Y Year, numeric, four digits
        addFormatter('Y', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.year().getAsShortText(Locale.ENGLISH);
            }
        });
        // %y Year, numeric (two digits)
        addFormatter('y', new FormatTimestampPartFunction() {
            @Override
            public String format(DateTime timestamp) {
                return timestamp.yearOfCentury().getAsShortText(Locale.ENGLISH);
            }
        });
    }

    private static String zeroPadded(int to, String val) {
        int length = val.length();
        if (length >= to) {
            return val;
        } else {
            char[] padded = new char[to];
            Arrays.fill(padded, '0');
            val.getChars(0, length, padded, Math.max(0, to - length));
            return new String(padded);
        }
    }

    public static String format(String formatString, DateTime timestamp) {
        int length = formatString.length();
        StringBuilder buffer = new StringBuilder(length);
        boolean percentEscape = false;
        for (int i = 0; i < length; i++) {
            char current = formatString.charAt(i);

            if (current == '%') {
                if (!percentEscape) {
                    percentEscape = true;
                } else {
                    buffer.append('%');
                    percentEscape = false;
                }
            } else {
                if (percentEscape) {
                    FormatTimestampPartFunction partFormatter = PART_FORMATTERS.get(current);
                    if (partFormatter == null) {
                        buffer.append(current);
                    } else {
                        buffer.append(partFormatter.format(timestamp));
                    }
                } else {
                    buffer.append(current);
                }
                percentEscape = false;
            }
        }
        return buffer.toString();
    }
}
