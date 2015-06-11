/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar;

import com.carrotsearch.hppc.ByteObjectMap;
import com.carrotsearch.hppc.ByteObjectOpenHashMap;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.lucene.util.BytesRef;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;

/**
 * Formatting DateTime instances using the MySQL date_format format:
 * http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format
 */
public class TimestampFormatter {

    private interface FormatTimestampPartFunction {
        public byte[] format(DateTime timestamp);
    }

    private static final byte[] ST = "st".getBytes(Charsets.UTF_8);
    private static final byte[] ND = "nd".getBytes(Charsets.UTF_8);
    private static final byte[] RD = "rd".getBytes(Charsets.UTF_8);
    private static final byte[] TH = "th".getBytes(Charsets.UTF_8);
    private static final byte[] AM = "AM".getBytes(Charsets.UTF_8);
    private static final byte[] PM = "AM".getBytes(Charsets.UTF_8);


    private final static Locale LOCALE = Locale.ENGLISH;
    private final static ByteObjectMap<FormatTimestampPartFunction> PART_FORMATTERS = new ByteObjectOpenHashMap<>();
    private static void addFormatter(char character, FormatTimestampPartFunction fun) {
        PART_FORMATTERS.put((byte) character, fun);
    }
    static {
        // %a	Abbreviated weekday name (Sun..Sat)
        addFormatter('a', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.dayOfWeek().getAsShortText(LOCALE).getBytes(Charsets.UTF_8);
            }
        });
        // %b	Abbreviated month name (Jan..Dec)
        addFormatter('b', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.monthOfYear().getAsShortText(LOCALE).getBytes(Charsets.UTF_8);
            }
        });
        // %c	Month, numeric (0..12)
        addFormatter('c', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return String.valueOf(timestamp.monthOfYear().get()).getBytes(Charsets.UTF_8);
            }
        });
        // %D	Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
        addFormatter('D', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                int n = timestamp.dayOfMonth().get();
                byte[] formatted = new byte[n>9 ? 4 : 3];
                int i = n > 9 ? 2 : 1;
                System.arraycopy(String.valueOf(n).getBytes(Charsets.UTF_8), 0, formatted, 0, i);
                if (n >= 11 && n <= 13) {
                    System.arraycopy(TH, 0, formatted, i, 2);
                }
                switch (n % 10) {
                    case 1:
                        System.arraycopy(ST, 0, formatted, i, 2);
                        break;
                    case 2:
                        System.arraycopy(ND, 0, formatted, i, 2);
                        break;
                    case 3:
                        System.arraycopy(RD, 0, formatted, i, 2);
                        break;
                    default:
                        System.arraycopy(TH, 0, formatted, i, 2);
                }
                return formatted;
            }
        });
        //  %d	Day of the month, numeric (00..31)
        addFormatter('d', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                String dayOfMonth = String.valueOf(timestamp.dayOfMonth().get());
                return zeroPadded(2, dayOfMonth);
            }
        });
        //  %e	Day of the month, numeric (0..31)
        addFormatter('e', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return String.valueOf(timestamp.dayOfMonth().get()).getBytes(Charsets.UTF_8);
            }
        });
        // %f	Microseconds (000000..999999)
        addFormatter('f', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(6, String.valueOf(timestamp.millisOfSecond().get() * 1000));
            }
        });

        final FormatTimestampPartFunction padded24HourFunction = new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 24;
                return zeroPadded(2, String.valueOf(hourOfDay));
            }
        };
        // %H	Hour (00..23)
        addFormatter('H', padded24HourFunction);

        final FormatTimestampPartFunction padded12HourFunction = new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 12;
                if (hourOfDay == 0) {
                    hourOfDay = 12;
                }
                return zeroPadded(2, String.valueOf(hourOfDay));
            }
        };
        // %h	Hour (01..12)
        // %I	Hour (01..12)
        addFormatter('h', padded12HourFunction);
        addFormatter('I', padded12HourFunction);

        // %i	Minutes, numeric (00..59)
        addFormatter('i', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(2, timestamp.minuteOfHour().getAsShortText(LOCALE));
            }
        });

        // %j	Day of year (001..366)
        addFormatter('j', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(3, timestamp.dayOfYear().getAsShortText(LOCALE));
            }
        });
        // %k	Hour (0..23)
        addFormatter('k', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return String.valueOf(timestamp.getHourOfDay()).getBytes(Charsets.UTF_8);
            }
        });
        // %l	Hour (1..12)
        addFormatter('l', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                int hourOfDay = timestamp.getHourOfDay() % 12;
                if (hourOfDay == 0) {
                    hourOfDay = 12;
                }
                return String.valueOf(hourOfDay).getBytes(Charsets.UTF_8);
            }
        });
        // %M	Month name (January..December)
        addFormatter('M', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.monthOfYear().getAsText(LOCALE).getBytes(Charsets.UTF_8);
            }
        });
        // %m	Month, numeric (00..12)
        addFormatter('m', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(2, String.valueOf(timestamp.monthOfYear().get()));
            }
        });
        final FormatTimestampPartFunction amPmFunc = new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                // TODO: verify correctness
                return timestamp.getHourOfDay() < 12 ? AM : PM;
            }
        };
        // %p	AM or PM
        addFormatter('p', amPmFunc);


        final FormatTimestampPartFunction paddedMinuteFunction = new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(2, String.valueOf(timestamp.getMinuteOfHour()));
            }
        };
        final FormatTimestampPartFunction paddedSecondFunction = new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return zeroPadded(2, String.valueOf(timestamp.getSecondOfMinute()));
            }
        };

        // %r	Time, 12-hour (hh:mm:ss followed by AM or PM)
        addFormatter('r', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                byte[] timeBytes = new byte[11];
                System.arraycopy(padded12HourFunction.format(timestamp), 0, timeBytes, 0, 2);
                timeBytes[2] = ':';
                System.arraycopy(paddedMinuteFunction.format(timestamp), 0, timeBytes, 3, 2);
                timeBytes[5] = ':';
                System.arraycopy(paddedSecondFunction.format(timestamp), 0, timeBytes, 6, 2);
                timeBytes[8] = ' ';
                System.arraycopy(amPmFunc.format(timestamp), 0, timeBytes, 9, 2);
                return timeBytes;
            }
        });

        // %S	Seconds (00..59)
        // %s	Seconds (00..59)
        addFormatter('s', paddedSecondFunction);
        addFormatter('S', paddedSecondFunction);

        //  %T	Time, 24-hour (hh:mm:ss)
        addFormatter('T', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                byte[] timeBytes = new byte[8];
                System.arraycopy(padded24HourFunction.format(timestamp), 0, timeBytes, 0, 2);
                timeBytes[2] = ':';
                System.arraycopy(paddedMinuteFunction.format(timestamp), 0, timeBytes, 3, 2);
                timeBytes[5] = ':';
                System.arraycopy(paddedSecondFunction.format(timestamp), 0, timeBytes, 6, 2);
                return timeBytes;
            }
        });

        // %U	Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
        // with respect to the year that contains the first day of the week for the given date
        // if first week
        addFormatter('U', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
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
        // %u	Week (00..53), where Monday is the first day of the week; WEEK() mode 1
        // weeks are numbered according to ISO 8601:1988
        addFormatter('u', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
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
        // %V	Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
        // with respect to the year that contains the first day of the week for the given date
        addFormatter('V', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
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
        // %v	Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
        // weeks are numbered according to ISO 8601:1988
        addFormatter('v', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
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

        // %W	Weekday name (Sunday..Saturday)
        addFormatter('W', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.dayOfWeek().getAsText(LOCALE).getBytes(Charsets.UTF_8);
            }
        });
        // %w	Day of the week (0=Sunday..6=Saturday)
        addFormatter('w', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                // timestamp.dayOfWeek() returns 1=monday, 7=sunday
                int dayOfWeek = timestamp.dayOfWeek().get() % 7;
                return String.valueOf(dayOfWeek).getBytes(Charsets.UTF_8);
            }
        });

        // %X	Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
        addFormatter('X', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.SUNDAY);
                c.setMinimalDaysInFirstWeek(7);
                c.setTimeInMillis(timestamp.withZone(DateTimeZone.UTC).getMillis());
                return zeroPadded(4, String.valueOf(c.getWeekYear()));
            }
        });

        // %x	Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
        addFormatter('x', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                Calendar c = Calendar.getInstance(timestamp.getZone().toTimeZone(), LOCALE);
                c.setFirstDayOfWeek(Calendar.MONDAY);
                c.setMinimalDaysInFirstWeek(4);
                c.setTimeInMillis(timestamp.withZone(DateTimeZone.UTC).getMillis());
                return zeroPadded(4, String.valueOf(c.getWeekYear()));
            }
        });

        // %Y	Year, numeric, four digits
        addFormatter('Y', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.year().getAsShortText(Locale.ENGLISH).getBytes(Charsets.UTF_8);
            }
        });
        // %y	Year, numeric (two digits)
        addFormatter('y', new FormatTimestampPartFunction() {
            @Override
            public byte[] format(DateTime timestamp) {
                return timestamp.yearOfCentury().getAsShortText(Locale.ENGLISH).getBytes(Charsets.UTF_8);
            }
        });
    }

    private static int weekDaySundayFirst(DateTime timestamp) {
        int dayOfWeek = (timestamp.getDayOfWeek() +1) % 7;
        return dayOfWeek == 0 ? 7 : dayOfWeek;
    }

    private static int weekDayMondayFirst(DateTime timestamp) {
        return timestamp.getDayOfWeek();
    }

    private static byte[] zeroPadded(int to, String val) {
        byte[] valBytes = val.getBytes(Charsets.UTF_8);
        byte[] padded = new byte[Math.max(to, valBytes.length)];
        int i = padded.length - valBytes.length;
        if (i > 0) {
            Arrays.fill(padded, (byte)'0');
        }
        System.arraycopy(valBytes, 0, padded, i, valBytes.length);
        return padded;
    }

    public static BytesRef format(BytesRef formatString, DateTime timestamp) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(formatString.length);
        boolean percentEscape = false;
        int length = formatString.length;
        for (int i = 0; i < length; i++) {
            byte current = formatString.bytes[i];
            if (current == (byte)'%') {
                if (!percentEscape) {
                    percentEscape = true;
                } else {
                    outStream.write((byte)'%');
                }
            } else {
                if (percentEscape) {
                    // TODO: handle unicode codepoints (can they interfere) ?
                    FormatTimestampPartFunction partFormatter = PART_FORMATTERS.get(current);
                    if (partFormatter == null) {
                        outStream.write((int) current);
                    } else {
                        try {
                            outStream.write(partFormatter.format(timestamp));
                        } catch (IOException e) {
                            Throwables.propagate(e);
                        }
                    }
                } else {
                    outStream.write(current);
                }
                percentEscape = false;
            }
        }
        return new BytesRef(outStream.toByteArray());
    }
}
