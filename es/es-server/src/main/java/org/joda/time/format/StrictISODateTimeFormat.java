/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.joda.time.format;

import org.joda.time.DateTimeFieldType;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/*
 * Elasticsearch Note: This class has been copied almost identically from joda, where the
 * class is named ISODatetimeFormat
 *
 * However there has been done one huge modification in several methods, which forces the date
 * year to be exactly n digits, so that a year like "5" is invalid and must be "0005"
 *
 * All methods have been marked with an "// ES change" commentary
 *
 * In case you compare this with the original ISODateTimeFormat, make sure you use a diff
 * call, that ignores whitespaces/tabs/indentations like 'diff -b'
 */
/**
 * Factory that creates instances of DateTimeFormatter based on the ISO8601
 * standard.
 * <p>
 * Date-time formatting is performed by the {@link DateTimeFormatter} class.
 * Three classes provide factory methods to create formatters, and this is one.
 * The others are {@link DateTimeFormat} and {@link DateTimeFormatterBuilder}.
 * <p>
 * ISO8601 is the international standard for data interchange. It defines a
 * framework, rather than an absolute standard. As a result this provider has a
 * number of methods that represent common uses of the framework. The most
 * common formats are {@link #date() date}, {@link #time() time}, and
 * {@link #dateTime() dateTime}.
 * <p>
 * For example, to format a date time in ISO format:
 *
 * <pre>
 * DateTime dt = new DateTime();
 * DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
 * String str = fmt.print(dt);
 * </pre>
 * <p>
 * Note that these formatters mostly follow the ISO8601 standard for printing.
 * For parsing, the formatters are more lenient and allow formats that are not
 * in strict compliance with the standard.
 * <p>
 * It is important to understand that these formatters are not linked to the
 * <code>ISOChronology</code>. These formatters may be used with any chronology,
 * however there may be certain side effects with more unusual chronologies. For
 * example, the ISO formatters rely on dayOfWeek being single digit, dayOfMonth
 * being two digit and dayOfYear being three digit. A chronology with a ten day
 * week would thus cause issues. However, in general, it is safe to use these
 * formatters with other chronologies.
 * <p>
 * ISODateTimeFormat is thread-safe and immutable, and the formatters it returns
 * are as well.
 *
 * @author Brian S O'Neill
 * @since 1.0
 * @see DateTimeFormat
 * @see DateTimeFormatterBuilder
 */
public class StrictISODateTimeFormat {

    /**
     * Constructor.
     *
     * @since 1.1 (previously private)
     */
    protected StrictISODateTimeFormat() {
        super();
    }

    // -----------------------------------------------------------------------
    /**
     * Returns a formatter that outputs only those fields specified.
     * <p>
     * This method examines the fields provided and returns an ISO-style formatter
     * that best fits. This can be useful for outputting less-common ISO styles,
     * such as YearMonth (YYYY-MM) or MonthDay (--MM-DD).
     * <p>
     * The list provided may have overlapping fields, such as dayOfWeek and
     * dayOfMonth. In this case, the style is chosen based on the following list,
     * thus in the example, the calendar style is chosen as dayOfMonth is higher in
     * priority than dayOfWeek:
     * <ul>
     * <li>monthOfYear - calendar date style
     * <li>dayOfYear - ordinal date style
     * <li>weekOfWeekYear - week date style
     * <li>dayOfMonth - calendar date style
     * <li>dayOfWeek - week date style
     * <li>year
     * <li>weekyear
     * </ul>
     * The supported formats are:
     *
     * <pre>
     * Extended      Basic       Fields
     * 2005-03-25    20050325    year/monthOfYear/dayOfMonth
     * 2005-03       2005-03     year/monthOfYear
     * 2005--25      2005--25    year/dayOfMonth *
     * 2005          2005        year
     * --03-25       --0325      monthOfYear/dayOfMonth
     * --03          --03        monthOfYear
     * ---03         ---03       dayOfMonth
     * 2005-084      2005084     year/dayOfYear
     * -084          -084        dayOfYear
     * 2005-W12-5    2005W125    weekyear/weekOfWeekyear/dayOfWeek
     * 2005-W-5      2005W-5     weekyear/dayOfWeek *
     * 2005-W12      2005W12     weekyear/weekOfWeekyear
     * -W12-5        -W125       weekOfWeekyear/dayOfWeek
     * -W12          -W12        weekOfWeekyear
     * -W-5          -W-5        dayOfWeek
     * 10:20:30.040  102030.040  hour/minute/second/milli
     * 10:20:30      102030      hour/minute/second
     * 10:20         1020        hour/minute
     * 10            10          hour
     * -20:30.040    -2030.040   minute/second/milli
     * -20:30        -2030       minute/second
     * -20           -20         minute
     * --30.040      --30.040    second/milli
     * --30          --30        second
     * ---.040       ---.040     milli *
     * 10-30.040     10-30.040   hour/second/milli *
     * 10:20-.040    1020-.040   hour/minute/milli *
     * 10-30         10-30       hour/second *
     * 10--.040      10--.040    hour/milli *
     * -20-.040      -20-.040    minute/milli *
     *   plus datetime formats like {date}T{time}
     * </pre>
     *
     * * indicates that this is not an official ISO format and can be excluded by
     * passing in <code>strictISO</code> as <code>true</code>.
     * <p>
     * This method can side effect the input collection of fields. If the input
     * collection is modifiable, then each field that was added to the formatter
     * will be removed from the collection, including any duplicates. If the input
     * collection is unmodifiable then no side effect occurs.
     * <p>
     * This side effect processing is useful if you need to know whether all the
     * fields were converted into the formatter or not. To achieve this, pass in a
     * modifiable list, and check that it is empty on exit.
     *
     * @param fields    the fields to get a formatter for, not null, updated by the
     *                  method call unless unmodifiable, removing those fields built
     *                  in the formatter
     * @param extended  true to use the extended format (with separators)
     * @param strictISO true to stick exactly to ISO8601, false to include
     *                  additional formats
     * @return a suitable formatter
     * @throws IllegalArgumentException if there is no format for the fields
     * @since 1.1
     */
    public static DateTimeFormatter forFields(Collection<DateTimeFieldType> fields, boolean extended,
            boolean strictISO) {

        if (fields == null || fields.size() == 0) {
            throw new IllegalArgumentException("The fields must not be null or empty");
        }
        Set<DateTimeFieldType> workingFields = new HashSet<DateTimeFieldType>(fields);
        int inputSize = workingFields.size();
        boolean reducedPrec = false;
        DateTimeFormatterBuilder bld = new DateTimeFormatterBuilder();
        // date
        if (workingFields.contains(DateTimeFieldType.monthOfYear())) {
            reducedPrec = dateByMonth(bld, workingFields, extended, strictISO);
        } else if (workingFields.contains(DateTimeFieldType.dayOfYear())) {
            reducedPrec = dateByOrdinal(bld, workingFields, extended);
        } else if (workingFields.contains(DateTimeFieldType.weekOfWeekyear())) {
            reducedPrec = dateByWeek(bld, workingFields, extended, strictISO);
        } else if (workingFields.contains(DateTimeFieldType.dayOfMonth())) {
            reducedPrec = dateByMonth(bld, workingFields, extended, strictISO);
        } else if (workingFields.contains(DateTimeFieldType.dayOfWeek())) {
            reducedPrec = dateByWeek(bld, workingFields, extended, strictISO);
        } else if (workingFields.remove(DateTimeFieldType.year())) {
            bld.append(Constants.YE);
            reducedPrec = true;
        } else if (workingFields.remove(DateTimeFieldType.weekyear())) {
            bld.append(Constants.WE);
            reducedPrec = true;
        }
        boolean datePresent = (workingFields.size() < inputSize);

        // time
        time(bld, workingFields, extended, strictISO, reducedPrec, datePresent);

        // result
        if (bld.canBuildFormatter() == false) {
            throw new IllegalArgumentException("No valid format for fields: " + fields);
        }

        // side effect the input collection to indicate the processed fields
        // handling unmodifiable collections with no side effect
        try {
            fields.retainAll(workingFields);
        } catch (UnsupportedOperationException ex) {
            // ignore, so we can handle unmodifiable collections
        }
        return bld.toFormatter();
    }

    // -----------------------------------------------------------------------
    /**
     * Creates a date using the calendar date format. Specification reference:
     * 5.2.1.
     *
     * @param bld       the builder
     * @param fields    the fields
     * @param extended  true to use extended format
     * @param strictISO true to only allow ISO formats
     * @return true if reduced precision
     * @since 1.1
     */
    private static boolean dateByMonth(DateTimeFormatterBuilder bld, Collection<DateTimeFieldType> fields,
            boolean extended, boolean strictISO) {

        boolean reducedPrec = false;
        if (fields.remove(DateTimeFieldType.year())) {
            bld.append(Constants.YE);
            if (fields.remove(DateTimeFieldType.monthOfYear())) {
                if (fields.remove(DateTimeFieldType.dayOfMonth())) {
                    // YYYY-MM-DD/YYYYMMDD
                    appendSeparator(bld, extended);
                    bld.appendMonthOfYear(2);
                    appendSeparator(bld, extended);
                    bld.appendDayOfMonth(2);
                } else {
                    // YYYY-MM/YYYY-MM
                    bld.appendLiteral('-');
                    bld.appendMonthOfYear(2);
                    reducedPrec = true;
                }
            } else {
                if (fields.remove(DateTimeFieldType.dayOfMonth())) {
                    // YYYY--DD/YYYY--DD (non-iso)
                    checkNotStrictISO(fields, strictISO);
                    bld.appendLiteral('-');
                    bld.appendLiteral('-');
                    bld.appendDayOfMonth(2);
                } else {
                    // YYYY/YYYY
                    reducedPrec = true;
                }
            }

        } else if (fields.remove(DateTimeFieldType.monthOfYear())) {
            bld.appendLiteral('-');
            bld.appendLiteral('-');
            bld.appendMonthOfYear(2);
            if (fields.remove(DateTimeFieldType.dayOfMonth())) {
                // --MM-DD/--MMDD
                appendSeparator(bld, extended);
                bld.appendDayOfMonth(2);
            } else {
                // --MM/--MM
                reducedPrec = true;
            }
        } else if (fields.remove(DateTimeFieldType.dayOfMonth())) {
            // ---DD/---DD
            bld.appendLiteral('-');
            bld.appendLiteral('-');
            bld.appendLiteral('-');
            bld.appendDayOfMonth(2);
        }
        return reducedPrec;
    }

    // -----------------------------------------------------------------------
    /**
     * Creates a date using the ordinal date format. Specification reference: 5.2.2.
     *
     * @param bld      the builder
     * @param fields   the fields
     * @param extended true to use extended format
     * @since 1.1
     */
    private static boolean dateByOrdinal(DateTimeFormatterBuilder bld, Collection<DateTimeFieldType> fields,
            boolean extended) {

        boolean reducedPrec = false;
        if (fields.remove(DateTimeFieldType.year())) {
            bld.append(Constants.YE);
            if (fields.remove(DateTimeFieldType.dayOfYear())) {
                // YYYY-DDD/YYYYDDD
                appendSeparator(bld, extended);
                bld.appendDayOfYear(3);
            } else {
                // YYYY/YYYY
                reducedPrec = true;
            }

        } else if (fields.remove(DateTimeFieldType.dayOfYear())) {
            // -DDD/-DDD
            bld.appendLiteral('-');
            bld.appendDayOfYear(3);
        }
        return reducedPrec;
    }

    // -----------------------------------------------------------------------
    /**
     * Creates a date using the calendar date format. Specification reference:
     * 5.2.3.
     *
     * @param bld       the builder
     * @param fields    the fields
     * @param extended  true to use extended format
     * @param strictISO true to only allow ISO formats
     * @since 1.1
     */
    private static boolean dateByWeek(DateTimeFormatterBuilder bld, Collection<DateTimeFieldType> fields,
            boolean extended, boolean strictISO) {

        boolean reducedPrec = false;
        if (fields.remove(DateTimeFieldType.weekyear())) {
            bld.append(Constants.WE);
            if (fields.remove(DateTimeFieldType.weekOfWeekyear())) {
                appendSeparator(bld, extended);
                bld.appendLiteral('W');
                bld.appendWeekOfWeekyear(2);
                if (fields.remove(DateTimeFieldType.dayOfWeek())) {
                    // YYYY-WWW-D/YYYYWWWD
                    appendSeparator(bld, extended);
                    bld.appendDayOfWeek(1);
                } else {
                    // YYYY-WWW/YYYY-WWW
                    reducedPrec = true;
                }
            } else {
                if (fields.remove(DateTimeFieldType.dayOfWeek())) {
                    // YYYY-W-D/YYYYW-D (non-iso)
                    checkNotStrictISO(fields, strictISO);
                    appendSeparator(bld, extended);
                    bld.appendLiteral('W');
                    bld.appendLiteral('-');
                    bld.appendDayOfWeek(1);
                } else {
                    // YYYY/YYYY
                    reducedPrec = true;
                }
            }

        } else if (fields.remove(DateTimeFieldType.weekOfWeekyear())) {
            bld.appendLiteral('-');
            bld.appendLiteral('W');
            bld.appendWeekOfWeekyear(2);
            if (fields.remove(DateTimeFieldType.dayOfWeek())) {
                // -WWW-D/-WWWD
                appendSeparator(bld, extended);
                bld.appendDayOfWeek(1);
            } else {
                // -WWW/-WWW
                reducedPrec = true;
            }
        } else if (fields.remove(DateTimeFieldType.dayOfWeek())) {
            // -W-D/-W-D
            bld.appendLiteral('-');
            bld.appendLiteral('W');
            bld.appendLiteral('-');
            bld.appendDayOfWeek(1);
        }
        return reducedPrec;
    }

    // -----------------------------------------------------------------------
    /**
     * Adds the time fields to the builder. Specification reference: 5.3.1.
     *
     * @param bld         the builder
     * @param fields      the fields
     * @param extended    whether to use the extended format
     * @param strictISO   whether to be strict
     * @param reducedPrec whether the date was reduced precision
     * @param datePresent whether there was a date
     * @since 1.1
     */
    private static void time(DateTimeFormatterBuilder bld, Collection<DateTimeFieldType> fields, boolean extended,
            boolean strictISO, boolean reducedPrec, boolean datePresent) {

        boolean hour = fields.remove(DateTimeFieldType.hourOfDay());
        boolean minute = fields.remove(DateTimeFieldType.minuteOfHour());
        boolean second = fields.remove(DateTimeFieldType.secondOfMinute());
        boolean milli = fields.remove(DateTimeFieldType.millisOfSecond());
        if (!hour && !minute && !second && !milli) {
            return;
        }
        if (hour || minute || second || milli) {
            if (strictISO && reducedPrec) {
                throw new IllegalArgumentException(
                        "No valid ISO8601 format for fields because Date was reduced precision: " + fields);
            }
            if (datePresent) {
                bld.appendLiteral('T');
            }
        }
        if (hour && minute && second || (hour && !second && !milli)) {
            // OK - HMSm/HMS/HM/H - valid in combination with date
        } else {
            if (strictISO && datePresent) {
                throw new IllegalArgumentException(
                        "No valid ISO8601 format for fields because Time was truncated: " + fields);
            }
            if (!hour && (minute && second || (minute && !milli) || second)) {
                // OK - MSm/MS/M/Sm/S - valid ISO formats
            } else {
                if (strictISO) {
                    throw new IllegalArgumentException("No valid ISO8601 format for fields: " + fields);
                }
            }
        }
        if (hour) {
            bld.appendHourOfDay(2);
        } else if (minute || second || milli) {
            bld.appendLiteral('-');
        }
        if (extended && hour && minute) {
            bld.appendLiteral(':');
        }
        if (minute) {
            bld.appendMinuteOfHour(2);
        } else if (second || milli) {
            bld.appendLiteral('-');
        }
        if (extended && minute && second) {
            bld.appendLiteral(':');
        }
        if (second) {
            bld.appendSecondOfMinute(2);
        } else if (milli) {
            bld.appendLiteral('-');
        }
        if (milli) {
            bld.appendLiteral('.');
            bld.appendMillisOfSecond(3);
        }
    }

    // -----------------------------------------------------------------------
    /**
     * Checks that the iso only flag is not set, throwing an exception if it is.
     *
     * @param fields    the fields
     * @param strictISO true if only ISO formats allowed
     * @since 1.1
     */
    private static void checkNotStrictISO(Collection<DateTimeFieldType> fields, boolean strictISO) {
        if (strictISO) {
            throw new IllegalArgumentException("No valid ISO8601 format for fields: " + fields);
        }
    }

    /**
     * Appends the separator if necessary.
     *
     * @param bld      the builder
     * @param extended whether to append the separator
     * @since 1.1
     */
    private static void appendSeparator(DateTimeFormatterBuilder bld, boolean extended) {
        if (extended) {
            bld.appendLiteral('-');
        }
    }

    // -----------------------------------------------------------------------
    /**
     * Returns a generic ISO date parser for parsing dates with a possible zone.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * date              = date-element ['T' offset]
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
     * </pre>
     */
    public static DateTimeFormatter dateParser() {
        return Constants.DP;
    }

    /**
     * Returns a generic ISO date parser for parsing local dates.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * This parser is initialised with the local (UTC) time zone.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * </pre>
     *
     * @since 1.3
     */
    public static DateTimeFormatter localDateParser() {
        return Constants.LDP;
    }

    /**
     * Returns a generic ISO date parser for parsing dates.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * </pre>
     */
    public static DateTimeFormatter dateElementParser() {
        return Constants.DPE;
    }

    /**
     * Returns a generic ISO time parser for parsing times with a possible zone.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * The parser is strict by default, thus time string {@code 24:00} cannot be
     * parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * time           = ['T'] time-element [offset]
     * time-element   = HH [minute-element] | [fraction]
     * minute-element = ':' mm [second-element] | [fraction]
     * second-element = ':' ss [fraction]
     * fraction       = ('.' | ',') digit+
     * offset         = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
     * </pre>
     */
    public static DateTimeFormatter timeParser() {
        return Constants.TP;
    }

    /**
     * Returns a generic ISO time parser for parsing local times.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * This parser is initialised with the local (UTC) time zone. The parser is
     * strict by default, thus time string {@code 24:00} cannot be parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * time           = ['T'] time-element
     * time-element   = HH [minute-element] | [fraction]
     * minute-element = ':' mm [second-element] | [fraction]
     * second-element = ':' ss [fraction]
     * fraction       = ('.' | ',') digit+
     * </pre>
     *
     * @since 1.3
     */
    public static DateTimeFormatter localTimeParser() {
        return Constants.LTP;
    }

    /**
     * Returns a generic ISO time parser.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * The parser is strict by default, thus time string {@code 24:00} cannot be
     * parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * time-element   = HH [minute-element] | [fraction]
     * minute-element = ':' mm [second-element] | [fraction]
     * second-element = ':' ss [fraction]
     * fraction       = ('.' | ',') digit+
     * </pre>
     */
    public static DateTimeFormatter timeElementParser() {
        return Constants.TPE;
    }

    /**
     * Returns a generic ISO datetime parser which parses either a date or a time or
     * both.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * The parser is strict by default, thus time string {@code 24:00} cannot be
     * parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * datetime          = time | date-opt-time
     * time              = 'T' time-element [offset]
     * date-opt-time     = date-element ['T' [time-element] [offset]]
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * time-element      = HH [minute-element] | [fraction]
     * minute-element    = ':' mm [second-element] | [fraction]
     * second-element    = ':' ss [fraction]
     * fraction          = ('.' | ',') digit+
     * offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
     * </pre>
     */
    public static DateTimeFormatter dateTimeParser() {
        return Constants.DTP;
    }

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the
     * time is optional.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * This parser can parse zoned datetimes. The parser is strict by default, thus
     * time string {@code 24:00} cannot be parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * date-opt-time     = date-element ['T' [time-element] [offset]]
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * time-element      = HH [minute-element] | [fraction]
     * minute-element    = ':' mm [second-element] | [fraction]
     * second-element    = ':' ss [fraction]
     * fraction          = ('.' | ',') digit+
     * </pre>
     *
     * @since 1.3
     */
    public static DateTimeFormatter dateOptionalTimeParser() {
        return Constants.DOTP;
    }

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the
     * time is optional.
     * <p>
     * The returned formatter can only be used for parsing, printing is unsupported.
     * <p>
     * This parser only parses local datetimes. This parser is initialised with the
     * local (UTC) time zone. The parser is strict by default, thus time string
     * {@code 24:00} cannot be parsed.
     * <p>
     * It accepts formats described by the following syntax:
     *
     * <pre>
     * datetime          = date-element ['T' time-element]
     * date-element      = std-date-element | ord-date-element | week-date-element
     * std-date-element  = yyyy ['-' MM ['-' dd]]
     * ord-date-element  = yyyy ['-' DDD]
     * week-date-element = xxxx '-W' ww ['-' e]
     * time-element      = HH [minute-element] | [fraction]
     * minute-element    = ':' mm [second-element] | [fraction]
     * second-element    = ':' ss [fraction]
     * fraction          = ('.' | ',') digit+
     * </pre>
     *
     * @since 1.3
     */
    public static DateTimeFormatter localDateOptionalTimeParser() {
        return Constants.LDOTP;
    }

    // -----------------------------------------------------------------------
    /**
     * Returns a formatter for a full date as four digit year, two digit month of
     * year, and two digit day of month (yyyy-MM-dd).
     * <p>
     * The returned formatter prints and parses only this format. See
     * {@link #dateParser()} for a more flexible parser that accepts different
     * formats.
     *
     * @return a formatter for yyyy-MM-dd
     */
    public static DateTimeFormatter date() {
        return yearMonthDay();
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, three digit fraction of second, and time zone
     * offset (HH:mm:ss.SSSZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds. See {@link #timeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for HH:mm:ss.SSSZZ
     */
    public static DateTimeFormatter time() {
        return Constants.T;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, and time zone offset (HH:mm:ssZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds. See {@link #timeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for HH:mm:ssZZ
     */
    public static DateTimeFormatter timeNoMillis() {
        return Constants.TX;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, three digit fraction of second, and time zone
     * offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds. See {@link #timeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for 'T'HH:mm:ss.SSSZZ
     */
    public static DateTimeFormatter tTime() {
        return Constants.TT;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, and time zone offset prefixed by 'T'
     * ('T'HH:mm:ssZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds. See {@link #timeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for 'T'HH:mm:ssZZ
     */
    public static DateTimeFormatter tTimeNoMillis() {
        return Constants.TTX;
    }

    /**
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (yyyy-MM-dd'T'HH:mm:ss.SSSZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm:ss.SSSZZ
     */
    public static DateTimeFormatter dateTime() {
        return Constants.DT;
    }

    /**
     * Returns a formatter that combines a full date and time without millis,
     * separated by a 'T' (yyyy-MM-dd'T'HH:mm:ssZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm:ssZZ
     */
    public static DateTimeFormatter dateTimeNoMillis() {
        return Constants.DTX;
    }

    /**
     * Returns a formatter for a full ordinal date, using a four digit year and
     * three digit dayOfYear (yyyy-DDD).
     * <p>
     * The returned formatter prints and parses only this format. See
     * {@link #dateParser()} for a more flexible parser that accepts different
     * formats.
     *
     * @return a formatter for yyyy-DDD
     * @since 1.1
     */
    public static DateTimeFormatter ordinalDate() {
        return Constants.OD;
    }

    /**
     * Returns a formatter for a full ordinal date and time, using a four digit year
     * and three digit dayOfYear (yyyy-DDD'T'HH:mm:ss.SSSZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for yyyy-DDD'T'HH:mm:ss.SSSZZ
     * @since 1.1
     */
    public static DateTimeFormatter ordinalDateTime() {
        return Constants.ODT;
    }

    /**
     * Returns a formatter for a full ordinal date and time without millis, using a
     * four digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ssZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for yyyy-DDD'T'HH:mm:ssZZ
     * @since 1.1
     */
    public static DateTimeFormatter ordinalDateTimeNoMillis() {
        return Constants.ODTX;
    }

    /**
     * Returns a formatter for a full date as four digit weekyear, two digit week of
     * weekyear, and one digit day of week (xxxx-'W'ww-e).
     * <p>
     * The returned formatter prints and parses only this format. See
     * {@link #dateParser()} for a more flexible parser that accepts different
     * formats.
     *
     * @return a formatter for xxxx-'W'ww-e
     */
    public static DateTimeFormatter weekDate() {
        return Constants.WWD;
    }

    /**
     * Returns a formatter that combines a full weekyear date and time, separated by
     * a 'T' (xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ
     */
    public static DateTimeFormatter weekDateTime() {
        return Constants.WDT;
    }

    /**
     * Returns a formatter that combines a full weekyear date and time without
     * millis, separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ssZZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HH:mm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds. See {@link #dateTimeParser()} for a more flexible parser that
     * accepts different formats.
     *
     * @return a formatter for xxxx-'W'ww-e'T'HH:mm:ssZZ
     */
    public static DateTimeFormatter weekDateTimeNoMillis() {
        return Constants.WDTX;
    }

    // -----------------------------------------------------------------------
    /**
     * Returns a basic formatter for a full date as four digit year, two digit month
     * of year, and two digit day of month (yyyyMMdd).
     * <p>
     * The returned formatter prints and parses only this format.
     *
     * @return a formatter for yyyyMMdd
     */
    public static DateTimeFormatter basicDate() {
        return Constants.BD;
    }

    /**
     * Returns a basic formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit millis, and time zone offset
     * (HHmmss.SSSZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds.
     *
     * @return a formatter for HHmmss.SSSZ
     */
    public static DateTimeFormatter basicTime() {
        return Constants.BT;
    }

    /**
     * Returns a basic formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset (HHmmssZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds.
     *
     * @return a formatter for HHmmssZ
     */
    public static DateTimeFormatter basicTimeNoMillis() {
        return Constants.BTX;
    }

    /**
     * Returns a basic formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit millis, and time zone offset
     * prefixed by 'T' ('T'HHmmss.SSSZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds.
     *
     * @return a formatter for 'T'HHmmss.SSSZ
     */
    public static DateTimeFormatter basicTTime() {
        return Constants.BTT;
    }

    /**
     * Returns a basic formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset prefixed by 'T'
     * ('T'HHmmssZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds.
     *
     * @return a formatter for 'T'HHmmssZ
     */
    public static DateTimeFormatter basicTTimeNoMillis() {
        return Constants.BTTX;
    }

    /**
     * Returns a basic formatter that combines a basic date and time, separated by a
     * 'T' (yyyyMMdd'T'HHmmss.SSSZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds.
     *
     * @return a formatter for yyyyMMdd'T'HHmmss.SSSZ
     */
    public static DateTimeFormatter basicDateTime() {
        return Constants.BDT;
    }

    /**
     * Returns a basic formatter that combines a basic date and time without millis,
     * separated by a 'T' (yyyyMMdd'T'HHmmssZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds.
     *
     * @return a formatter for yyyyMMdd'T'HHmmssZ
     */
    public static DateTimeFormatter basicDateTimeNoMillis() {
        return Constants.BDTX;
    }

    /**
     * Returns a formatter for a full ordinal date, using a four digit year and
     * three digit dayOfYear (yyyyDDD).
     * <p>
     * The returned formatter prints and parses only this format.
     *
     * @return a formatter for yyyyDDD
     * @since 1.1
     */
    public static DateTimeFormatter basicOrdinalDate() {
        return Constants.BOD;
    }

    /**
     * Returns a formatter for a full ordinal date and time, using a four digit year
     * and three digit dayOfYear (yyyyDDD'T'HHmmss.SSSZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds.
     *
     * @return a formatter for yyyyDDD'T'HHmmss.SSSZ
     * @since 1.1
     */
    public static DateTimeFormatter basicOrdinalDateTime() {
        return Constants.BODT;
    }

    /**
     * Returns a formatter for a full ordinal date and time without millis, using a
     * four digit year and three digit dayOfYear (yyyyDDD'T'HHmmssZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds.
     *
     * @return a formatter for yyyyDDD'T'HHmmssZ
     * @since 1.1
     */
    public static DateTimeFormatter basicOrdinalDateTimeNoMillis() {
        return Constants.BODTX;
    }

    /**
     * Returns a basic formatter for a full date as four digit weekyear, two digit
     * week of weekyear, and one digit day of week (xxxx'W'wwe).
     * <p>
     * The returned formatter prints and parses only this format.
     *
     * @return a formatter for xxxx'W'wwe
     */
    public static DateTimeFormatter basicWeekDate() {
        return Constants.BWD;
    }

    /**
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmss.SSSZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which includes
     * milliseconds.
     *
     * @return a formatter for xxxx'W'wwe'T'HHmmss.SSSZ
     */
    public static DateTimeFormatter basicWeekDateTime() {
        return Constants.BWDT;
    }

    /**
     * Returns a basic formatter that combines a basic weekyear date and time
     * without millis, separated by a 'T' (xxxx'W'wwe'T'HHmmssZ).
     * <p>
     * The time zone offset is 'Z' for zero, and of the form '\u00b1HHmm' for
     * non-zero. The parser is strict by default, thus time string {@code 24:00}
     * cannot be parsed.
     * <p>
     * The returned formatter prints and parses only this format, which excludes
     * milliseconds.
     *
     * @return a formatter for xxxx'W'wwe'T'HHmmssZ
     */
    public static DateTimeFormatter basicWeekDateTimeNoMillis() {
        return Constants.BWDTX;
    }

    // -----------------------------------------------------------------------
    /**
     * Returns a formatter for a four digit year. (yyyy)
     *
     * @return a formatter for yyyy
     */
    public static DateTimeFormatter year() {
        return Constants.YE;
    }

    /**
     * Returns a formatter for a four digit year and two digit month of year.
     * (yyyy-MM)
     *
     * @return a formatter for yyyy-MM
     */
    public static DateTimeFormatter yearMonth() {
        return Constants.YM;
    }

    /**
     * Returns a formatter for a four digit year, two digit month of year, and two
     * digit day of month. (yyyy-MM-dd)
     *
     * @return a formatter for yyyy-MM-dd
     */
    public static DateTimeFormatter yearMonthDay() {
        return Constants.YMD;
    }

    /**
     * Returns a formatter for a four digit weekyear. (xxxx)
     *
     * @return a formatter for xxxx
     */
    public static DateTimeFormatter weekyear() {
        return Constants.WE;
    }

    /**
     * Returns a formatter for a four digit weekyear and two digit week of weekyear.
     * (xxxx-'W'ww)
     *
     * @return a formatter for xxxx-'W'ww
     */
    public static DateTimeFormatter weekyearWeek() {
        return Constants.WW;
    }

    /**
     * Returns a formatter for a four digit weekyear, two digit week of weekyear,
     * and one digit day of week. (xxxx-'W'ww-e)
     *
     * @return a formatter for xxxx-'W'ww-e
     */
    public static DateTimeFormatter weekyearWeekDay() {
        return Constants.WWD;
    }

    /**
     * Returns a formatter for a two digit hour of day. (HH)
     *
     * @return a formatter for HH
     */
    public static DateTimeFormatter hour() {
        return Constants.HDE;
    }

    /**
     * Returns a formatter for a two digit hour of day and two digit minute of hour.
     * (HH:mm)
     *
     * @return a formatter for HH:mm
     */
    public static DateTimeFormatter hourMinute() {
        return Constants.HM;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * and two digit second of minute. (HH:mm:ss)
     *
     * @return a formatter for HH:mm:ss
     */
    public static DateTimeFormatter hourMinuteSecond() {
        return Constants.HMS;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, and three digit fraction of second
     * (HH:mm:ss.SSS). Parsing will parse up to 3 fractional second digits.
     *
     * @return a formatter for HH:mm:ss.SSS
     */
    public static DateTimeFormatter hourMinuteSecondMillis() {
        return Constants.HMSL;
    }

    /**
     * Returns a formatter for a two digit hour of day, two digit minute of hour,
     * two digit second of minute, and three digit fraction of second
     * (HH:mm:ss.SSS). Parsing will parse up to 9 fractional second digits, throwing
     * away all except the first three.
     *
     * @return a formatter for HH:mm:ss.SSS
     */
    public static DateTimeFormatter hourMinuteSecondFraction() {
        return Constants.HMSF;
    }

    /**
     * Returns a formatter that combines a full date and two digit hour of day.
     * (yyyy-MM-dd'T'HH)
     *
     * @return a formatter for yyyy-MM-dd'T'HH
     */
    public static DateTimeFormatter dateHour() {
        return Constants.DH;
    }

    /**
     * Returns a formatter that combines a full date, two digit hour of day, and two
     * digit minute of hour. (yyyy-MM-dd'T'HH:mm)
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm
     */
    public static DateTimeFormatter dateHourMinute() {
        return Constants.DHM;
    }

    /**
     * Returns a formatter that combines a full date, two digit hour of day, two
     * digit minute of hour, and two digit second of minute. (yyyy-MM-dd'T'HH:mm:ss)
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm:ss
     */
    public static DateTimeFormatter dateHourMinuteSecond() {
        return Constants.DHMS;
    }

    /**
     * Returns a formatter that combines a full date, two digit hour of day, two
     * digit minute of hour, two digit second of minute, and three digit fraction of
     * second (yyyy-MM-dd'T'HH:mm:ss.SSS). Parsing will parse up to 3 fractional
     * second digits.
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm:ss.SSS
     */
    public static DateTimeFormatter dateHourMinuteSecondMillis() {
        return Constants.DHMSL;
    }

    /**
     * Returns a formatter that combines a full date, two digit hour of day, two
     * digit minute of hour, two digit second of minute, and three digit fraction of
     * second (yyyy-MM-dd'T'HH:mm:ss.SSS). Parsing will parse up to 9 fractional
     * second digits, throwing away all except the first three.
     *
     * @return a formatter for yyyy-MM-dd'T'HH:mm:ss.SSS
     */
    public static DateTimeFormatter dateHourMinuteSecondFraction() {
        return Constants.DHMSF;
    }

    // -----------------------------------------------------------------------
    static final class Constants {

        private static final DateTimeFormatter YE = yearElement(); // year element (yyyy)
        private static final DateTimeFormatter MYE = monthElement(); // monthOfYear element (-MM)
        private static final DateTimeFormatter DME = dayOfMonthElement(); // dayOfMonth element (-dd)
        private static final DateTimeFormatter WE = weekyearElement(); // weekyear element (xxxx)
        private static final DateTimeFormatter WWE = weekElement(); // weekOfWeekyear element (-ww)
        private static final DateTimeFormatter DWE = dayOfWeekElement(); // dayOfWeek element (-ee)
        private static final DateTimeFormatter DYE = dayOfYearElement(); // dayOfYear element (-DDD)
        private static final DateTimeFormatter HDE = hourElement(); // hourOfDay element (HH)
        private static final DateTimeFormatter MHE = minuteElement(); // minuteOfHour element (:mm)
        private static final DateTimeFormatter SME = secondElement(); // secondOfMinute element (:ss)
        private static final DateTimeFormatter FSE = fractionElement(); // fractionOfSecond element (.SSSSSSSSS)
        private static final DateTimeFormatter ZE = offsetElement(); // zone offset element
        private static final DateTimeFormatter LTE = literalTElement(); // literal 'T' element

        private static final DateTimeFormatter YM = yearMonth(); // year month
        private static final DateTimeFormatter YMD = yearMonthDay(); // year month day

        private static final DateTimeFormatter WW = weekyearWeek(); // weekyear week
        private static final DateTimeFormatter WWD = weekyearWeekDay(); // weekyear week day

        private static final DateTimeFormatter HM = hourMinute(); // hour minute
        private static final DateTimeFormatter HMS = hourMinuteSecond(); // hour minute second
        private static final DateTimeFormatter HMSL = hourMinuteSecondMillis(); // hour minute second millis
        private static final DateTimeFormatter HMSF = hourMinuteSecondFraction(); // hour minute second fraction

        private static final DateTimeFormatter DH = dateHour(); // date hour
        private static final DateTimeFormatter DHM = dateHourMinute(); // date hour minute
        private static final DateTimeFormatter DHMS = dateHourMinuteSecond(); // date hour minute second
        private static final DateTimeFormatter DHMSL = dateHourMinuteSecondMillis(); // date hour minute second millis
        private static final DateTimeFormatter DHMSF = dateHourMinuteSecondFraction(); // date hour minute second fraction

        private static final DateTimeFormatter T = time(); // time
        private static final DateTimeFormatter TX = timeNoMillis(); // time no millis
        private static final DateTimeFormatter TT = tTime(); // Ttime
        private static final DateTimeFormatter TTX = tTimeNoMillis(); // Ttime no millis
        private static final DateTimeFormatter DT = dateTime(); // date time
        private static final DateTimeFormatter DTX = dateTimeNoMillis(); // date time no millis

        private static final DateTimeFormatter WDT = weekDateTime(); // week date time
        private static final DateTimeFormatter WDTX = weekDateTimeNoMillis(); // week date time no millis

        private static final DateTimeFormatter OD = ordinalDate(); // ordinal date (same as yd)
        private static final DateTimeFormatter ODT = ordinalDateTime(); // ordinal date time
        private static final DateTimeFormatter ODTX = ordinalDateTimeNoMillis(); // ordinal date time no millis

        private static final DateTimeFormatter BD = basicDate(); // basic date
        private static final DateTimeFormatter BT = basicTime(); // basic time
        private static final DateTimeFormatter BTX = basicTimeNoMillis(); // basic time no millis
        private static final DateTimeFormatter BTT = basicTTime(); // basic Ttime
        private static final DateTimeFormatter BTTX = basicTTimeNoMillis(); // basic Ttime no millis
        private static final DateTimeFormatter BDT = basicDateTime(); // basic date time
        private static final DateTimeFormatter BDTX = basicDateTimeNoMillis(); // basic date time no millis

        private static final DateTimeFormatter BOD = basicOrdinalDate(); // basic ordinal date
        private static final DateTimeFormatter BODT = basicOrdinalDateTime(); // basic ordinal date time
        private static final DateTimeFormatter BODTX = basicOrdinalDateTimeNoMillis(); // basic ordinal date time no millis

        private static final DateTimeFormatter BWD = basicWeekDate(); // basic week date
        private static final DateTimeFormatter BWDT = basicWeekDateTime(); // basic week date time
        private static final DateTimeFormatter BWDTX = basicWeekDateTimeNoMillis(); // basic week date time no millis

        private static final DateTimeFormatter DPE = dateElementParser(); // date parser element
        private static final DateTimeFormatter TPE = timeElementParser(); // time parser element
        private static final DateTimeFormatter DP = dateParser(); // date parser
        private static final DateTimeFormatter LDP = localDateParser(); // local date parser
        private static final DateTimeFormatter TP = timeParser(); // time parser
        private static final DateTimeFormatter LTP = localTimeParser(); // local time parser
        private static final DateTimeFormatter DTP = dateTimeParser(); // date time parser
        private static final DateTimeFormatter DOTP = dateOptionalTimeParser(); // date optional time parser
        private static final DateTimeFormatter LDOTP = localDateOptionalTimeParser(); // local date optional time parser

        // -----------------------------------------------------------------------
        private static DateTimeFormatter dateParser() {
            if (DP == null) {
                DateTimeParser tOffset = new DateTimeFormatterBuilder().appendLiteral('T').append(offsetElement())
                        .toParser();
                return new DateTimeFormatterBuilder().append(dateElementParser()).appendOptional(tOffset).toFormatter();
            }
            return DP;
        }

        private static DateTimeFormatter localDateParser() {
            if (LDP == null) {
                return dateElementParser().withZoneUTC();
            }
            return LDP;
        }

        private static DateTimeFormatter dateElementParser() {
            if (DPE == null) {
                return new DateTimeFormatterBuilder().append(null, new DateTimeParser[] {
                        new DateTimeFormatterBuilder().append(yearElement())
                                .appendOptional(new DateTimeFormatterBuilder().append(monthElement())
                                        .appendOptional(dayOfMonthElement().getParser()).toParser())
                                .toParser(),
                        new DateTimeFormatterBuilder().append(weekyearElement()).append(weekElement())
                                .appendOptional(dayOfWeekElement().getParser()).toParser(),
                        new DateTimeFormatterBuilder().append(yearElement()).append(dayOfYearElement()).toParser() })
                        .toFormatter();
            }
            return DPE;
        }

        private static DateTimeFormatter timeParser() {
            if (TP == null) {
                return new DateTimeFormatterBuilder().appendOptional(literalTElement().getParser())
                        .append(timeElementParser()).appendOptional(offsetElement().getParser()).toFormatter();
            }
            return TP;
        }

        private static DateTimeFormatter localTimeParser() {
            if (LTP == null) {
                return new DateTimeFormatterBuilder().appendOptional(literalTElement().getParser())
                        .append(timeElementParser()).toFormatter().withZoneUTC();
            }
            return LTP;
        }

        private static DateTimeFormatter timeElementParser() {
            if (TPE == null) {
                // Decimal point can be either '.' or ','
                DateTimeParser decimalPoint = new DateTimeFormatterBuilder().append(null,
                        new DateTimeParser[] { new DateTimeFormatterBuilder().appendLiteral('.').toParser(),
                                new DateTimeFormatterBuilder().appendLiteral(',').toParser() })
                        .toParser();

                return new DateTimeFormatterBuilder()
                    // time-element
                    .append(hourElement())
                    .append(null, new DateTimeParser[] { new DateTimeFormatterBuilder()
                        // minute-element
                        .append(minuteElement())
                        .append(
                            null,
                            new DateTimeParser[] {
                                new DateTimeFormatterBuilder()
                                    // second-element
                                    .append(secondElement())
                                    // second fraction
                                    .appendOptional(new DateTimeFormatterBuilder()
                                        .append(decimalPoint)
                                        .appendFractionOfSecond(1, 9).toParser()
                                    )
                                    .toParser(),

                                // minute fraction
                                new DateTimeFormatterBuilder()
                                    .append(decimalPoint)
                                    .appendFractionOfMinute(1, 9)
                                    .toParser(),

                                null
                            }
                        )
                        .toParser(),
                        // hour fraction
                        new DateTimeFormatterBuilder().append(decimalPoint).appendFractionOfHour(1, 9).toParser(),
                        null
                    })
                    .toFormatter();
            }
            return TPE;
        }

        private static DateTimeFormatter dateTimeParser() {
            if (DTP == null) {
                // This is different from the general time parser in that the 'T'
                // is required.
                DateTimeParser time = new DateTimeFormatterBuilder().appendLiteral('T').append(timeElementParser())
                        .appendOptional(offsetElement().getParser()).toParser();
                return new DateTimeFormatterBuilder()
                        .append(null, new DateTimeParser[] { time, dateOptionalTimeParser().getParser() })
                        .toFormatter();
            }
            return DTP;
        }

        private static DateTimeFormatter dateOptionalTimeParser() {
            if (DOTP == null) {
                DateTimeParser timeOrOffset = new DateTimeFormatterBuilder().appendLiteral('T')
                        .appendOptional(timeElementParser().getParser()).appendOptional(offsetElement().getParser())
                        .toParser();
                return new DateTimeFormatterBuilder().append(dateElementParser()).appendOptional(timeOrOffset)
                        .toFormatter();
            }
            return DOTP;
        }

        private static DateTimeFormatter localDateOptionalTimeParser() {
            if (LDOTP == null) {
                DateTimeParser time = new DateTimeFormatterBuilder().appendLiteral('T').append(timeElementParser())
                        .toParser();
                return new DateTimeFormatterBuilder().append(dateElementParser()).appendOptional(time).toFormatter()
                        .withZoneUTC();
            }
            return LDOTP;
        }

        // -----------------------------------------------------------------------
        private static DateTimeFormatter time() {
            if (T == null) {
                return new DateTimeFormatterBuilder().append(hourMinuteSecondFraction()).append(offsetElement())
                        .toFormatter();
            }
            return T;
        }

        private static DateTimeFormatter timeNoMillis() {
            if (TX == null) {
                return new DateTimeFormatterBuilder().append(hourMinuteSecond()).append(offsetElement()).toFormatter();
            }
            return TX;
        }

        private static DateTimeFormatter tTime() {
            if (TT == null) {
                return new DateTimeFormatterBuilder().append(literalTElement()).append(time()).toFormatter();
            }
            return TT;
        }

        private static DateTimeFormatter tTimeNoMillis() {
            if (TTX == null) {
                return new DateTimeFormatterBuilder().append(literalTElement()).append(timeNoMillis()).toFormatter();
            }
            return TTX;
        }

        private static DateTimeFormatter dateTime() {
            if (DT == null) {
                return new DateTimeFormatterBuilder().append(date()).append(tTime()).toFormatter();
            }
            return DT;
        }

        private static DateTimeFormatter dateTimeNoMillis() {
            if (DTX == null) {
                return new DateTimeFormatterBuilder().append(date()).append(tTimeNoMillis()).toFormatter();
            }
            return DTX;
        }

        private static DateTimeFormatter ordinalDate() {
            if (OD == null) {
                return new DateTimeFormatterBuilder().append(yearElement()).append(dayOfYearElement()).toFormatter();
            }
            return OD;
        }

        private static DateTimeFormatter ordinalDateTime() {
            if (ODT == null) {
                return new DateTimeFormatterBuilder().append(ordinalDate()).append(tTime()).toFormatter();
            }
            return ODT;
        }

        private static DateTimeFormatter ordinalDateTimeNoMillis() {
            if (ODTX == null) {
                return new DateTimeFormatterBuilder().append(ordinalDate()).append(tTimeNoMillis()).toFormatter();
            }
            return ODTX;
        }

        private static DateTimeFormatter weekDateTime() {
            if (WDT == null) {
                return new DateTimeFormatterBuilder().append(weekDate()).append(tTime()).toFormatter();
            }
            return WDT;
        }

        private static DateTimeFormatter weekDateTimeNoMillis() {
            if (WDTX == null) {
                return new DateTimeFormatterBuilder().append(weekDate()).append(tTimeNoMillis()).toFormatter();
            }
            return WDTX;
        }

        // -----------------------------------------------------------------------
        private static DateTimeFormatter basicDate() {
            if (BD == null) {
                return new DateTimeFormatterBuilder().appendYear(4, 4)
                        .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
                        .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2).toFormatter();
            }
            return BD;
        }

        private static DateTimeFormatter basicTime() {
            if (BT == null) {
                return new DateTimeFormatterBuilder().appendFixedDecimal(DateTimeFieldType.hourOfDay(), 2)
                        .appendFixedDecimal(DateTimeFieldType.minuteOfHour(), 2)
                        .appendFixedDecimal(DateTimeFieldType.secondOfMinute(), 2).appendLiteral('.')
                        .appendFractionOfSecond(3, 9).appendTimeZoneOffset("Z", false, 2, 2).toFormatter();
            }
            return BT;
        }

        private static DateTimeFormatter basicTimeNoMillis() {
            if (BTX == null) {
                return new DateTimeFormatterBuilder().appendFixedDecimal(DateTimeFieldType.hourOfDay(), 2)
                        .appendFixedDecimal(DateTimeFieldType.minuteOfHour(), 2)
                        .appendFixedDecimal(DateTimeFieldType.secondOfMinute(), 2)
                        .appendTimeZoneOffset("Z", false, 2, 2).toFormatter();
            }
            return BTX;
        }

        private static DateTimeFormatter basicTTime() {
            if (BTT == null) {
                return new DateTimeFormatterBuilder().append(literalTElement()).append(basicTime()).toFormatter();
            }
            return BTT;
        }

        private static DateTimeFormatter basicTTimeNoMillis() {
            if (BTTX == null) {
                return new DateTimeFormatterBuilder().append(literalTElement()).append(basicTimeNoMillis())
                        .toFormatter();
            }
            return BTTX;
        }

        private static DateTimeFormatter basicDateTime() {
            if (BDT == null) {
                return new DateTimeFormatterBuilder().append(basicDate()).append(basicTTime()).toFormatter();
            }
            return BDT;
        }

        private static DateTimeFormatter basicDateTimeNoMillis() {
            if (BDTX == null) {
                return new DateTimeFormatterBuilder().append(basicDate()).append(basicTTimeNoMillis()).toFormatter();
            }
            return BDTX;
        }

        private static DateTimeFormatter basicOrdinalDate() {
            if (BOD == null) {
                return new DateTimeFormatterBuilder().appendYear(4, 4)
                        .appendFixedDecimal(DateTimeFieldType.dayOfYear(), 3).toFormatter();
            }
            return BOD;
        }

        private static DateTimeFormatter basicOrdinalDateTime() {
            if (BODT == null) {
                return new DateTimeFormatterBuilder().append(basicOrdinalDate()).append(basicTTime()).toFormatter();
            }
            return BODT;
        }

        private static DateTimeFormatter basicOrdinalDateTimeNoMillis() {
            if (BODTX == null) {
                return new DateTimeFormatterBuilder().append(basicOrdinalDate()).append(basicTTimeNoMillis())
                        .toFormatter();
            }
            return BODTX;
        }

        private static DateTimeFormatter basicWeekDate() {
            if (BWD == null) {
                // ES change, was .appendWeekyear(4, 4)
                return new DateTimeFormatterBuilder()
                    .appendFixedSignedDecimal(DateTimeFieldType.weekyear(), 4)
                    .appendLiteral('W')
                    .appendFixedDecimal(DateTimeFieldType.weekOfWeekyear(), 2)
                    .appendFixedDecimal(DateTimeFieldType.dayOfWeek(), 1)
                    .toFormatter();
            }
            return BWD;
        }

        private static DateTimeFormatter basicWeekDateTime() {
            if (BWDT == null) {
                return new DateTimeFormatterBuilder().append(basicWeekDate()).append(basicTTime()).toFormatter();
            }
            return BWDT;
        }

        private static DateTimeFormatter basicWeekDateTimeNoMillis() {
            if (BWDTX == null) {
                return new DateTimeFormatterBuilder().append(basicWeekDate()).append(basicTTimeNoMillis())
                        .toFormatter();
            }
            return BWDTX;
        }

        // -----------------------------------------------------------------------
        private static DateTimeFormatter yearMonth() {
            if (YM == null) {
                return new DateTimeFormatterBuilder().append(yearElement()).append(monthElement()).toFormatter();
            }
            return YM;
        }

        private static DateTimeFormatter yearMonthDay() {
            if (YMD == null) {
                return new DateTimeFormatterBuilder().append(yearElement()).append(monthElement())
                        .append(dayOfMonthElement()).toFormatter();
            }
            return YMD;
        }

        private static DateTimeFormatter weekyearWeek() {
            if (WW == null) {
                return new DateTimeFormatterBuilder().append(weekyearElement()).append(weekElement()).toFormatter();
            }
            return WW;
        }

        private static DateTimeFormatter weekyearWeekDay() {
            if (WWD == null) {
                return new DateTimeFormatterBuilder().append(weekyearElement()).append(weekElement())
                        .append(dayOfWeekElement()).toFormatter();
            }
            return WWD;
        }

        private static DateTimeFormatter hourMinute() {
            if (HM == null) {
                return new DateTimeFormatterBuilder().append(hourElement()).append(minuteElement()).toFormatter();
            }
            return HM;
        }

        private static DateTimeFormatter hourMinuteSecond() {
            if (HMS == null) {
                return new DateTimeFormatterBuilder().append(hourElement()).append(minuteElement())
                        .append(secondElement()).toFormatter();
            }
            return HMS;
        }

        private static DateTimeFormatter hourMinuteSecondMillis() {
            if (HMSL == null) {
                return new DateTimeFormatterBuilder().append(hourElement()).append(minuteElement())
                        .append(secondElement()).appendLiteral('.').appendFractionOfSecond(3, 3).toFormatter();
            }
            return HMSL;
        }

        private static DateTimeFormatter hourMinuteSecondFraction() {
            if (HMSF == null) {
                return new DateTimeFormatterBuilder().append(hourElement()).append(minuteElement())
                        .append(secondElement()).append(fractionElement()).toFormatter();
            }
            return HMSF;
        }

        private static DateTimeFormatter dateHour() {
            if (DH == null) {
                return new DateTimeFormatterBuilder().append(date()).append(literalTElement()).append(hour())
                        .toFormatter();
            }
            return DH;
        }

        private static DateTimeFormatter dateHourMinute() {
            if (DHM == null) {
                return new DateTimeFormatterBuilder().append(date()).append(literalTElement()).append(hourMinute())
                        .toFormatter();
            }
            return DHM;
        }

        private static DateTimeFormatter dateHourMinuteSecond() {
            if (DHMS == null) {
                return new DateTimeFormatterBuilder().append(date()).append(literalTElement())
                        .append(hourMinuteSecond()).toFormatter();
            }
            return DHMS;
        }

        private static DateTimeFormatter dateHourMinuteSecondMillis() {
            if (DHMSL == null) {
                return new DateTimeFormatterBuilder().append(date()).append(literalTElement())
                        .append(hourMinuteSecondMillis()).toFormatter();
            }
            return DHMSL;
        }

        private static DateTimeFormatter dateHourMinuteSecondFraction() {
            if (DHMSF == null) {
                return new DateTimeFormatterBuilder().append(date()).append(literalTElement())
                        .append(hourMinuteSecondFraction()).toFormatter();
            }
            return DHMSF;
        }

        // -----------------------------------------------------------------------
        private static DateTimeFormatter yearElement() {
            if (YE == null) {
                return new DateTimeFormatterBuilder()
                    .appendFixedSignedDecimal(DateTimeFieldType.year(), 4)
                    .toFormatter();
            }
            return YE;
        }

        private static DateTimeFormatter monthElement() {
            if (MYE == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral('-')
                    .appendFixedSignedDecimal(DateTimeFieldType.monthOfYear(), 2)
                    .toFormatter();
            }
            return MYE;
        }

        private static DateTimeFormatter dayOfMonthElement() {
            if (DME == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral('-')
                    .appendFixedSignedDecimal(DateTimeFieldType.dayOfMonth(), 2)
                    .toFormatter();
            }
            return DME;
        }

        private static DateTimeFormatter weekyearElement() {
            if (WE == null) {
                return new DateTimeFormatterBuilder()
                    .appendFixedSignedDecimal(DateTimeFieldType.weekyear(), 4)
                    .toFormatter();
            }
            return WE;
        }

        private static DateTimeFormatter weekElement() {
            if (WWE == null) {
                return new DateTimeFormatterBuilder().appendLiteral("-W")
                    .appendFixedSignedDecimal(DateTimeFieldType.weekOfWeekyear(), 2)
                    .toFormatter();
            }
            return WWE;
        }

        private static DateTimeFormatter dayOfWeekElement() {
            if (DWE == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral('-')
                    .appendDayOfWeek(1)
                    .toFormatter();
            }
            return DWE;
        }

        private static DateTimeFormatter dayOfYearElement() {
            if (DYE == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral('-')
                    .appendFixedSignedDecimal(DateTimeFieldType.dayOfYear(), 3)
                    .toFormatter();
            }
            return DYE;
        }

        private static DateTimeFormatter literalTElement() {
            if (LTE == null) {
                return new DateTimeFormatterBuilder().appendLiteral('T').toFormatter();
            }
            return LTE;
        }

        private static DateTimeFormatter hourElement() {
            if (HDE == null) {
                return new DateTimeFormatterBuilder()
                    .appendFixedSignedDecimal(DateTimeFieldType.hourOfDay(), 2)
                    .toFormatter();
            }
            return HDE;
        }

        private static DateTimeFormatter minuteElement() {
            if (MHE == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral(':')
                    .appendFixedSignedDecimal(DateTimeFieldType.minuteOfHour(), 2)
                    .toFormatter();
            }
            return MHE;
        }

        private static DateTimeFormatter secondElement() {
            if (SME == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral(':')
                    .appendFixedSignedDecimal(DateTimeFieldType.secondOfMinute(), 2)
                    .toFormatter();
            }
            return SME;
        }

        private static DateTimeFormatter fractionElement() {
            if (FSE == null) {
                return new DateTimeFormatterBuilder()
                    .appendLiteral('.')
                    // Support parsing up to nanosecond precision even though
                    // those extra digits will be dropped.
                    .appendFractionOfSecond(3, 9)
                    .toFormatter();
            }
            return FSE;
        }

        private static DateTimeFormatter offsetElement() {
            if (ZE == null) {
                return new DateTimeFormatterBuilder().appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
            }
            return ZE;
        }

    }

}
