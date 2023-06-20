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

package org.elasticsearch.common.joda;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Locale;

import org.elasticsearch.common.Strings;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.ReadablePartial;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.joda.time.format.DateTimePrinter;

import io.crate.common.time.IsoLocale;
import io.crate.time.StrictISODateTimeFormat;

public class Joda {

    /**
     * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
     */
    public static FormatDateTimeFormatter forPattern(String input) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        DateTimeFormatter formatter;
        if ("epoch_millis".equals(input)) {
            formatter = new DateTimeFormatterBuilder()
                .append(new EpochTimePrinter(true), new EpochTimeParser(true))
                .toFormatter();
        } else if ("strict_date_optional_time".equals(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new FormatDateTimeFormatter(input,
                    StrictISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC),
                    StrictISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC), IsoLocale.ROOT);
        } else if (Strings.hasLength(input) && input.contains("||")) {
            String[] formats = Strings.delimitedListToStringArray(input, "||");
            DateTimeParser[] parsers = new DateTimeParser[formats.length];

            if (formats.length == 1) {
                formatter = forPattern(input).parser();
            } else {
                DateTimeFormatter dateTimeFormatter = null;
                for (int i = 0; i < formats.length; i++) {
                    FormatDateTimeFormatter currentFormatter = forPattern(formats[i]);
                    DateTimeFormatter currentParser = currentFormatter.parser();
                    if (dateTimeFormatter == null) {
                        dateTimeFormatter = currentFormatter.printer();
                    }
                    parsers[i] = currentParser.getParser();
                }

                assert dateTimeFormatter != null;
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(dateTimeFormatter.withZone(DateTimeZone.UTC).getPrinter(), parsers);
                formatter = builder.toFormatter();
            }
        } else {
            try {
                formatter = DateTimeFormat.forPattern(input);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
        return new FormatDateTimeFormatter(input, formatter.withZone(DateTimeZone.UTC), IsoLocale.ROOT);
    }

    public static FormatDateTimeFormatter getStrictStandardDateFormatter() {
        // 2014/10/10
        DateTimeFormatter shortFormatter = new DateTimeFormatterBuilder()
                .appendFixedDecimal(DateTimeFieldType.year(), 4)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
                .toFormatter()
                .withZoneUTC();

        // 2014/10/10 12:12:12
        DateTimeFormatter longFormatter = new DateTimeFormatterBuilder()
                .appendFixedDecimal(DateTimeFieldType.year(), 4)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
                .appendLiteral(' ')
                .appendFixedSignedDecimal(DateTimeFieldType.hourOfDay(), 2)
                .appendLiteral(':')
                .appendFixedSignedDecimal(DateTimeFieldType.minuteOfHour(), 2)
                .appendLiteral(':')
                .appendFixedSignedDecimal(DateTimeFieldType.secondOfMinute(), 2)
                .toFormatter()
                .withZoneUTC();

        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(longFormatter.withZone(DateTimeZone.UTC).getPrinter(), new DateTimeParser[]{longFormatter.getParser(), shortFormatter.getParser(), new EpochTimeParser(true)});

        return new FormatDateTimeFormatter("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis", builder.toFormatter().withZone(DateTimeZone.UTC), Locale.ROOT);
    }


    public static final DurationFieldType QUARTERS = new DurationFieldType("quarters") {
        @Override
        public DurationField getField(Chronology chronology) {
            return new ScaledDurationField(chronology.months(), QUARTERS, 3);
        }
    };

    public static final DateTimeFieldType QUARTER_OF_YEAR = new DateTimeFieldType("quarterOfYear") {
        @Override
        public DurationFieldType getDurationType() {
            return QUARTERS;
        }

        @Override
        public DurationFieldType getRangeDurationType() {
            return DurationFieldType.years();
        }

        @Override
        public DateTimeField getField(Chronology chronology) {
            return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QUARTER_OF_YEAR, 3), 1);
        }
    };

    public static class EpochTimeParser implements DateTimeParser {

        private final boolean hasMilliSecondPrecision;

        public EpochTimeParser(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimateParsedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }

        @Override
        public int parseInto(DateTimeParserBucket bucket, String text, int position) {
            boolean isPositive = text.startsWith("-") == false;
            int firstDotIndex = text.indexOf('.');
            boolean isTooLong = (firstDotIndex == -1 ? text.length() : firstDotIndex) > estimateParsedLength();

            if (bucket.getZone() != DateTimeZone.UTC) {
                String format = hasMilliSecondPrecision ? "epoch_millis" : "epoch_second";
                throw new IllegalArgumentException("time_zone must be UTC for format [" + format + "]");
            } else if (isPositive && isTooLong) {
                return -1;
            }

            int factor = hasMilliSecondPrecision ? 1 : 1000;
            try {
                long millis = new BigDecimal(text).longValue() * factor;
                DateTime dt = new DateTime(millis, DateTimeZone.UTC);
                bucket.saveField(DateTimeFieldType.year(), dt.getYear());
                bucket.saveField(DateTimeFieldType.monthOfYear(), dt.getMonthOfYear());
                bucket.saveField(DateTimeFieldType.dayOfMonth(), dt.getDayOfMonth());
                bucket.saveField(DateTimeFieldType.hourOfDay(), dt.getHourOfDay());
                bucket.saveField(DateTimeFieldType.minuteOfHour(), dt.getMinuteOfHour());
                bucket.saveField(DateTimeFieldType.secondOfMinute(), dt.getSecondOfMinute());
                bucket.saveField(DateTimeFieldType.millisOfSecond(), dt.getMillisOfSecond());
                bucket.setZone(DateTimeZone.UTC);
            } catch (Exception e) {
                return -1;
            }
            return text.length();
        }
    }

    public static class EpochTimePrinter implements DateTimePrinter {

        private boolean hasMilliSecondPrecision;

        public EpochTimePrinter(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimatePrintedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }


        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(StringBuffer buf, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(instant - displayOffset);
            } else {
                buf.append((instant - displayOffset) / 1000);
            }
        }

        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(Writer out, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale) throws IOException {
            if (hasMilliSecondPrecision) {
                out.write(String.valueOf(instant - displayOffset));
            } else {
                out.append(String.valueOf((instant - displayOffset) / 1000));
            }
        }

        @Override
        public void printTo(StringBuffer buf, ReadablePartial partial, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                buf.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        @Override
        public void printTo(Writer out, ReadablePartial partial, Locale locale) throws IOException {
            if (hasMilliSecondPrecision) {
                out.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                out.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        private long getDateTimeMillis(ReadablePartial partial) {
            int year = partial.get(DateTimeFieldType.year());
            int monthOfYear = partial.get(DateTimeFieldType.monthOfYear());
            int dayOfMonth = partial.get(DateTimeFieldType.dayOfMonth());
            int hourOfDay = partial.get(DateTimeFieldType.hourOfDay());
            int minuteOfHour = partial.get(DateTimeFieldType.minuteOfHour());
            int secondOfMinute = partial.get(DateTimeFieldType.secondOfMinute());
            int millisOfSecond = partial.get(DateTimeFieldType.millisOfSecond());
            return partial.getChronology().getDateTimeMillis(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond);
        }
    }
}
