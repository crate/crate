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

package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Objects;

/**
 * A wrapper around ZonedDateTime that exposes joda methods for backcompat.
 */
public class JodaCompatibleZonedDateTime {
    private static final DeprecationLogger DEPRECATION_LOGGER =
        new DeprecationLogger(LogManager.getLogger(JodaCompatibleZonedDateTime.class));

    private static void logDeprecated(String key, String message, Object... params) {
        // NOTE: we don't check SpecialPermission because this will be called (indirectly) from scripts
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            DEPRECATION_LOGGER.deprecatedAndMaybeLog(key, message, params);
            return null;
        });
    }

    private static void logDeprecatedMethod(String oldMethod, String newMethod) {
        logDeprecated(oldMethod, "Use of the joda time method [{}] is deprecated. Use [{}] instead.", oldMethod, newMethod);
    }

    private ZonedDateTime dt;

    public JodaCompatibleZonedDateTime(Instant instant, ZoneId zone) {
        this.dt = ZonedDateTime.ofInstant(instant, zone);
    }

    // access the underlying ZonedDateTime
    public ZonedDateTime getZonedDateTime() {
        return dt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JodaCompatibleZonedDateTime that = (JodaCompatibleZonedDateTime) o;
        return Objects.equals(dt, that.dt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dt);
    }

    @Override
    public String toString() {
        return dt.toString();
    }

    public boolean isAfter(ZonedDateTime o) {
        return dt.isAfter(o);
    }

    public boolean isBefore(ZonedDateTime o) {
        return dt.isBefore(o);
    }

    public boolean isEqual(ZonedDateTime o) {
        return dt.isEqual(o);
    }

    public int getDayOfMonth() {
        return dt.getDayOfMonth();
    }

    public int getDayOfYear() {
        return dt.getDayOfYear();
    }

    public int getHour() {
        return dt.getHour();
    }

    public LocalDate toLocalDate() {
        return dt.toLocalDate();
    }

    public LocalDateTime toLocalDateTime() {
        return dt.toLocalDateTime();
    }

    public int getMinute() {
        return dt.getMinute();
    }

    public Month getMonth() {
        return dt.getMonth();
    }

    public int getMonthValue() {
        return dt.getMonthValue();
    }

    public int getNano() {
        return dt.getNano();
    }

    public int getSecond() {
        return dt.getSecond();
    }

    public int getYear() {
        return dt.getYear();
    }

    public ZonedDateTime minus(TemporalAmount delta) {
        return dt.minus(delta);
    }

    public ZonedDateTime minus(long amount, TemporalUnit unit) {
        return dt.minus(amount, unit);
    }

    public ZonedDateTime minusYears(long amount) {
        return dt.minusYears(amount);
    }

    public ZonedDateTime minusMonths(long amount) {
        return dt.minusMonths(amount);
    }

    public ZonedDateTime minusWeeks(long amount) {
        return dt.minusWeeks(amount);
    }

    public ZonedDateTime minusDays(long amount) {
        return dt.minusDays(amount);
    }

    public ZonedDateTime minusHours(long amount) {
        return dt.minusHours(amount);
    }

    public ZonedDateTime minusMinutes(long amount) {
        return dt.minusMinutes(amount);
    }

    public ZonedDateTime minusSeconds(long amount) {
        return dt.minusSeconds(amount);
    }

    public ZonedDateTime minusNanos(long amount) {
        return dt.minusNanos(amount);
    }

    public ZonedDateTime plus(TemporalAmount amount) {
        return dt.plus(amount);
    }

    public ZonedDateTime plus(long amount,TemporalUnit unit) {
        return dt.plus(amount, unit);
    }

    public ZonedDateTime plusDays(long amount) {
        return dt.plusDays(amount);
    }

    public ZonedDateTime plusHours(long amount) {
        return dt.plusHours(amount);
    }

    public ZonedDateTime plusMinutes(long amount) {
        return dt.plusMinutes(amount);
    }

    public ZonedDateTime plusMonths(long amount) {
        return dt.plusMonths(amount);
    }

    public ZonedDateTime plusNanos(long amount) {
        return dt.plusNanos(amount);
    }

    public ZonedDateTime plusSeconds(long amount) {
        return dt.plusSeconds(amount);
    }

    public ZonedDateTime plusWeeks(long amount) {
        return dt.plusWeeks(amount);
    }

    public ZonedDateTime plusYears(long amount) {
        return dt.plusYears(amount);
    }

    public Instant toInstant() {
        return dt.toInstant();
    }

    public OffsetDateTime toOffsetDateTime() {
        return dt.toOffsetDateTime();
    }

    @SuppressForbidden(reason = "only exposing the method as a passthrough")
    public ZonedDateTime truncatedTo(TemporalUnit unit) {
        return dt.truncatedTo(unit);
    }

    public ZonedDateTime with(TemporalAdjuster adjuster) {
        return dt.with(adjuster);
    }

    public ZonedDateTime with(TemporalField field, long newValue) {
        return dt.with(field, newValue);
    }

    public ZonedDateTime withDayOfMonth(int value) {
        return dt.withDayOfMonth(value);
    }

    public ZonedDateTime withDayOfYear(int value) {
        return dt.withDayOfYear(value);
    }

    public ZonedDateTime withEarlierOffsetAtOverlap() {
        return dt.withEarlierOffsetAtOverlap();
    }

    public ZonedDateTime withFixedOffsetZone() {
        return dt.withFixedOffsetZone();
    }

    public ZonedDateTime withHour(int value) {
        return dt.withHour(value);
    }

    public ZonedDateTime withLaterOffsetAtOverlap() {
        return dt.withLaterOffsetAtOverlap();
    }

    public ZonedDateTime withMinute(int value) {
        return dt.withMinute(value);
    }

    public ZonedDateTime withMonth(int value) {
        return dt.withMonth(value);
    }

    public ZonedDateTime withNano(int value) {
        return dt.withNano(value);
    }

    public ZonedDateTime withSecond(int value) {
        return dt.withSecond(value);
    }

    public ZonedDateTime withYear(int value) {
        return dt.withYear(value);
    }

    public ZonedDateTime withZoneSameLocal(ZoneId zone) {
        return dt.withZoneSameLocal(zone);
    }

    public ZonedDateTime withZoneSameInstant(ZoneId zone) {
        return dt.withZoneSameInstant(zone);
    }

    @Deprecated
    public long getMillis() {
        logDeprecatedMethod("getMillis()", "toInstant().toEpochMilli()");
        return dt.toInstant().toEpochMilli();
    }

    @Deprecated
    public int getCenturyOfEra() {
        logDeprecatedMethod("getCenturyOfEra()", "get(ChronoField.YEAR_OF_ERA) / 100");
        return dt.get(ChronoField.YEAR_OF_ERA) / 100;
    }

    @Deprecated
    public int getEra() {
        logDeprecatedMethod("getEra()", "get(ChronoField.ERA)");
        return dt.get(ChronoField.ERA);
    }

    @Deprecated
    public int getHourOfDay() {
        logDeprecatedMethod("getHourOfDay()", "getHour()");
        return dt.getHour();
    }

    @Deprecated
    public int getMillisOfDay() {
        logDeprecatedMethod("getMillisOfDay()", "get(ChronoField.MILLI_OF_DAY)");
        return dt.get(ChronoField.MILLI_OF_DAY);
    }

    @Deprecated
    public int getMillisOfSecond() {
        logDeprecatedMethod("getMillisOfSecond()", "get(ChronoField.MILLI_OF_SECOND)");
        return dt.get(ChronoField.MILLI_OF_SECOND);
    }

    @Deprecated
    public int getMinuteOfDay() {
        logDeprecatedMethod("getMinuteOfDay()", "get(ChronoField.MINUTE_OF_DAY)");
        return dt.get(ChronoField.MINUTE_OF_DAY);
    }

    @Deprecated
    public int getMinuteOfHour() {
        logDeprecatedMethod("getMinuteOfHour()", "getMinute()");
        return dt.getMinute();
    }

    @Deprecated
    public int getMonthOfYear() {
        logDeprecatedMethod("getMonthOfYear()", "getMonthValue()");
        return dt.getMonthValue();
    }

    @Deprecated
    public int getSecondOfDay() {
        logDeprecatedMethod("getSecondOfDay()", "get(ChronoField.SECOND_OF_DAY)");
        return dt.get(ChronoField.SECOND_OF_DAY);
    }

    @Deprecated
    public int getSecondOfMinute() {
        logDeprecatedMethod("getSecondOfMinute()", "getSecond()");
        return dt.getSecond();
    }

    @Deprecated
    public int getWeekOfWeekyear() {
        logDeprecatedMethod("getWeekOfWeekyear()", "get(WeekFields.ISO.weekOfWeekBasedYear())");
        return dt.get(WeekFields.ISO.weekOfWeekBasedYear());
    }

    @Deprecated
    public int getWeekyear() {
        logDeprecatedMethod("getWeekyear()", "get(WeekFields.ISO.weekBasedYear())");
        return dt.get(WeekFields.ISO.weekBasedYear());
    }

    @Deprecated
    public int getYearOfCentury() {
        logDeprecatedMethod("getYearOfCentury()", "get(ChronoField.YEAR_OF_ERA) % 100");
        return dt.get(ChronoField.YEAR_OF_ERA) % 100;
    }

    @Deprecated
    public int getYearOfEra() {
        logDeprecatedMethod("getYearOfEra()", "get(ChronoField.YEAR_OF_ERA)");
        return dt.get(ChronoField.YEAR_OF_ERA);
    }

    @Deprecated
    public String toString(String format) {
        logDeprecatedMethod("toString(String)", "a DateTimeFormatter");
        // TODO: replace with bwc formatter
        return new DateTime(dt.toInstant().toEpochMilli(), DateTimeZone.forID(dt.getZone().getId())).toString(format);
    }

    @Deprecated
    public String toString(String format, Locale locale) {
        logDeprecatedMethod("toString(String,Locale)", "a DateTimeFormatter");
        // TODO: replace with bwc formatter
        return new DateTime(dt.toInstant().toEpochMilli(), DateTimeZone.forID(dt.getZone().getId())).toString(format, locale);
    }

    public DayOfWeek getDayOfWeekEnum() {
        return dt.getDayOfWeek();
    }

    @Deprecated
    public int getDayOfWeek() {
        logDeprecated("getDayOfWeek()",
            "The return type of [getDayOfWeek()] will change to an enum in 7.0. Use getDayOfWeekEnum().getValue().");
        return dt.getDayOfWeek().getValue();
    }
}
