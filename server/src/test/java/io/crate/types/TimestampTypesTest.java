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

package io.crate.types;

import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampTypesTest {

    @Test
    public void testTimestampWithZoneParseWithOffset() {
        assertThat(TimestampType.parseTimestamp("1999-01-08T01:00:00Z"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T01:00:00+00"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00-03:00"), is(915778800000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00+0300"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00+03:00"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00.123456789+03:00"), is(915757200123L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00+0000"), is(915768000000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00.123456789-0000"), is(915768000123L));
    }

    @Test
    public void testTimestampWithZoneParseWithOffsetSQLStandardFormat() {
        assertThat(TimestampType.parseTimestamp("1999-01-08 01:00:00Z"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08 04:00:00+03:00"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08 04:00:00.123456789+03:00"), is(915757200123L));
    }

    @Test
    public void testTimestampWithZoneParseWithoutOffset() {
        assertThat(TimestampType.parseTimestamp("1999-01-08"), is(915753600000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00"), is(915768000000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00.123456789"), is(915768000123L));
    }

    @Test
    public void testTimestampWithZoneParseWithoutOffsetSQLStandardFormat() {
        assertThat(TimestampType.parseTimestamp("1999-01-08 04:00:00.123456789"), is(915768000123L));
    }

    @Test
    public void testTimestampWithoutZoneParseWithOffset() {
        long expected = 915768000000L;
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00Z"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00+09:00"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00+0900"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00-0100"), is(expected));
    }

    @Test
    public void testTimestampWithoutZoneParseWithOffsetSQLStandardFormat() {
        long expected = 915768000000L;
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 04:00:00Z"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 04:00:00+0900"), is(expected));
    }

    @Test
    public void testTimestampWithoutZoneParseWithoutOffset() {
        long expected = 915768000000L;
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08"), is(915753600000L));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00.123456789"), is(expected + 123));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00+01"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00.123456789+01:00"), is(expected + 123));
    }

    @Test
    public void testTimestampWithoutZoneParseWithoutOffsetSQLStandardFormat() {
        long expected = 915768000000L;
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 04:00:00"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 04:00:00.123456789"), is(expected + 123));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 04:00:00.123456789+01:00"), is(expected + 123));
    }

    @Test
    public void testTimestampWithZoneUsingDoubleSpaceBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestamp("1999-01-08  04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithoutZoneUsingDoubleSpaceBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestampIgnoreTimeZone("1999-01-08  04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithZoneNothingBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestamp("1999-01-0804:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithoutZoneNothingBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestampIgnoreTimeZone("1999-01-0804:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithZoneUsingSpaceAndTBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestamp("1999-01-08 T04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithoutZoneUsingSpaceAndTBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestampIgnoreTimeZone("1999-01-08 T04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithZoneUsingTAndSpaceBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestamp("1999-01-08T 04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampWithoutZoneUsingTAndSpaceBetweenDateAndTimeDoesNotParse() {
        assertThrows(IllegalArgumentException.class,
                     () -> TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T 04:00:00"),
                     "could not be parsed, unparsed text found at index 10");
    }

    @Test
    public void testTimestampParseUnixTimestampAsString() {
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1395961200000"), is(1395961200000L));
        assertThat(TimestampType.parseTimestamp("1395961200000"), is(1395961200000L));
    }

    @Test
    public void test_cast_object_to_timestamptz_throws_exception() {
        assertThrows(ClassCastException.class,
                     () -> TimestampType.INSTANCE_WITH_TZ.implicitCast(Map.of()),
                     "Can't cast '{}' to timestamp with time zone");
    }
}
