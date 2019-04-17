package io.crate.types;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TimestampTypesTest {

    @Test
    public void testTimestampWithZoneParseWithOffset() {
        assertThat(TimestampType.parseTimestamp("1999-01-08T01:00:00Z"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T01:00:00+00"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00-03:00"), is(915778800000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00+0300"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00+03:00"), is(915757200000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00.123456789+03:00"), is(915757200123L));
    }

    @Test
    public void testTimestampWithZoneParseWithoutOffset() {
        assertThat(TimestampType.parseTimestamp("1999-01-08"), is(915753600000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00"), is(915768000000L));
        assertThat(TimestampType.parseTimestamp("1999-01-08T04:00:00.123456789"), is(915768000123L));
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
    public void testTimestampWithoutZoneParseWithoutOffset() {
        long expected = 915768000000L;
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08"), is(915753600000L));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00.123456789"), is(expected + 123));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00+01"), is(expected));
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1999-01-08T04:00:00.123456789+01:00"), is(expected + 123));
    }

    @Test
    public void testTimestampParseUnixTimestampAsString() {
        assertThat(TimestampType.parseTimestampIgnoreTimeZone("1395961200000"), is(1395961200000L));
        assertThat(TimestampType.parseTimestamp("1395961200000"), is(1395961200000L));
    }
}
