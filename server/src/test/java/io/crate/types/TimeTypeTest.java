package io.crate.types;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class TimeTypeTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_parse_time_range_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400001] is out of range for TimeType [0, 86400000]");
        TimeType.parseTime(String.valueOf(24 * 3600 * 1000 + 1));
    }

    @Test
    public void test_parse_time_range_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [-3600000] is out of range for TimeType [0, 86400000]");
        TimeType.parseTime("-1");
    }

    @Test
    public void test_parse_time_range_overflow_take_two() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400999] is out of range for TimeType [0, 86400000]");
        TimeType.parseTime("240000.999");
    }

    @Test
    public void test_parse_time_should_always_ignore_time_zone() {
        assertThat(TimeType.parseTime("01:00:00Z"), is(3600000L));
        assertThat(TimeType.parseTime("01:00:00+00"), is(3600000L));
        assertThat(TimeType.parseTime("04:00:00-03:00"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00+0300"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00+03:00"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00.123456789+03:00"), is(14400123L));
        assertThat(TimeType.parseTime("04:00:00+0000"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00.123456789-0000"), is(14400123L));
    }

    @Test
    public void test_parse_time_no_time_zone_explicitly_mentioned() {
        assertThat(TimeType.parseTime("04:00:00"), is(14400000L));
        assertThat(TimeType.parseTime("14400000"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00.123456789"), is(14400123L));
        assertThat(TimeType.parseTime("14400123"), is(14400123L));
    }

    @Test
    public void test_format_time() {
        assertThat(TimeType.formatTime(14400000L), is("04:00:00"));
        assertThat(TimeType.formatTime(14400123L), is("04:00:00.123"));
    }

    @Test
    public void test_value() {

        Function<Object, Long> fun = TimeType.INSTANCE::value;

        assertNull(fun.apply(null));
        assertThat(fun.apply("01:00:00.000"), is(3600000L));
        assertThat(fun.apply("00:01:00.000"), is(60000L));
        assertThat(fun.apply("00:00:01.000"), is(1000L));
        assertThat(fun.apply("00:00:00.000"), is(0L));
        assertThat(fun.apply("23:59:59.998"), is(24 * 60 * 60 * 1000 - 2L));

        assertThat(fun.apply("010000.000"), is(3600000L));
        assertThat(fun.apply("000100.000"), is(60000L));
        assertThat(fun.apply("000001.000"), is(1000L));
        assertThat(fun.apply("000000.000"), is(0L));
        assertThat(fun.apply("235959.998"), is(24 * 60 * 60 * 1000 - 2L));

        assertThat(fun.apply("010000"), is(3600000L)); // same as 01:00:00.000
        assertThat(fun.apply("000100"), is(60000L));
        assertThat(fun.apply("000001"), is(1000L));
        assertThat(fun.apply("000000"), is(0L));
        assertThat(fun.apply("235959"), is(24 * 60 * 60 * 1000 - 1000L));

        assertThat(fun.apply("010000000"), is(10000000L));
        assertThat(fun.apply("000100000"), is(100000L));
        assertThat(fun.apply("000001000"), is(1000L));
        assertThat(fun.apply("000000000"), is(0L));
        assertThat(fun.apply(String.valueOf(24 * 60 * 60 * 1000L - 1L)), is(24 * 60 * 60 * 1000 - 1L));

        assertThat(fun.apply(010000.987), is(10000987L));
        assertThat(fun.apply(000100.987), is(100987L));
        assertThat(fun.apply(000003.14159), is(3141L));
        assertThat(fun.apply(000000.321), is(321L));

        // floats drop precision, but you can still use them
        assertThat(fun.apply(010000.987F), is(10000987L));
        assertThat(fun.apply(000100.987F), is(100987L));
        assertThat(fun.apply(000003.14159F), is(3141L));
        assertThat(fun.apply(000000.321F), is(321L));

        assertThat(fun.apply(10000), is(10000L));
        assertThat(fun.apply(100), is(100L));
        assertThat(fun.apply(1), is(1L));
        assertThat(fun.apply(0), is(0L));
        assertThat(fun.apply(235959), is(235959L));
        assertThat(fun.apply((short) 144), is(144L));

        assertThat(fun.apply("01:00:00Z"), is(3600000L));
        assertThat(fun.apply("01:00:00+00"), is(3600000L));
        assertThat(fun.apply("04:00:00-03:00"), is(14400000L));
        assertThat(fun.apply("04:00:00+0300"), is(14400000L));
        assertThat(fun.apply("04:00:00+03:00"), is(14400000L));
        assertThat(fun.apply("04:00:00.123456789+03:00"), is(14400123L));
        assertThat(fun.apply("04:00:00+0000"), is(14400000L));
        assertThat(fun.apply("04:00:00.123456789-0000"), is(14400123L));
    }
}
