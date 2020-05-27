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
        expectedException.expectMessage("value [86400000001] is out of range for 'TimeType' [0, 86400000000]");
        TimeType.parseTime(String.valueOf(24 * 3600 * 1000_000L + 1));
    }

    @Test
    public void test_parse_time_range_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [-86400000000] is out of range for 'TimeType' [0, 86400000000]");
        TimeType.parseTime(String.valueOf(-24 * 3600 * 1000_000L));
    }

    @Test
    public void test_parse_time_out_of_range_hh() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'hh' [0, 24]");
        TimeType.parseTime("25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'mm' [0, 59]");
        TimeType.parseTime("1778");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'ss' [0, 59]");
        TimeType.parseTime("175978");
    }

    @Test
    public void test_parse_time_out_of_range_hh_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'hh' [0, 24]");
        TimeType.parseTime("25.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'mm' [0, 59]");
        TimeType.parseTime("1778.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'ss' [0, 59]");
        TimeType.parseTime("175978.999999");
    }

    @Test
    public void test_parse_time_out_of_range_micros_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [9999999] is out of range for 'micros' [0, 999999]");
        TimeType.parseTime("00.9999999");
    }

    @Test
    public void test_parse_time_range_overflow_take_two() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400999000] is out of range for 'TimeType' [0, 86400000000]");
        TimeType.parseTime("240000.999");
    }

    @Test
    public void test_parse_time_midnight_when_ISO_parser_does_not_like_it() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [24:00:00.000] is not a valid literal for TimeType");
        TimeType.parseTime("24:00:00.000");
    }

    @Test
    public void test_parse_time_midnight_when_ISO_parser_does_like_it() {
        assertThat(TimeType.parseTime("240000.000"), is(24 * 60 * 60 * 1000_000L));
    }

    @Test
    public void test_parse_time_should_always_ignore_time_zone() {
        assertThat(TimeType.parseTime("01:00:00Z"), is(3600000000L));
        assertThat(TimeType.parseTime("01:00:00+00"), is(3600000000L));
        assertThat(TimeType.parseTime("04:00:00-03:00"), is(14400000000L));
        assertThat(TimeType.parseTime("04:00:00+0300"), is(14400000000L));
        assertThat(TimeType.parseTime("04:00:00+03:00"), is(14400000000L));
        assertThat(TimeType.parseTime("04:00:00.123456789+03:00"), is(14400123456L));
        assertThat(TimeType.parseTime("04:00:00+0000"), is(14400000000L));
        assertThat(TimeType.parseTime("04:00:00.123456789-0000"), is(14400123456L));
    }

    @Test
    public void test_parse_time_no_time_zone_explicitly_mentioned() {
        assertThat(TimeType.parseTime("04:00:00"), is(14400000000L));
        assertThat(TimeType.parseTime("14400000"), is(14400000L));
        assertThat(TimeType.parseTime("04:00:00.123456789"), is(14400123456L));
        assertThat(TimeType.parseTime("14400123"), is(14400123L));
    }

    @Test
    public void test_format_time() {
        assertThat(TimeType.formatTime(14400000000L), is("04:00:00"));
        assertThat(TimeType.formatTime(14400123000L), is("04:00:00.123"));
    }

    @Test
    public void test_value() {

        Function<Object, Long> fun = TimeType.INSTANCE::value;

        assertNull(fun.apply(null));
        assertThat(fun.apply("01:00:00.000"), is(3600000000L));
        assertThat(fun.apply("00:01:00.000"), is(60000000L));
        assertThat(fun.apply("00:00:01.000"), is(1000000L));
        assertThat(fun.apply("00:00:00.000"), is(0L));
        assertThat(fun.apply("23:59:59.999998"), is(24 * 60 * 60 * 1000_000L - 2L));

        assertThat(fun.apply("010000.000"), is(3600000000L));
        assertThat(fun.apply("000100.000"), is(60000000L));
        assertThat(fun.apply("000001.000"), is(1000000L));
        assertThat(fun.apply("000000.000"), is(0L));
        assertThat(fun.apply("235959.999998"), is(24 * 60 * 60 * 1000_000L - 2L));

        assertThat(fun.apply("235959.998"), is(24 * 60 * 60 * 1000_000L - 2000L));
        assertThat(fun.apply("240000.000"), is(24 * 60 * 60 * 1000_000L));

        assertThat(fun.apply("010000"), is(3600000000L)); // same as 01:00:00.000
        assertThat(fun.apply("000100"), is(60000000L));
        assertThat(fun.apply("000001"), is(1000000L));
        assertThat(fun.apply("000000"), is(0L));
        assertThat(fun.apply("235959"), is(24 * 60 * 60 * 1000_000L - 1000_000L));

        assertThat(fun.apply("010000000"), is(10000000L));
        assertThat(fun.apply("000100000"), is(100000L));
        assertThat(fun.apply("000001000"), is(1000L));
        assertThat(fun.apply("000000000"), is(0L));
        assertThat(fun.apply(String.valueOf(24 * 60 * 60 * 1000L - 1L)), is(24 * 60 * 60 * 1000 - 1L));

        assertThat(fun.apply("01:00:00Z"), is(3600000000L));
        assertThat(fun.apply("01:00:00+00"), is(3600000000L));
        assertThat(fun.apply("04:00:00-03:00"), is(14400000000L));
        assertThat(fun.apply("04:00:00+0300"), is(14400000000L));
        assertThat(fun.apply("04:00:00+03:00"), is(14400000000L));
        assertThat(fun.apply("04:00:00.123456789+03:00"), is(14400123456L));
        assertThat(fun.apply("04:00:00+0000"), is(14400000000L));
        assertThat(fun.apply("04:00:00.123456789-0000"), is(14400123456L));
    }
}
