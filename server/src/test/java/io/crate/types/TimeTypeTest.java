package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class TimeTypeTest extends CrateUnitTest {

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
    public void test_parse_time_unsupported_literal_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [234.9999] is not a valid literal for type TimeType");
        TimeType.parseTime("234.9999");
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
    public void test_value_null() {
        assertNull(TimeType.INSTANCE.value(null));
    }

    @Test
    public void test_value_ISO_formats_with_time_zone() {
        assertThat(TimeType.INSTANCE.value("01:00:00Z"), is(3600000000L));
        assertThat(TimeType.INSTANCE.value("01:00:00+00"), is(3600000000L));
        assertThat(TimeType.INSTANCE.value("04:00:00-03:00"), is(14400000000L));
        assertThat(TimeType.INSTANCE.value("04:00:00+0300"), is(14400000000L));
        assertThat(TimeType.INSTANCE.value("04:00:00+03:00"), is(14400000000L));
        assertThat(TimeType.INSTANCE.value("04:00:00.123456789+03:00"), is(14400123456L));
        assertThat(TimeType.INSTANCE.value("04:00:00+0000"), is(14400000000L));
        assertThat(TimeType.INSTANCE.value("04:00:00.123456789-0000"), is(14400123456L));
    }

    @Test
    public void test_value_ISO_formats_without_time_zone() {
        assertThat(TimeType.INSTANCE.value("01:00:00.000"), is(3600000000L));
        assertThat(TimeType.INSTANCE.value("00:00:00.000"), is(0L));
        assertThat(TimeType.INSTANCE.value("23:59:59.999998"), is(24 * 60 * 60 * 1000_000L - 2L));
    }

    @Test
    public void test_value_short_hand_format_floating_point() {
        assertThat(TimeType.INSTANCE.value("010000.000"), is(3600000000L));
        assertThat(TimeType.INSTANCE.value("000000.000"), is(0L));
        assertThat(TimeType.INSTANCE.value("235959.999998"), is(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeType.INSTANCE.value("235959.998"), is(24 * 60 * 60 * 1000_000L - 2000L));
        assertThat(TimeType.INSTANCE.value("240000.000"), is(24 * 60 * 60 * 1000_000L));
    }

    @Test
    public void test_value_short_hand_format_long() {
        assertThat(TimeType.INSTANCE.value("010000"), is(3600000000L)); // same as 01:00:00.000
        assertThat(TimeType.INSTANCE.value("000000"), is(0L));
        assertThat(TimeType.INSTANCE.value("235959"), is(24 * 60 * 60 * 1000_000L - 1000_000L));
    }

    @Test
    public void test_value_is_a_long_in_range() {
        assertThat(TimeType.INSTANCE.value("010000000"), is(10000000L));
        assertThat(TimeType.INSTANCE.value("000000000"), is(0L));
        assertThat(TimeType.INSTANCE.value(String.valueOf(24 * 60 * 60 * 1000L - 1L)), is(24 * 60 * 60 * 1000 - 1L));
    }
}
