package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class TimeTZTypeTest extends CrateUnitTest {

    @Test
    public void test_parse_time_range_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400000001] is out of range for '24:00:00.000001' [0, 86400000000]");
        TimeTZType.parseTime("24:00:00.000001");
    }

    @Test
    public void test_parse_time_range_overflow_take_two() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400999000] is out of range for '240000.999' [0, 86400000000]");
        TimeTZType.parseTime("240000.999");
    }

    @Test
    public void test_parse_time_unsupported_literal_long() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [234] is not a valid literal for TimeTZType");
        TimeTZType.parseTime("234");
    }

    @Test
    public void test_parse_time_unsupported_literal_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [234.9999] is not a valid literal for TimeTZType");
        TimeTZType.parseTime("234.9999");
    }

    @Test
    public void test_parse_time_out_of_range_hh() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'HH' [0, 24]");
        TimeTZType.parseTime("25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'MM' [0, 59]");
        TimeTZType.parseTime("1778");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'SS' [0, 59]");
        TimeTZType.parseTime("175978");
    }

    @Test
    public void test_parse_time_out_of_range_hh_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'HH' [0, 24]");
        TimeTZType.parseTime("25.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'MM' [0, 59]");
        TimeTZType.parseTime("1778.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'SS' [0, 59]");
        TimeTZType.parseTime("175978.999999");
    }

    @Test
    public void test_parse_time() {
        assertThat(TimeTZType.parseTime("04"), is(4 * 60 * 60 * 1000_000L));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("0400"));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("04:00"));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("040000"));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("04:00:00"));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("040000.0"));
        assertEquals(TimeTZType.parseTime("04"), TimeTZType.parseTime("04:00:00.0"));
    }

    @Test
    public void test_format_time() {
        assertThat(TimeTZType.formatTime(14400000000L), is("04:00:00"));
        assertThat(TimeTZType.formatTime(14400123000L), is("04:00:00.123"));
        assertThat(TimeTZType.formatTime(14400123666L), is("04:00:00.123666"));
    }

    @Test
    public void test_value_null() {
        assertNull(TimeTZType.INSTANCE.value(null));
    }

    @Test
    public void test_value_long() {
        assertThat(TimeTZType.INSTANCE.value(0), is(TimeTZType.parseTime("00")));
        assertThat(TimeTZType.INSTANCE.value(TimeTZType.MAX_MICROS), is(TimeTZType.parseTime("24")));
    }

    @Test
    public void test_value_ISO_formats_with_time_zone() {
        assertThat(TimeTZType.INSTANCE.value("01:00:00Z"), is(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00+00"), is(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00-03:00"), is(14400000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+0300"), is(14400000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+03:00"), is(14400000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00.123456789+03:00"), is(14400123456L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+0000"), is(14400000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00.123456789-0000"), is(14400123456L));
    }

    @Test
    public void test_value_ISO_formats_without_time_zone() {
        assertThat(TimeTZType.INSTANCE.value("01.99999"), is(3600999990L));
        assertThat(TimeTZType.INSTANCE.value("0110.99999"), is(4200999990L));
        assertThat(TimeTZType.INSTANCE.value("011101.99999"), is(4261999990L));

        assertThat(TimeTZType.INSTANCE.value("01:00:00.000"), is(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("23:59:59.999998"), is(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeTZType.INSTANCE.value("24:00:00.000"), is(TimeTZType.MAX_MICROS));

    }

    @Test
    public void test_value_short_hand_format_floating_point() {
        assertThat(TimeTZType.INSTANCE.value("010000.000"), is(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00.000"), is(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("000000.000"), is(0L));
        assertThat(TimeTZType.INSTANCE.value("235959.999998"), is(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeTZType.INSTANCE.value("235959.998"), is(24 * 60 * 60 * 1000_000L - 2000L));
        assertThat(TimeTZType.INSTANCE.value("240000.000"), is(24 * 60 * 60 * 1000_000L));
    }

    @Test
    public void test_value_short_hand_format_long() {
        assertThat(TimeTZType.INSTANCE.value("010000"), is(3600000000L)); // same as 01:00:00.000
        assertThat(TimeTZType.INSTANCE.value("000000"), is(0L));
        assertThat(TimeTZType.INSTANCE.value("235959"), is(24 * 60 * 60 * 1000_000L - 1000_000L));
    }

    @Test
    public void test_value_is_a_long_in_range() {
        assertThat(TimeTZType.INSTANCE.value("010000000"), is(10000000L));
        assertThat(TimeTZType.INSTANCE.value("000000000"), is(0L));
        assertThat(TimeTZType.INSTANCE.value(
            String.valueOf(24 * 60 * 60 * 1000L - 1L)), is(24 * 60 * 60 * 1000 - 1L));
    }
}
