package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;


public class TimeTZTypeTest extends CrateUnitTest {

    private org.hamcrest.Matcher<TimeTZ> isTZ(long value) {
        return is(new TimeTZ(value, 0));
    }

    private static org.hamcrest.Matcher<TimeTZ> isTZ(long value, int zoneOffset) {
        return is(new TimeTZ(value, zoneOffset));
    }

    @Test
    public void test_value_null() {
        assertNull(TimeTZType.INSTANCE.value(null));
    }

    @Test
    public void test_value_ISO_formats_with_time_zone() {
        assertThat(TimeTZType.INSTANCE.value("01:00:00     CET"), isTZ(3600000000L, 7200));
        assertThat(TimeTZType.INSTANCE.value("01:00:00     UTC"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00     GMT"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00  Z"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00 +00"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00-03:00"), isTZ(14400000000L, -10800));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+0300"), isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+03:00"), isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.value("04:00:00.123456789+03:00"), isTZ(14400123456L, 10800));
        assertThat(TimeTZType.INSTANCE.value("04:00:00+0000"), isTZ(14400000000L));
        assertThat(TimeTZType.INSTANCE.value("04:00:00.123456789-0000"), isTZ(14400123456L));
    }

    @Test
    public void test_value_ISO_formats_without_time_zone() {
        assertThat(TimeTZType.INSTANCE.value("01.99999"), isTZ(3600999990L));
        assertThat(TimeTZType.INSTANCE.value("0110.99999"), isTZ(4200999990L));
        assertThat(TimeTZType.INSTANCE.value("011101.99999"), isTZ(4261999990L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("23:59:59.999998"), isTZ(24 * 60 * 60 * 1000_000L - 2L));
    }

    @Test
    public void test_value_short_hand_format_floating_point() {
        assertThat(TimeTZType.INSTANCE.value("010000.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("01:00:00.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.value("000000.000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.value("235959.999998"), isTZ(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeTZType.INSTANCE.value("235959.998"), isTZ(24 * 60 * 60 * 1000_000L - 2000L));
    }

    @Test
    public void test_value_short_hand_format_long() {
        assertThat(TimeTZType.INSTANCE.value("010000"), isTZ(3600000000L)); // same as 01:00:00.000
        assertThat(TimeTZType.INSTANCE.value("000000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.value("235959"), isTZ(24 * 60 * 60 * 1000_000L - 1000_000L));
    }

    @Test
    public void test_value_is_a_long_in_range() {
        assertThat(TimeTZType.INSTANCE.value("010000000"), isTZ(10000000L));
        assertThat(TimeTZType.INSTANCE.value("000000000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.value(
            String.valueOf(24 * 60 * 60 * 1000L - 1L)), isTZ(24 * 60 * 60 * 1000 - 1L));
    }
}
