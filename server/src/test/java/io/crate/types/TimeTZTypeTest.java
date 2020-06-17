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
    public void test_value_cast_null() {
        assertNull(TimeTZType.INSTANCE.value(null));
    }

    @Test
    public void test_value_cast_not_null() {
        assertThat(TimeTZType.INSTANCE.value(new TimeTZ(3600000000L, 7200)),
                   isTZ(3600000000L, 7200));
    }

    @Test
    public void test_implicit_cast_null() {
        assertNull(TimeTZType.INSTANCE.implicitCast(null));
    }

    @Test
    public void test_implicit_cast_ISO_formats_with_time_zone() {
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     CET"), isTZ(3600000000L, 7200));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     UTC"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     GMT"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00  Z"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00 +00"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00-03:00"), isTZ(14400000000L, -10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+0300"), isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+03:00"), isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00.123456789+03:00"), isTZ(14400123456L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+0000"), isTZ(14400000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00.123456789-0000"), isTZ(14400123456L));
    }

    @Test
    public void test_implicit_cast_ISO_formats_without_time_zone() {
        assertThat(TimeTZType.INSTANCE.implicitCast("01.99999"), isTZ(3600999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("0110.99999"), isTZ(4200999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("011101.99999"), isTZ(4261999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("23:59:59.999998"), isTZ(24 * 60 * 60 * 1000_000L - 2L));
    }

    @Test
    public void test_implicit_cast_short_hand_format_floating_point() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00.000"), isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("000000.000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959.999998"), isTZ(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959.998"), isTZ(24 * 60 * 60 * 1000_000L - 2000L));
    }

    @Test
    public void test_implicit_cast_short_hand_format_long() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000"), isTZ(3600000000L)); // same as 01:00:00.000
        assertThat(TimeTZType.INSTANCE.implicitCast("000000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959"), isTZ(24 * 60 * 60 * 1000_000L - 1000_000L));
    }

    @Test
    public void test_implicit_cast_is_a_long_in_range() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000000"), isTZ(10000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("000000000"), isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast(
            String.valueOf(24 * 60 * 60 * 1000L - 1L)), isTZ(24 * 60 * 60 * 1000 - 1L));
    }
}
