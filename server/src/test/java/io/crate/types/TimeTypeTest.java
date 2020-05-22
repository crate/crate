package io.crate.types;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class TimeTypeTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        assertNull(TimeType.INSTANCE.value(null));
        assertThat(TimeType.INSTANCE.value("01:00:00Z"), is(3600000L));
        assertThat(TimeType.INSTANCE.value("01:00:00+00"), is(3600000L));
        assertThat(TimeType.INSTANCE.value("04:00:00-03:00"), is(14400000L));
        assertThat(TimeType.INSTANCE.value("04:00:00+0300"), is(14400000L));
        assertThat(TimeType.INSTANCE.value("04:00:00+03:00"), is(14400000L));
        assertThat(TimeType.INSTANCE.value("04:00:00.123456789+03:00"), is(14400123L));
        assertThat(TimeType.INSTANCE.value("04:00:00+0000"), is(14400000L));
        assertThat(TimeType.INSTANCE.value("04:00:00.123456789-0000"), is(14400123L));
        assertThat(TimeType.INSTANCE.value((short) 144), is(144L));
        assertThat(TimeType.INSTANCE.value(14400000), is(14400000L));
        assertThat(TimeType.INSTANCE.value(14400.123456789f), is(14400123L));
        assertThat(TimeType.INSTANCE.value(14400.123456789d), is(14400123L));
    }
}
