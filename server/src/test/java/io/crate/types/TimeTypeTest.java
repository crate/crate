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
    public void test_parse_time() {
        assertThat(TimeType.parseTime("21.31415"), is(75600314L)); // hh.ffffff
        assertThat(TimeType.parseTime("1203.999999"), is(43380999L)); // hhmm.ffffff
        assertThat(TimeType.parseTime("120312.59876"), is(43392598L)); // hhmmss.ffffff

        assertThat(TimeType.parseTime("21.31415f"), is(75600314L)); // hh.ffffff
        assertThat(TimeType.parseTime("1203.999999f"), is(43380999L)); // hhmm.ffffff
        assertThat(TimeType.parseTime("120312.59876f"), is(43392598L)); // hhmmss.ffffff

        assertThat(TimeType.parseTime("21"), is(75600000L)); // hh
        assertThat(TimeType.parseTime("1203"), is(43380000L)); // hhmm
        assertThat(TimeType.parseTime("120312"), is(43392000L)); // hhmmss

        assertThat(TimeType.parseTime("21:00:00.31415"), is(TimeType.parseTime("21.31415")));
        assertThat(TimeType.parseTime("12:03:00.999999"), is(TimeType.parseTime("1203.999999")));
        assertThat(TimeType.parseTime("12:03:12.59876"), is(TimeType.parseTime("120312.59876")));

        long time = 23 * 3600 + 25 * 60 + 17 + 876L;
        assertThat(TimeType.parseTime(String.valueOf(time)), is(time));

        float timeWithMillisAsFloat = 23 * 3600 + 25 * 60 + 17 + 0.876f;
        String timeWithMillisAsStr = String.valueOf(timeWithMillisAsFloat);
        long expectedEpochMilli = (long) Math.floor(Double.parseDouble(timeWithMillisAsStr) * 1000L);
        assertThat(TimeType.parseTime(timeWithMillisAsStr), is(expectedEpochMilli)); // loss of precision

        double timeWithMillis = 23 * 3600 + 25 * 60 + 17 + 0.876;
        assertThat(TimeType.parseTime(String.valueOf(timeWithMillis)), is((long) (timeWithMillis * 1000L)));
    }

    @Test
    public void test_parse_time_range_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400001] is out of range");
        TimeType.parseTime(String.valueOf(24 * 3600 * 1000 + 1));
    }

    @Test
    public void test_parse_time_range_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [-1] as 'hh' is out of range");
        TimeType.parseTime("-1");
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
