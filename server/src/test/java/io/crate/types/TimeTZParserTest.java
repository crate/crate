package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static io.crate.types.TimeTZParser.parse;


public class TimeTZParserTest extends CrateUnitTest {

    private static org.hamcrest.Matcher<TimeTZ> isTZ(long value, int zoneOffset) {
        return is(new TimeTZ(value, zoneOffset));
    }

    @Test
    public void test_parse_time_correct_syntax_no_colon() {
        assertThat(parse("12"), isTZ(43200000000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("12.000001"), isTZ(43200000001L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("1200"), isTZ(43200000000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("1200.002"), isTZ(43200002000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("120000"), isTZ(43200000000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("120000.000003"), isTZ(43200000003L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
    }

    @Test
    public void test_parse_time_correct_syntax_colon() {
        assertThat(parse("12:00"), isTZ(43200000000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("12:00.999"), isTZ(43200999000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("12:00:00"), isTZ(43200000000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertThat(parse("12:00:00.003"), isTZ(43200003000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
    }

    @Test
    public void test_parse_time_correct_syntax_tz() {
        assertThat(parse("00+12"), isTZ(0L, 12 * 3600));
        assertThat(parse("00+1200"), isTZ(0L, 12 * 3600));
        assertThat(parse("00-12:00"), isTZ(0L, -12 * 3600));
        assertThat(parse("00.0+12"), isTZ(0L, 12 * 3600));
        assertThat(parse("00.0+1200"), isTZ(0L, 12 * 3600));
        assertThat(parse("00.000001001  +12:00"), isTZ(1L, 12 * 3600));
    }

    @Test
    public void test_parse_time_range_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [86400000001] is out of range for '24:00:00.000001' [0, 86400000000]");
        parse("24:00:00.000001");
    }

    @Test
    public void test_parse_time_unsupported_literal_long() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [234] is not a valid literal for TimeTZType");
        parse("234");
    }

    @Test
    public void test_parse_time_unsupported_literal_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [234.9999] is not a valid literal for TimeTZType");
        parse("234.9999");
    }

    @Test
    public void test_parse_time_out_of_range_hh() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'HH' [0, 24]");
        parse("25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'MM' [0, 59]");
        parse("1778");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'SS' [0, 59]");
        parse("175978");
    }

    @Test
    public void test_parse_time_out_of_range_hh_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [25] is out of range for 'HH' [0, 24]");
        parse("25.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'MM' [0, 59]");
        parse("1778.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("value [78] is out of range for 'SS' [0, 59]");
        parse("175978.999999");
    }

    @Test
    public void test_parse_time() {
        assertThat(parse("04"), isTZ(4 * 60 * 60 * 1000_000L, TimeTZParser.LOCAL_TZ_SECONDS_FROM_UTC));
        assertEquals(parse("04"), parse("0400"));
        assertEquals(parse("04"), parse("04:00"));
        assertEquals(parse("04"), parse("040000"));
        assertEquals(parse("04"), parse("04:00:00"));
        assertEquals(parse("04"), parse("040000.0"));
        assertEquals(parse("04"), parse("04:00:00.0"));
        assertEquals(parse("04+00"), parse("04:00:00.0+00"));
        assertEquals(parse("04+00"), parse("04:00:00.0+0000"));
        assertEquals(parse("04+00"), parse("04:00:00.0+00:00"));
    }

    @Test
    public void test_format_time() {
        assertThat(TimeTZType.formatTime(new TimeTZ(14400000000L)), is("04:00:00"));
        assertThat(TimeTZType.formatTime(new TimeTZ(14400123000L)), is("04:00:00.123"));
        assertThat(TimeTZType.formatTime(new TimeTZ(14400123666L)), is("04:00:00.123666"));
    }

    @Test
    public void test_format_time_with_tz() {
        assertThat(TimeTZType.formatTime(new TimeTZ(14400123000L, 123)),
                   is("04:00:00.123+00:02:03"));
        assertThat(TimeTZType.formatTime(new TimeTZ(14400123666L, 14 * 3600 + 59 * 60)),
                   is("04:00:00.123666+14:59"));
    }
}
