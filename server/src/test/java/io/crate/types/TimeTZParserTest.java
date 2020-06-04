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
        assertThat(parse("12"), isTZ(43200000000L, 0));
        assertThat(parse("12.000001"), isTZ(43200000001L, 0));
        assertThat(parse("1200"), isTZ(43200000000L, 0));
        assertThat(parse("1200.002"), isTZ(43200002000L, 0));
        assertThat(parse("120000"), isTZ(43200000000L, 0));
        assertThat(parse("120000.000003"), isTZ(43200000003L, 0));
    }

    @Test
    public void test_parse_time_correct_syntax_colon() {
        assertThat(parse("12:00"), isTZ(43200000000L, 0));
        assertThat(parse("12:00.999"), isTZ(43200999000L, 0));
        assertThat(parse("12:00:00"), isTZ(43200000000L, 0));
        assertThat(parse("12:00:00.003"), isTZ(43200003000L, 0));
    }

    @Test
    public void test_parse_time_correct_syntax_tz() {
        assertThat(parse("00+12"), isTZ(0L, 12 * 3600));
        assertThat(parse("00+1200"), isTZ(0L, 12 * 3600));
        assertThat(parse("00-12:00"), isTZ(0L, -12 * 3600));
        assertThat(parse("00.0+12"), isTZ(0L, 12 * 3600));
        assertThat(parse("00.0+1200"), isTZ(0L, 12 * 3600));
        assertThat(parse("00.000001  +12:00"), isTZ(1L, 12 * 3600));
    }

    @Test
    public void test_parse_time_range_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '24:00:00.000001' could not be parsed: Invalid value for HourOfDay (valid values 0 - 23): 24");
        parse("24:00:00.000001");
    }

    @Test
    public void test_parse_time_unsupported_literal_long() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '234' could not be parsed, unparsed text found at index 2");
        parse("234");
    }

    @Test
    public void test_parse_time_unsupported_literal_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '234.9999' could not be parsed, unparsed text found at index 2");
        parse("234.9999");
    }

    @Test
    public void test_parse_time_out_of_range_hh() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '25' could not be parsed: Invalid value for HourOfDay (valid values 0 - 23): 25");
        parse("25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '1778' could not be parsed: Invalid value for MinuteOfHour (valid values 0 - 59): 78");
        parse("1778");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '175978' could not be parsed: Invalid value for SecondOfMinute (valid values 0 - 59): 78");
        parse("175978");
    }

    @Test
    public void test_parse_time_out_of_range_hh_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for HourOfDay (valid values 0 - 23): 25");
        parse("25.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for MinuteOfHour (valid values 0 - 59): 78");
        parse("1778.999999");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss_floating_point() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Text '175978.999999' could not be parsed: Invalid value for SecondOfMinute (valid values 0 - 59): 78");
        parse("175978.999999");
    }

    @Test
    public void test_parse_time() {
        assertThat(parse("04"), isTZ(4 * 60 * 60 * 1000_000L, 0));
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
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400000000L, 0)), is("04:00:00"));
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123000L, 0)), is("04:00:00.123"));
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123666L, 65)), is("04:00:00.123666+00:01:05"));
    }

    @Test
    public void test_format_time_with_tz() {
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123000L, 123)),
                   is("04:00:00.123+00:02:03"));
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123666L, 14 * 3600 + 59 * 60)),
                   is("04:00:00.123666+14:59"));
    }
}
