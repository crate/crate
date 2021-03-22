package io.crate.types;

import org.junit.Test;

import static io.crate.testing.Asserts.assertThrows;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class DateTypeTest {

    @Test
    public void testCastFromInvalidString() {
        assertThrows(() -> DateType.INSTANCE.implicitCast("not-a-number"),
            ClassCastException.class,
            "Can't cast 'not-a-number' to date");
    }

    @Test
    public void testCastString() {
        assertThat(DateType.INSTANCE.implicitCast("123"), is(123L));
    }

    @Test
    public void testCastDateString() {
        assertThat(DateType.INSTANCE.implicitCast("2020-02-09"), is(1581206400000L));
    }

    @Test
    public void testCastFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(123.123f), is(123123L));
    }

    @Test
    public void testCastNumericNonFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(123), is(123L));
    }

    @Test
    public void testCastNull() {
        assertThat(DateType.INSTANCE.implicitCast(null), is(nullValue()));
    }




}
