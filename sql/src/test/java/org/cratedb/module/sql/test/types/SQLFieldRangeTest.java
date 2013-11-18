package org.cratedb.module.sql.test.types;

import org.cratedb.sql.ValidationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

@RunWith( value = Parameterized.class)
public class SQLFieldRangeTest extends SQLFieldMapperTest {
    private String fieldName;
    private Number[] testNumbers;
    public Class<?> klass;

    public SQLFieldRangeTest(String fieldName, Number[] testNumbers, Class<?> klass) {
        this.fieldName = fieldName;
        this.testNumbers = testNumbers;
        this.klass = klass;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"byte_field", new Number[]{Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0,
                    Byte.MAX_VALUE, Byte.MAX_VALUE + 1}, Integer.class},
                {"short_field", new Number[]{Short.MIN_VALUE - 1, Short.MIN_VALUE, 0,
                        Short.MAX_VALUE,
                        Short.MAX_VALUE + 1}, Integer.class},
                {"integer_field", new Number[]{Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE, Integer.MAX_VALUE + 1L}, Integer.class}
        };
        return Arrays.asList(data);
    }

    @Test
    public void testRange() {
        try {
            this.mapper.convertToXContentValue(fieldName, testNumbers[0]);
            fail("did not validate lower bound");
        } catch(ValidationException e) {}

        assertThat(this.mapper.convertToXContentValue(fieldName, testNumbers[1]),
                instanceOf(this.klass));
        assertThat(
                this.mapper.convertToXContentValue(fieldName, testNumbers[2]),
                instanceOf(this.klass));
        assertThat(this.mapper.convertToXContentValue(fieldName,
                testNumbers[3]),
                instanceOf(this.klass));
        try {
            this.mapper.convertToXContentValue(fieldName, testNumbers[4]);
            fail("did not validate upper bound");
        } catch(ValidationException e) {}
    }
}
