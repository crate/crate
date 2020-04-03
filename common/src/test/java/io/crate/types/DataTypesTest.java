/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.crate.types.DataTypes.compareTypesById;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;

public class DataTypesTest extends CrateUnitTest {

    @Test
    public void testConvertBooleanToString() {
        String value = DataTypes.STRING.value(true);
        assertEquals("t", value);
    }

    @Test
    public void testConvertStringToBoolean() {
        assertEquals(true, DataTypes.BOOLEAN.value("t"));
        assertEquals(false, DataTypes.BOOLEAN.value("false"));
        assertEquals(false, DataTypes.BOOLEAN.value("FALSE"));
        assertEquals(false, DataTypes.BOOLEAN.value("f"));
        assertEquals(false, DataTypes.BOOLEAN.value("F"));
        assertEquals(true, DataTypes.BOOLEAN.value("true"));
        assertEquals(true, DataTypes.BOOLEAN.value("TRUE"));
        assertEquals(true, DataTypes.BOOLEAN.value("t"));
        assertEquals(true, DataTypes.BOOLEAN.value("T"));
    }

    @Test
    public void testLongToNumbers() {
        Long longValue = 123L;

        assertEquals((Long) 123L, DataTypes.LONG.value((longValue)));
        assertEquals((Integer) 123, DataTypes.INTEGER.value(longValue));
        assertEquals((Double) 123.0d, DataTypes.DOUBLE.value(longValue));
        assertEquals((Float) 123.0f, DataTypes.FLOAT.value(longValue));
        assertEquals((Short) (short) 123, DataTypes.SHORT.value(longValue));
        assertEquals((Byte) (byte) 123, DataTypes.BYTE.value(longValue));
        assertEquals((Long) 123L, DataTypes.TIMESTAMPZ.value(longValue));
        assertEquals("123", DataTypes.STRING.value(longValue));
    }

    private static Map<String, Object> testMap = new HashMap<String, Object>() {{
        put("int", 1);
        put("boolean", false);
        put("double", 2.8d);
        put("list", Arrays.asList(1, 3, 4));
    }};

    private static Map<String, Object> testCompareMap = new HashMap<String, Object>() {{
        put("int", 2);
        put("boolean", true);
        put("double", 2.9d);
        put("list", Arrays.asList(9, 9, 9, 9));
    }};

    @Test
    public void testConvertToObject() {
        DataType objectType = ObjectType.untyped();
        assertThat(objectType.value(testMap), is(testMap));
    }

    @Test(expected = ClassCastException.class)
    public void testMapToBoolean() {
        DataTypes.BOOLEAN.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToLong() {
        DataTypes.LONG.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToInteger() {
        DataTypes.INTEGER.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertToDouble() {
        DataTypes.DOUBLE.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertToFloat() {
        DataTypes.FLOAT.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertToShort() {
        DataTypes.SHORT.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertToByte() {
        DataTypes.BYTE.value(testMap);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertMapToTimestamp() {
        DataTypes.TIMESTAMPZ.value(testMap);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompareTo() {
        Map testMapCopy = Map.copyOf(testMap);
        Map emptyMap = Map.of();
        DataType objectType = ObjectType.untyped();

        assertThat(objectType.compare(testMap, testMapCopy), is(0));
        assertThat(objectType.compare(testMapCopy, testMap), is(0));

        // first number of argument is checked
        assertThat(objectType.compare(testMap, emptyMap), is(1));
        assertThat(objectType.compare(emptyMap, testMap), is(-1));

        // then values
        assertThat(objectType.compare(testMap, testCompareMap), is(1));
        assertThat(objectType.compare(testCompareMap, testMap), is(1));
    }

    @Test
    public void testStringConvertToNumbers() {
        String value = "123";

        assertEquals((Long) 123L, DataTypes.LONG.value(value));
        assertEquals((Integer) 123, DataTypes.INTEGER.value(value));
        assertEquals((Double) 123.0d, DataTypes.DOUBLE.value(value));
        assertEquals((Float) 123.0f, DataTypes.FLOAT.value(value));
        assertEquals((Short) (short) 123, DataTypes.SHORT.value(value));
        assertEquals((Byte) (byte) 123, DataTypes.BYTE.value(value));
        assertEquals("123", DataTypes.STRING.value(value));
    }

    @Test(expected = NumberFormatException.class)
    public void testConvertToUnsupportedNumberConversion() {
        DataTypes.LONG.value("hello");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToUnsupportedBooleanConversion() {
        DataTypes.BOOLEAN.value("hello");
    }


    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToLong() {
        DataTypes.LONG.value(true);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToInteger() {
        DataTypes.INTEGER.value(true);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToDouble() {
        DataTypes.DOUBLE.value(true);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToFloat() {
        DataTypes.FLOAT.value(true);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToShort() {
        DataTypes.SHORT.value(true);
    }

    @Test(expected = ClassCastException.class)
    public void testConvertBooleanToByte() {
        DataTypes.BYTE.value(true);
    }

    @Test
    public void testLongTypeCompareValueToWith() {
        assertCompareValueTo(DataTypes.LONG, null, null, 0);
        assertCompareValueTo(null, 2L, -1);
        assertCompareValueTo(3L, 2L, 1);
        assertCompareValueTo(2L, 2L, 0);
        assertCompareValueTo(2L, null, 1);
    }

    @Test
    public void testShortTypeCompareValueToWith() {
        assertCompareValueTo(DataTypes.LONG, null, null, 0);
        assertCompareValueTo(null, (short) 2, -1);
        assertCompareValueTo((short) 3, (short) 2, 1);
        assertCompareValueTo((short) 2, (short) 2, 0);
        assertCompareValueTo((short) 2, null, 1);
    }

    @Test
    public void testIntTypeCompareValueTo() {
        assertCompareValueTo(DataTypes.INTEGER, null, null, 0);
        assertCompareValueTo(null, 2, -1);
        assertCompareValueTo(3, 2, 1);
        assertCompareValueTo(2, 2, 0);
        assertCompareValueTo(2, null, 1);
    }

    @Test
    public void testDoubleTypeCompareValueTo() {
        assertCompareValueTo(DataTypes.DOUBLE, null, null, 0);
        assertCompareValueTo(null, 2.0d, -1);
        assertCompareValueTo(3.0d, 2.0d, 1);
        assertCompareValueTo(2.0d, 2.0d, 0);
        assertCompareValueTo(2.0d, null, 1);
    }

    @Test
    public void testFloatTypeCompareValueTo() {
        assertCompareValueTo(DataTypes.FLOAT, null, null, 0);
        assertCompareValueTo(null, 2.0f, -1);
        assertCompareValueTo(2.0f, 3.0f, -1);
        assertCompareValueTo(2.0f, 2.0f, 0);
        assertCompareValueTo(2.0f, null, 1);
    }

    @Test
    public void testByteTypeCompareValueTo() {
        assertCompareValueTo(DataTypes.BYTE, null, null, 0);
        assertCompareValueTo(null, (byte) 2, -1);
        assertCompareValueTo((byte) 3, (byte) 2, 1);
        assertCompareValueTo((byte) 2, (byte) 2, 0);
        assertCompareValueTo((byte) 2, null, 1);
    }

    @Test
    public void testBooleanTypeCompareValueTo() {
        assertCompareValueTo(DataTypes.BOOLEAN, null, null, 0);
        assertCompareValueTo(null, true, -1);
        assertCompareValueTo(true, false, 1);
        assertCompareValueTo(true, null, 1);
    }

    @Test
    public void testSmallIntIsAliasedToShort() {
        assertThat(DataTypes.ofName("smallint"), is(DataTypes.SHORT));
    }

    @Test
    public void testInt2IsAliasedToShort() {
        assertThat(DataTypes.ofName("int2"), is(DataTypes.SHORT));
    }

    @Test
    public void testInt4IsAliasedToInteger() {
        assertThat(DataTypes.ofName("int4"), is(DataTypes.INTEGER));
    }

    @Test
    public void testBigIntIsAliasedToLong() {
        assertThat(DataTypes.ofName("bigint"), is(DataTypes.LONG));
    }

    @Test
    public void testInt8IsAliasedToLong() {
        assertThat(DataTypes.ofName("int8"), is(DataTypes.LONG));
    }

    @Test
    public void test_compare_by_id_primitive_types() {
        assertThat(compareTypesById(DataTypes.STRING, DataTypes.STRING), is(true));
        assertThat(compareTypesById(DataTypes.INTEGER, DataTypes.DOUBLE), is(false));
    }

    @Test
    public void test_compare_by_id_complex_types() {
        assertThat(compareTypesById(ObjectType.untyped(), DataTypes.BIGINT_ARRAY), is(false));
        assertThat(compareTypesById(ObjectType.untyped(), DataTypes.GEO_POINT), is(false));
    }

    @Test
    public void test_compare_by_id_primitive_and_complex_types() {
        assertThat(compareTypesById(DataTypes.STRING_ARRAY, DataTypes.STRING), is(false));
        assertThat(compareTypesById(ObjectType.untyped(), DataTypes.DOUBLE), is(false));
    }

    @Test
    public void test_compare_by_id_array_types_of_the_same_dimension() {
        assertThat(compareTypesById(DataTypes.STRING_ARRAY, DataTypes.STRING_ARRAY), is(true));
        assertThat(compareTypesById(DataTypes.STRING_ARRAY, DataTypes.BIGINT_ARRAY), is(false));
    }

    @Test
    public void test_compare_by_id_array_types_of_not_equal_dimension_and_same_inner_type() {
        assertThat(
            compareTypesById(
                new ArrayType<>(DataTypes.STRING_ARRAY),
                DataTypes.STRING_ARRAY),
            is(false));
    }

    private static void assertCompareValueTo(Object val1, Object val2, int expected) {
        DataType type = DataTypes.guessType(Objects.requireNonNullElse(val1, val2));
        assertThat(type, not(instanceOf(DataTypes.UNDEFINED.getClass())));
        assertCompareValueTo(type, val1, val2, expected);
    }

    private static void assertCompareValueTo(DataType dt, Object val1, Object val2, int expected) {
        if (val1 == null || val2 == null) {
            assertThat(Comparator.nullsFirst(dt).compare(dt.value(val1), dt.value(val2)), is(expected));
        } else {
            assertThat(dt.compare(dt.value(val1), dt.value(val2)), is(expected));
        }
    }
}
