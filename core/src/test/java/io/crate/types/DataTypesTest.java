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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class DataTypesTest extends CrateUnitTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testConvertBooleanToString() {
        BytesRef value = DataTypes.STRING.value(true);
        assertEquals(new BytesRef("t"), value);
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

        assertEquals((Long)123L, DataTypes.LONG.value((longValue)));
        assertEquals((Integer)123, DataTypes.INTEGER.value(longValue));
        assertEquals((Double)123.0d, DataTypes.DOUBLE.value(longValue));
        assertEquals((Float)123.0f, DataTypes.FLOAT.value(longValue));
        assertEquals((Short)(short)123, DataTypes.SHORT.value(longValue));
        assertEquals((Byte)(byte)123, DataTypes.BYTE.value(longValue));
        assertEquals((Long)123L, DataTypes.TIMESTAMP.value(longValue));
        assertEquals(new BytesRef("123"), DataTypes.STRING.value(longValue));
    }

    private static Map<String, Object> testMap = new HashMap<String, Object>(){{
        put("int", 1);
        put("boolean", false);
        put("double", 2.8d);
        put("list", Arrays.asList(1, 3, 4));
    }};

    private static Map<String, Object> testCompareMap = new HashMap<String, Object>(){{
        put("int", 2);
        put("boolean", true);
        put("double", 2.9d);
        put("list", Arrays.asList(9, 9, 9, 9));
    }};

    @Test
    public void testConvertToObject() {
        assertThat(DataTypes.OBJECT.value(testMap), is(testMap));
    }

    @Test( expected = ClassCastException.class)
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
        DataTypes.TIMESTAMP.value(testMap);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompareTo() {
        Map testMapCopy = ImmutableMap.copyOf(testMap);
        Map emptyMap = ImmutableMap.of();

        assertThat(DataTypes.OBJECT.compareValueTo(testMap, testMapCopy), is(0));
        assertThat(DataTypes.OBJECT.compareValueTo(testMapCopy, testMap), is(0));

        // first number of argument is checked
        assertThat(DataTypes.OBJECT.compareValueTo(testMap, emptyMap), is(1));
        assertThat(DataTypes.OBJECT.compareValueTo(emptyMap, testMap), is(-1));

        // then values
        assertThat(DataTypes.OBJECT.compareValueTo(testMap, testCompareMap), is(1));
        assertThat(DataTypes.OBJECT.compareValueTo(testCompareMap, testMap), is(1));
    }

    @Test
    public void testSetTypeCompareToSameLength() {
        HashSet<String> set1 = Sets.newHashSet("alpha", "bravo", "charlie", "delta");
        HashSet<String> set2 = Sets.newHashSet("foo", "alpha", "beta", "bar");

        // only length is compared, not the actual values
        DataType type = new SetType(DataTypes.STRING);
        assertThat(type.compareValueTo(set1, set2), is(0));
    }

    @Test
    public void testSetTypeCompareToDifferentLength() {
        HashSet<String> set1 = Sets.newHashSet("alpha", "bravo", "charlie", "delta");
        HashSet<String> set2 = Sets.newHashSet("foo", "alpha", "beta");

        DataType type = new SetType(DataTypes.STRING);
        assertThat(type.compareValueTo(set1, set2), is(1));
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
        assertEquals(new BytesRef("123"), DataTypes.STRING.value(value));
    }

    @Test
    public void testConvertStringsToTimestamp() {
        assertEquals((Long)1393555173000L, DataTypes.TIMESTAMP.value("2014-02-28T02:39:33"));
        assertEquals((Long)1393545600000L, DataTypes.TIMESTAMP.value("2014-02-28"));
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
}