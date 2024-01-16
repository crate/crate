/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.Lists;

public class TypeConversionTest extends ESTestCase {

    private Iterable<Byte> bytes(int num) {
        return () -> Stream.generate(ESTestCase::randomByte).limit(num).iterator();
    }

    private Iterable<Integer> integers(final int lower, final int upper, int num) {
        return () -> IntStream.generate(() -> randomIntBetween(lower, upper)).limit(num).iterator();
    }

    @Test
    public void numberConversionTest() throws Exception {
        for (Byte byteVal : bytes(10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.BYTE.id())) {
                var t = DataTypes.fromId(id);
                if (t.equals(DataTypes.IP)) {
                    byteVal = (byte) Math.abs(byteVal == Byte.MIN_VALUE ? byteVal >> 1 : byteVal);
                }
                t.implicitCast(byteVal);
            }
        }

        for (Integer shortVal : integers(Byte.MIN_VALUE, Byte.MAX_VALUE, 10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.SHORT.id())) {
                var t = DataTypes.fromId(id);
                Integer val = t.equals(DataTypes.IP) ? Math.abs(shortVal) : shortVal;
                t.implicitCast(val.shortValue());
            }
        }

        for (Integer intValue : integers(Byte.MIN_VALUE, Byte.MAX_VALUE, 10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.INTEGER.id())) {
                var t = DataTypes.fromId(id);
                int val = t.equals(DataTypes.IP) ? Math.abs(intValue) : intValue;
                t.implicitCast(val);
            }
        }

        for (Integer longValue : integers(Byte.MIN_VALUE, Byte.MAX_VALUE, 10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.LONG.id())) {
                var t = DataTypes.fromId(id);
                Integer val = t.equals(DataTypes.IP) ? Math.abs(longValue) : longValue;
                t.implicitCast(val.longValue());
            }
        }

        for (Integer floatValue : integers(Byte.MIN_VALUE, Byte.MAX_VALUE, 10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.FLOAT.id())) {
                var t = DataTypes.fromId(id);
                Integer val = t.equals(DataTypes.IP) ? Math.abs(floatValue) : floatValue;
                t.implicitCast(val.floatValue());
            }
        }

        for (Integer doubleValue : integers(Byte.MIN_VALUE, Byte.MAX_VALUE, 10)) {
            for (int id : DataTypes.ALLOWED_CONVERSIONS.get(DataTypes.DOUBLE.id())) {
                var t = DataTypes.fromId(id);
                Integer val = t.equals(DataTypes.IP) ? Math.abs(doubleValue) : doubleValue;
                t.implicitCast(val.doubleValue());
            }
        }
    }

    @Test
    public void selfConversionTest() throws Exception {
        for (DataType<?> type : Lists.concat(
            DataTypes.PRIMITIVE_TYPES,
            List.of(DataTypes.UNDEFINED, DataTypes.GEO_POINT, DataTypes.GEO_SHAPE, DataTypes.UNTYPED_OBJECT))) {
            assertThat(
                "type '" + type + "' is not self convertible",
                type.isConvertableTo(type, false), is(true));

            ArrayType<?> arrayType = new ArrayType<>(type);
            assertThat(
                "type '" + arrayType + "' is not self convertible",
                arrayType.isConvertableTo(arrayType, false), is(true));
        }
    }

    @Test
    public void testNotSupportedConversion() throws Exception {
        for (DataType<?> type : Lists.concat(
            DataTypes.PRIMITIVE_TYPES,
            Arrays.asList(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE, DataTypes.UNTYPED_OBJECT))) {

            assertFalse(DataTypes.NOT_SUPPORTED.isConvertableTo(type, false));
        }
    }

    @Test
    public void testToNullConversions() throws Exception {
        for (DataType<?> type : Lists.concat(
            DataTypes.PRIMITIVE_TYPES,
            Arrays.asList(DataTypes.GEO_POINT, DataTypes.GEO_SHAPE, DataTypes.UNTYPED_OBJECT))) {
            assertThat(type.isConvertableTo(DataTypes.UNDEFINED, false), is(false));
        }
        assertThat(DataTypes.UNDEFINED.isConvertableTo(DataTypes.UNDEFINED, false), is(true));
    }

    @Test
    public void testGeoPointConversion() throws Exception {
        assertThat(DataTypes.GEO_POINT.isConvertableTo(new ArrayType<>(DataTypes.DOUBLE), false), is(true));
        assertThat(DataTypes.STRING.isConvertableTo(DataTypes.GEO_POINT, false), is(true));
    }

    @Test
    public void test_conversion_bigint_array_to_geo_point() {
        assertThat(DataTypes.BIGINT_ARRAY.isConvertableTo(GeoPointType.INSTANCE, false), is(true));
    }

    @Test
    public void testGeoShapeConversion() throws Exception {
        assertThat(DataTypes.STRING.isConvertableTo(DataTypes.GEO_SHAPE, false), is(true));
        assertThat(DataTypes.UNTYPED_OBJECT.isConvertableTo(DataTypes.GEO_SHAPE, false), is(true));
    }

    @Test
    public void testTimestampToDoubleConversion() {
        assertThat(TimestampType.INSTANCE_WITH_TZ.isConvertableTo(DoubleType.INSTANCE, false),
            is(true));
        assertThat(TimestampType.INSTANCE_WITHOUT_TZ.isConvertableTo(DoubleType.INSTANCE, false),
            is(true));

    }

    @Test
    public void test_time_to_double_conversion() {
        assertThat(TimeTZType.INSTANCE.isConvertableTo(DoubleType.INSTANCE, false),
                   is(false));
        assertThat(DoubleType.INSTANCE.isConvertableTo(TimeTZType.INSTANCE, false),
                   is(false));
    }

    @Test
    public void test_time_to_long_conversion() {
        assertThat(TimeTZType.INSTANCE.isConvertableTo(LongType.INSTANCE, false),
                   is(false));
        assertThat(LongType.INSTANCE.isConvertableTo(TimeTZType.INSTANCE, false),
                   is(false));
    }

    @Test
    public void test_time_to_string_conversion() {
        assertThat(TimeTZType.INSTANCE.isConvertableTo(StringType.INSTANCE, false),
                   is(false));
        assertThat(StringType.INSTANCE.isConvertableTo(TimeTZType.INSTANCE, false),
                   is(true));
    }

    @Test
    public void test_object_to_object_conversion_when_either_has_no_inner_types() {
        var objectTypeWithInner = ObjectType.builder().setInnerType("field", DataTypes.STRING).build();
        var objectTypeWithoutInner = DataTypes.UNTYPED_OBJECT;

        assertThat(objectTypeWithInner.isConvertableTo(objectTypeWithoutInner, false), is(true));
        assertThat(objectTypeWithoutInner.isConvertableTo(objectTypeWithInner, false), is(true));
    }

    @Test
    public void test_object_to_object_conversion_with_not_convertible_inner_types() {
        var thisObj = ObjectType.builder().setInnerType("field", DataTypes.GEO_POINT).build();
        var thatObj = ObjectType.builder().setInnerType("field", DataTypes.INTEGER).build();

        assertThat(thisObj.isConvertableTo(thatObj, false), is(false));
    }

    @Test
    public void test_allow_conversion_from_object_to_object_with_new_inner_types() {
        // Dynamic objects allow dynamic creation of new sub-columns
        // Because of that we must allow conversions from one object to another
        // where one object contains more or different columns than the other.
        ObjectType objX = ObjectType.builder()
            .setInnerType("x", DataTypes.INTEGER)
            .build();
        ObjectType objY = ObjectType.builder()
            .setInnerType("y", DataTypes.INTEGER)
            .build();

        assertThat(objX.isConvertableTo(objY, false), is(true));
        assertThat(objY.isConvertableTo(objX, false), is(true));
    }

    @Test
    public void test_numeric_type_conversions_to_and_from_primitive_numeric_types() {
        for (DataType<?> type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertThat(
                "numeric is not convertible to type '" + type + "'",
                DataTypes.NUMERIC.isConvertableTo(type, false), is(true));

            assertThat(
                "'" + type + "' is not convertible to numeric type",
                type.isConvertableTo(DataTypes.NUMERIC, false), is(true));
        }
    }
}
