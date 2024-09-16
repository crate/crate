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

package io.crate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Point;

import io.crate.data.RowN;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.Regproc;
import io.crate.types.RowType;
import io.crate.types.TimeTZ;
import io.crate.types.TimeTZType;
import io.crate.types.UndefinedType;

public class DataTypeTest extends ESTestCase {

    @Test
    public void testForValueWithList() {
        List<String> strings = Arrays.asList("foo", "bar");
        DataType<?> dataType = DataTypes.guessType(strings);
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.STRING));

        List<Integer> integers = Arrays.asList(1, 2, 3);
        dataType = DataTypes.guessType(integers);
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.INTEGER));
    }

    @Test
    public void testForValueWithArray() {
        Boolean[] booleans = new Boolean[]{true, false};
        DataType<?> dataType = DataTypes.guessType(booleans);
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.BOOLEAN));
    }

    @Test
    public void testForValueWithTimestampArrayAsString() {
        String[] strings = {"2013-09-10T21:51:43", "2013-11-10T21:51:43"};
        DataType<?> dataType = DataTypes.guessType(strings);
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testForValueWithObjectList() {
        Map<String, Object> objA = new HashMap<>();
        objA.put("a", 1);

        Map<String, Object> objB = new HashMap<>();
        Map<String, Object> objBNested = new HashMap<>();

        objB.put("b", objBNested);
        objBNested.put("bn1", 1);
        objBNested.put("bn2", 2);

        List<Object> objects = List.of(objA, objB);
        DataType<?> dataType = DataTypes.guessType(objects);
        var expectedObjectType = ObjectType.builder()
            .setInnerType("a", DataTypes.INTEGER)
            .setInnerType(
                "b",
                ObjectType.builder()
                    .setInnerType("bn1", DataTypes.INTEGER)
                    .setInnerType("bn2", DataTypes.INTEGER)
                    .build()
            )
            .build();
        assertThat(dataType).isEqualTo(new ArrayType<>(expectedObjectType));
    }

    @Test
    public void testForValueWithArrayWithNullValues() {
        DataType<?> dataType = DataTypes.guessType(new String[]{"foo", null, "bar"});
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testForValueNestedList() {
        List<List<String>> nestedStrings = Arrays.asList(
            Arrays.asList("foo", "bar"),
            Arrays.asList("f", "b"));
        assertThat(DataTypes.guessType(nestedStrings)).isEqualTo(
            new ArrayType<>(new ArrayType<>(DataTypes.STRING)));
    }

    @Test
    public void testForValueMixedDataTypeInList() {
        List<Object> objects = Arrays.<Object>asList("foo", List.of(1));
        assertThatThrownBy(() -> DataTypes.guessType(objects))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testListCanContainMixedTypeIfSafeCastIsPossible() {
        List<Object> objects = Arrays.asList(1, null, 1.2f, 0);
        Collections.shuffle(objects);
        DataType<?> dataType = DataTypes.guessType(objects);
        assertThat(dataType).isEqualTo(new ArrayType<>(DataTypes.FLOAT));
    }

    @Test
    public void testForValueWithEmptyList() {
        List<Object> objects = Arrays.<Object>asList();
        DataType<?> type = DataTypes.guessType(objects);
        assertThat(type).isEqualTo(new ArrayType<>(DataTypes.UNDEFINED));
    }

    @Test
    public void test_is_compatible_type_with_same_names_different_inner_types() {
        ObjectType obj1 = ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build();
        ObjectType obj2 = ObjectType.builder().setInnerType("a", DataTypes.STRING).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isFalse();
    }

    @Test
    public void test_is_compatible_type_with_same_names_same_inner_types() {
        ObjectType obj1 = ObjectType.builder().setInnerType("a", DataTypes.STRING).build();
        ObjectType obj2 = ObjectType.builder().setInnerType("a", DataTypes.STRING).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isTrue();
    }

    @Test
    public void test_is_compatible_type_with_different_names_same_inner_types() {
        ObjectType obj1 = ObjectType.builder().setInnerType("a", DataTypes.STRING).build();
        ObjectType obj2 = ObjectType.builder().setInnerType("b", DataTypes.STRING).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isTrue();
    }

    @Test
    public void test_is_compatible_type_on_nested_types_with_same_names_same_inner_types() {
        ObjectType obj1 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        ObjectType obj2 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isTrue();
    }

    @Test
    public void test_is_compatible_type_on_nested_types_with_same_names_different_inner_types() {
        ObjectType obj1 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        ObjectType obj2 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("a", DataTypes.STRING).build()).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isFalse();
    }

    @Test
    public void test_is_compatible_type_on_nested_types_with_different_names_same_inner_types() {
        ObjectType obj1 = ObjectType.builder()
            .setInnerType("obj1",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        ObjectType obj2 = ObjectType.builder()
            .setInnerType("obj2",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        assertThat(DataTypes.isCompatibleType(obj1, obj2)).isTrue();

        ObjectType obj3 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("a", DataTypes.INTEGER).build()).build();
        ObjectType obj4 = ObjectType.builder()
            .setInnerType("obj",
                          ObjectType.builder().setInnerType("b", DataTypes.INTEGER).build()).build();
        assertThat(DataTypes.isCompatibleType(obj3, obj4)).isTrue();
    }

    @Test
    public void test_string_value_size() throws Exception {
        assertThat(DataTypes.STRING.valueBytes(null)).isEqualTo(0L);
        assertThat(DataTypes.STRING.valueBytes("hello")).isEqualTo(56L);
    }

    @Test
    public void test_byte_value_size() throws Exception {
        assertThat(DataTypes.BYTE.valueBytes(null)).isEqualTo(16L);
        assertThat(DataTypes.BYTE.valueBytes(Byte.valueOf("100"))).isEqualTo(16L);
    }

    @Test
    public void test_geopoint_value_size() throws Exception {
        assertThat(DataTypes.GEO_POINT.valueBytes(null)).isEqualTo(DataTypes.GEO_POINT.fixedSize());
        Point value = DataTypes.GEO_POINT.implicitCast(new Double[]{1.0d, 2.0d});
        assertThat(DataTypes.GEO_POINT.valueBytes(value)).isEqualTo(40L);
    }

    @Test
    public void test_undefined_type_estimate_size_for_null_value() {
        assertThat(DataTypes.UNDEFINED.valueBytes(null)).isEqualTo(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
    }

    @Test
    public void test_undefined_type_estimate_size_for_string_value() {
        assertThat(DataTypes.UNDEFINED.valueBytes("")).isEqualTo(RamUsageEstimator.sizeOfObject(""));
    }

    @Test
    public void test_undefined_array_type_estimate_size_for_null_value() {
        var type = new ArrayType<>(DataTypes.UNDEFINED);
        assertThat(type.valueBytes(null)).isEqualTo(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER);
    }

    @Test
    public void test_undefined_array_type_estimate_size_for_object() {
        UndefinedType undefined = DataTypes.UNDEFINED;
        var type = new ArrayType<>(undefined);
        assertThat(type.valueBytes(List.of("", ""))).isEqualTo(
            undefined.valueBytes("") + undefined.valueBytes("") + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER);
    }

    @Test
    public void test_regproc_type_estimate_size_for_value() {
        assertThat(DataTypes.REGPROC.valueBytes(null)).isEqualTo(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER);
        assertThat(DataTypes.REGPROC.valueBytes(Regproc.of("test"))).isEqualTo(64L);
    }

    @Test
    public void test_numeric_type_estimate_size_for_value() {
        assertThat(DataTypes.NUMERIC.valueBytes(null)).isEqualTo(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER);
        assertThat(DataTypes.NUMERIC.valueBytes(BigDecimal.valueOf(1))).isEqualTo(37L);
        assertThat(DataTypes.NUMERIC.valueBytes(BigDecimal.valueOf(Long.MAX_VALUE))).isEqualTo(44L);
    }

    @Test
    public void test_estimate_size_of_record() throws Exception {
        var type = new RowType(List.of(DataTypes.LONG, DataTypes.INTEGER));
        assertThat(type.valueBytes(new RowN(20L, 10))).isEqualTo(40L);
    }

    @Test
    public void test_guess_timetz_type() {
        assertThat(DataTypes.guessType(new TimeTZ(46800000000L, 0))).isEqualTo(TimeTZType.INSTANCE);
    }

    @Test
    public void test_guess_unknown_type() {
        record Dummy() {
        }

        assertThatThrownBy(() -> DataTypes.guessType(new Dummy()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot detect the type of the value: Dummy[]");
    }
}
