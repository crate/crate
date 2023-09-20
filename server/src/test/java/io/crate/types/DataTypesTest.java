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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class DataTypesTest extends ESTestCase {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCompareTo() {
        var testMap = Map.of(
            "int", 1,
            "boolean", false,
            "double", 2.8d,
            "list", List.of(1, 3, 4)
        );

        var testCompareMap = Map.of(
            "int", 2,
            "boolean", true,
            "double", 2.9d,
            "list", List.of(9, 9, 9, 9)
        );

        Map testMapCopy = Map.copyOf(testMap);
        Map emptyMap = Map.of();
        DataType objectType = DataTypes.UNTYPED_OBJECT;

        assertThat(objectType.compare(testMap, testMapCopy), is(0));
        assertThat(objectType.compare(testMapCopy, testMap), is(0));

        // first number of argument is checked
        assertThat(objectType.compare(testMap, emptyMap), is(1));
        assertThat(objectType.compare(emptyMap, testMap), is(-1));

        // then values
        assertThat(objectType.compare(testMap, testCompareMap), is(-1));
        assertThat(objectType.compare(testCompareMap, testMap), is(1));
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
    public void test_varchar_is_aliased_to_string() throws Exception {
        assertThat(DataTypes.ofName("varchar"), is(DataTypes.STRING));
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
    public void testFloat4IsAliasedToReal() {
        assertThat(DataTypes.ofName("float4"), is(DataTypes.FLOAT));
    }

    @Test
    public void testFloat8IsAliasedToDouble() {
        assertThat(DataTypes.ofName("float8"), is(DataTypes.DOUBLE));
    }

    @Test
    public void testDecimalIsAliasedToNumeric() {
        assertThat(DataTypes.ofName("decimal"), is(DataTypes.NUMERIC));
    }

    @Test
    public void test_is_same_type_on_primitive_types() {
        assertThat(DataTypes.isCompatibleType(DataTypes.STRING, DataTypes.STRING), is(true));
        assertThat(DataTypes.isCompatibleType(DataTypes.INTEGER, DataTypes.DOUBLE), is(false));
    }

    @Test
    public void test_is_same_type_on_complex_types() {
        assertThat(DataTypes.isCompatibleType(DataTypes.UNTYPED_OBJECT, DataTypes.BIGINT_ARRAY), is(false));
        assertThat(DataTypes.isCompatibleType(DataTypes.UNTYPED_OBJECT, DataTypes.GEO_POINT), is(false));
    }

    @Test
    public void test_is_same_type_on_primitive_and_complex_types() {
        assertThat(DataTypes.isCompatibleType(DataTypes.STRING_ARRAY, DataTypes.STRING), is(false));
        assertThat(DataTypes.isCompatibleType(DataTypes.UNTYPED_OBJECT, DataTypes.DOUBLE), is(false));
    }

    @Test
    public void test_is_same_type_on_array_types_of_the_same_dimension() {
        assertThat(DataTypes.isCompatibleType(DataTypes.STRING_ARRAY, DataTypes.STRING_ARRAY), is(true));
        assertThat(DataTypes.isCompatibleType(DataTypes.STRING_ARRAY, DataTypes.BIGINT_ARRAY), is(false));
    }

    @Test
    public void test_is_same_type_on_array_types_of_not_equal_dimension_and_same_inner_type() {
        assertThat(
            DataTypes.isCompatibleType(
                new ArrayType<>(DataTypes.STRING_ARRAY),
                DataTypes.STRING_ARRAY),
            is(false));
    }

    @Test
    public void test_resolve_text_data_type_with_length_limit() {
        assertThat(DataTypes.of("varchar", List.of(1)), is(StringType.of(1)));
    }

    @Test
    public void test_resolve_data_type_that_does_not_support_parameters_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The 'integer' type doesn't support type parameters.");
        DataTypes.of("integer", List.of(1));
    }

    private static void assertCompareValueTo(Object val1, Object val2, int expected) {
        DataType<?> type = DataTypes.guessType(Objects.requireNonNullElse(val1, val2));
        assertThat(type, not(instanceOf(DataTypes.UNDEFINED.getClass())));
        assertCompareValueTo(type, val1, val2, expected);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void assertCompareValueTo(DataType dt, Object val1, Object val2, int expected) {
        if (val1 == null || val2 == null) {
            assertThat(
                Comparator.nullsFirst(dt).compare(
                    dt.sanitizeValue(val1),
                    dt.sanitizeValue(val2)
                ),
                is(expected)
            );
        } else {
            assertThat(dt.compare(dt.sanitizeValue(val1), dt.sanitizeValue(val2)), is(expected));
        }
    }

    @Test
    public void test_merge_method_with_primitive_types() {
        assertThat(DataTypes.merge(DataTypes.INTEGER, DataTypes.STRING)).isEqualTo(DataTypes.INTEGER);
        assertThatThrownBy(() -> DataTypes.merge(DataTypes.IP, DataTypes.DATE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("'date' is not convertible to 'ip'");
    }

    @Test
    public void test_merge_method_with_primitive_type_and_container_type() {
        assertThatThrownBy(() -> DataTypes.merge(DataTypes.INTEGER_ARRAY, DataTypes.INTEGER))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("'integer' is not convertible to 'integer_array'");
        assertThatThrownBy(() -> DataTypes.merge(DataTypes.UNTYPED_OBJECT, DataTypes.INTEGER))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("'integer' is not convertible to 'object'");
    }

    @Test
    public void test_merge_method_with_object_types() {
        assertThat(
            DataTypes.merge(
                ObjectType.builder()
                    .setInnerType("x", DataTypes.INTEGER)
                    .setInnerType("y", DataTypes.STRING)
                    .setInnerType("z", DataTypes.CHARACTER)
                    .build(),
                ObjectType.builder()
                    .setInnerType("w", DataTypes.SHORT)
                    .setInnerType("x", DataTypes.STRING)
                    .setInnerType("y", DataTypes.INTEGER)
                    .build()))
            .isEqualTo(
                ObjectType.builder()
                    .setInnerType("w", DataTypes.SHORT)
                    .setInnerType("x", DataTypes.INTEGER)
                    .setInnerType("y", DataTypes.INTEGER)
                    .setInnerType("z", DataTypes.CHARACTER)
                    .build()
            );

        assertThatThrownBy(
            () -> DataTypes.merge(
                ObjectType.builder().setInnerType("a", DataTypes.INTEGER_ARRAY).build(),
                ObjectType.builder().setInnerType("a", DataTypes.DATE).build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("'date' is not convertible to 'integer_array'");
    }

    @Test
    public void test_merge_method_with_nulls() {
        assertThat(DataTypes.merge(DataTypes.UNDEFINED, DataTypes.UNDEFINED)).isEqualTo(DataTypes.UNDEFINED);
        assertThat(DataTypes.merge(DataTypes.UNDEFINED, DataTypes.INTEGER)).isEqualTo(DataTypes.INTEGER);
        assertThat(DataTypes.merge(DataTypes.INTEGER, DataTypes.UNDEFINED)).isEqualTo(DataTypes.INTEGER);
        assertThat(DataTypes.merge(DataTypes.UNTYPED_OBJECT, DataTypes.UNDEFINED)).isEqualTo(DataTypes.UNTYPED_OBJECT);
    }

    @Test
    public void test_merge_method_with_nested_object_arrays() {
        assertThat(
            DataTypes.merge(
                // left type
                ObjectType.builder()
                    .setInnerType("a", DataTypes.INTEGER_ARRAY)
                    .setInnerType("b", DataTypes.STRING_ARRAY)
                    .setInnerType("c",
                                  new ArrayType<>(
                                      ObjectType.builder()
                                          .setInnerType("h", DataTypes.INTEGER)
                                          .setInnerType("j",
                                                        ObjectType.builder()
                                                            .setInnerType("k", DataTypes.STRING).build())
                                          .build())
                    ).build(),
                // right type
                ObjectType.builder()
                    .setInnerType("a", DataTypes.STRING_ARRAY)
                    .setInnerType("b", DataTypes.INTEGER_ARRAY)
                    .setInnerType("c",
                                  new ArrayType<>(
                                      ObjectType.builder()
                                          .setInnerType("h", DataTypes.STRING)
                                          .setInnerType("j",
                                                        ObjectType.builder()
                                                            .setInnerType("k", DataTypes.INTEGER)
                                                            .setInnerType("l", DataTypes.INTEGER).build())
                                          .build())
                    ).build()
                )
        ).isEqualTo(
            // merged type
            ObjectType.builder()
                .setInnerType("a", DataTypes.INTEGER_ARRAY)
                .setInnerType("b", DataTypes.INTEGER_ARRAY)
                .setInnerType("c",
                              new ArrayType<>(
                                  ObjectType.builder()
                                      .setInnerType("h", DataTypes.INTEGER)
                                      .setInnerType("j",
                                                    ObjectType.builder()
                                                        .setInnerType("k", DataTypes.INTEGER)
                                                        .setInnerType("l", DataTypes.INTEGER).build())
                                      .build())
                ).build()
        );
    }
}
