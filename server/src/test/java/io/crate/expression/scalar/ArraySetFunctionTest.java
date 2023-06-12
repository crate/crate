/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ConversionException;

public class ArraySetFunctionTest extends ScalarTestCase {

    @Test
    public void test_indexes_out_of_range() {
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,1,1], [-2147483649], [2])"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `-2147483649::bigint` of type `bigint` to type `integer`");
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,1,1], [2147483648], [2])"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `2147483648::bigint` of type `bigint` to type `integer`");
    }

    @Test
    public void test_long_indexes_that_fit_into_integer() {
        assertEvaluate("array_set([1,2,3], [3::long], [-1])", List.of(1, 2, -1));
    }

    @Test
    public void test_set_to_long_values_that_fit_into_integer() {
        assertEvaluate("array_set([1,2,3], [3::long], [-1::long])", List.of(1L, 2L, -1L));
    }

    @Test
    public void test_appending_value_at_the_end() {
        assertEvaluate("array_set([1,2,3], [4], [-1])", List.of(1, 2, 3, -1));
    }

    @Test
    public void test_null_padding_added_in_between_array_set_index_and_max_index_of_source_array() {
        ArrayList<Integer> expected = new ArrayList<>(List.of(1, 2, 3));
        expected.add(null);
        expected.add(-1);
        assertEvaluate("array_set([1,2,3], [5], [-1])", expected);  // expected = [1, 2, 3, null, -1]
    }

    @Test
    public void test_negative_index() {
        assertThatThrownBy(
            () -> assertEvaluate(
                "array_set([1,2,3], [0, -1], [0, -1])",
                List.of(-1, 0, 1, 2, 3)))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Updating arrays with indexes <= 0 is not supported");
    }

    @Test
    public void test_empty_array_for_target_indexes_and_empty_array_for_target_values() {
        assertEvaluate("array_set([1,2,3], [], [])", List.of(1, 2, 3));
    }

    @Test
    public void test_null_for_target_indexes_and_null_for_target_values() {
        assertEvaluate("array_set([1,2,3], null, null)", List.of(1, 2, 3));
    }

    @Test
    public void test_either_target_indexes_or_target_values_are_null() {
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,2,3], null, [1])"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`array_set(array, indexes, values)`: the size of indexes and values must match or both be nulls");
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,2,3], [1], null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`array_set(array, indexes, values)`: the size of indexes and values must match or both be nulls");
    }

    @Test
    public void test_unmatched_number_of_target_values_and_target_indexes() {
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,2,3], [1], [])"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`array_set(array, indexes, values)`: the size of indexes and values must match or both be nulls");
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,2,3], [], [1])"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("`array_set(array, indexes, values)`: the size of indexes and values must match or both be nulls");
    }

    @Test
    public void test_source_is_null() {
        assertEvaluateNull("array_set(null, [1], [1])");
        assertEvaluateNull("array_set(null, null, null)");
    }

    @Test
    public void test_source_is_empty_array() {
        assertEvaluate("array_set([], [1], [1])", List.of(1));
    }

    @Test
    public void test_set_single_index_multiple_times() {
        assertEvaluate("array_set([1,2,3], [1,1,1], [1,2,3])", List.of(3,2,3));
    }

    @Test
    public void test_array_set_single_index() throws Exception {
        assertEvaluate("array_set([1, 2, 3], 2, 10)", List.of(1, 10, 3));
    }

    @Test
    public void test_array_set_single_index_null_array_is_null() throws Exception {
        assertEvaluate("array_set(null, 2, 10)", value -> assertThat(value).isNull());
    }

    @Test
    public void test_array_set_single_null_index_is_null() throws Exception {
        assertEvaluate("array_set([1, 2, 3], null, 10)", value -> assertThat(value).isNull());
    }

    @Test
    public void test_array_set_single_index_null_value() throws Exception {
        assertEvaluate("array_set([1, 2, 3], 2, null)", Arrays.asList(1, null, 3));
    }

    @Test
    public void test_set_single_array_element_to_wrong_type() {
        assertThatThrownBy(() -> assertEvaluateNull("array_set([1,2,3], [1], ['a'])"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'a'` of type `text` to type `integer`");
    }
}
