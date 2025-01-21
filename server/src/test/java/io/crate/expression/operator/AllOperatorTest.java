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

package io.crate.expression.operator;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class AllOperatorTest extends ScalarTestCase {

    @Test
    public void test_value_eq_all_array_is_false_if_no_item_matches() {
        assertEvaluate("1 = ALL(ARRAY[2, 3])", false);
    }

    @Test
    public void test_val_eq_all_array_is_false_if_subset_matches() {
        assertEvaluate("1 = ALL(ARRAY[1, 2])", false);
    }

    @Test
    public void test_val_eq_all_array_is_true_if_all_match() {
        assertEvaluate("1 = ALL(ARRAY[1, 1])", true);
    }

    @Test
    public void test_null_eq_all_array_is_null() {
        assertEvaluateNull("null = ALL(ARRAY[1, 1])");
    }

    @Test
    public void test_value_eq_all_null_is_null() {
        assertEvaluateNull("1 = ALL(NULL)");
    }

    @Test
    public void test_value_eq_empty_array_is_true() {
        assertEvaluate("1 = ALL(ARRAY[]::array(INTEGER))", true);
    }

    @Test
    public void test_automatically_levels_array_dimensions_to_left_arg() {
        assertEvaluate("1 = ALL([ [1] ])", true);
    }

    @Test
    public void test_nested_array() {
        assertEvaluate("[1] = ALL([ [1] ])", true);
    }

    @Test
    public void test_value_eq_array_with_null_element_and_match_is_null() {
        assertEvaluateNull("1 = ALL(ARRAY[1, null])");
    }

    @Test
    public void test_value_eq_array_with_null_element_and_no_match_is_false() {
        assertEvaluate("1 = ALL(ARRAY[2, null])", false);
    }

    @Test
    public void test_all_can_be_used_with_all_basic_cmp_operators() {
        assertEvaluate("1 = ALL(ARRAY[1, 1])", true);
        assertEvaluate("2 >= ALL(ARRAY[1, 1])", true);
        assertEvaluate("2 > ALL(ARRAY[1, 1])", true);
        assertEvaluate("1 <= ALL(ARRAY[1, 1])", true);
        assertEvaluate("0 < ALL(ARRAY[1, 1])", true);
        assertEvaluate("3 <> ALL(ARRAY[1, 2])", true);
    }
}
