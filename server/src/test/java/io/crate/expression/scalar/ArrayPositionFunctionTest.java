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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.assertThrowsMatches;

import org.junit.Test;

import io.crate.exceptions.ConversionException;

public class ArrayPositionFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_position_find_from_given_position() {
        assertEvaluate(
            "array_position([null, 'array','position','function','test','crate','db'], 'crate', 3)",
            6);
    }

    @Test
    public void test_array_position_find_text_in_int_array_with_value_effectively_equal() {
        assertEvaluate("array_position([3,4,1,4,6], '3')", 1);
    }

    @Test
    public void test_array_position_find_null() {
        assertEvaluate("array_position([3,2,null,4,6], null)", 3);
    }

    @Test
    public void test_array_position_find_array_of_objects() {
        assertEvaluate("array_position([[{id=101 ,name='John'}, {id=102 ,name='Harry'}], [{id=103 ,name='San'}]], [{id=103 , name='San'}])",
            2);
    }

    @Test
    public void test_array_position_find_element_when_begin_position_given_beyond_target_index() {
        assertEvaluate("array_position([3,2,1,5,4,6,2], 2, 3)", 7);
    }

    @Test
    public void test_array_position_find_element_when_begin_position_null_does_not_fail() {
        assertEvaluate("array_position([3,2,1,5,4,6], 5, null)", 4);
    }

    @Test
    public void test_array_position_find_element_when_begin_position_given_cast_compatible_returns_success() {
        assertEvaluate("array_position([3,2,1,5,4,6], 2, 1.4)", 2);
    }

    @Test
    public void test_array_position_with_empty_array() {
        assertEvaluateNull("array_position(cast([] as array(integer)), 14)");
    }

    @Test
    public void test_array_position_with_null_array() {
        assertEvaluateNull("array_position(null, 1)");
    }

    @Test
    public void test_array_position_find_element_when_begin_position_given_zero_or_negative_then_ignored() {
        assertEvaluateNull("array_position([3,2,1,5,4,6], 4, 0)");
    }

    @Test
    public void test_array_position_find_element_when_begin_position_given_greater_than_array_size_then_ignored() {
        assertEvaluateNull("array_position([3,2,1,5,4], 4, 6)");
    }

    @Test
    public void test_array_position_find_text_in_int_array_with_start_position_provided_throws_error() {
        assertThrowsMatches(() -> assertEvaluate("array_position([3,4,1,4,6], 'a', 3)", null),
            ConversionException.class, "Cannot cast `'a'` of type `text` to type `integer`");
    }
}
