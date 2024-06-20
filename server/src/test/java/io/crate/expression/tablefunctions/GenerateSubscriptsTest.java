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

package io.crate.expression.tablefunctions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class GenerateSubscriptsTest extends AbstractTableFunctionsTest {

    @Test
    public void test_null_array_returns_empty_table() {
        assertExecute("generate_subscripts(null, 1)", "");
    }

    @Test
    public void test_empty_array_returns_empty_table() {
        assertExecute("generate_subscripts([], 1)", "");
    }

    @Test
    public void test_null_value_for_reverse_attribute_is_equiv_to_false() {
        assertExecute("generate_subscripts([1], 1, null)", "1\n");
    }

    @Test
    public void test_null_value_for_dim_attribute_returns_empty_table() {
        assertExecute("generate_subscripts([1], null)", "");
    }

    @Test
    public void test_null_values_in_array_produce_a_subscript() {
        assertExecute("generate_subscripts([null, null, null], 1)", "1\n2\n3\n");
        assertExecute("generate_subscripts([[null], [null], [null]], 2)", "1\n");
    }

    @Test
    public void test_greater_dimension_than_the_array_has_returns_empty_table() {
        assertExecute("generate_subscripts([1], 2)", "");
    }

    @Test
    public void test_multidimensional_arrays_must_be_of_consistent_dimension() {
        assertExecute("generate_subscripts([[[1],[2]], [[3],[4]], [[4],[5]]], 1)", "1\n2\n3\n");
        assertExecute("generate_subscripts([[[1],[2]], [[3],[4]], [[4],[5]]], 2)", "1\n2\n");
        assertExecute("generate_subscripts([[[1],[2]], [[3],[4]], [[4],[5]]], 3)", "1\n");
    }

    @Test
    public void test_multidimensional_arrays_of_inconsistent_dimension_are_not_supported() {
        assertThatThrownBy(() ->
                assertExecute("generate_subscripts([[[1],[2]], [[3],[4]], [[4]]], 3)", "1\n2\n3\n"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("nested arrays must have the same dimension within a level, offending level 2, position 3");
    }

    @Test
    public void test_null_values_also_produce_a_subscript() {
        assertExecute("generate_subscripts([null, null, null], 1)", "1\n2\n3\n");
        assertExecute("generate_subscripts([[null], [null], [null]], 2)", "1\n");
        assertExecute("generate_subscripts([[[1], null], [[3], [null]], [null, null]], 1)", "1\n2\n3\n");
        assertExecute("generate_subscripts([[[1], null], [[3], [null]], [null, null]], 2)", "1\n2\n");
        assertExecute("generate_subscripts([ [[1], null], [[3], [null]], [null, null]], 3)", "1\n");
    }

    public void test_reversed() {
        assertExecute("generate_subscripts([1, 2, 3, 200, null, -1], 1, true)", "6\n5\n4\n3\n2\n1\n");
    }
}
