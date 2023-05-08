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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class ArrayUnnestFunctionTest extends ScalarTestCase {

    @Test
    public void test_unnest_nested_list() throws Exception {
        assertEvaluate("array_unnest([[1, 2], [3, 4, 5]])", List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void test_unnest_nested_nested_list() throws Exception {
        assertEvaluate(
            "array_unnest([ [[1, 2], [3, 4, 5]],  [[1]] ])",
            List.of(
                List.of(1, 2),
                List.of(3, 4, 5),
                List.of(1)
            ));
    }

    @Test
    public void test_null_arg_results_in_null() throws Exception {
        assertEvaluate("array_unnest(null)", t -> assertThat(t).isNull());
    }

    @Test
    public void test_inner_null_arrays_are_skipped() throws Exception {
        assertEvaluate("array_unnest([[1, 2], null, [5, 6]])", List.of(1, 2, 5, 6));
    }
}
