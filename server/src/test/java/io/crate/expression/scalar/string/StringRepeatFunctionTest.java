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

package io.crate.expression.scalar.string;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class StringRepeatFunctionTest extends ScalarTestCase {

    @Test
    public void test_repeat_zero_times_returns_empty_string() {
        assertEvaluate("repeat('test', 0)", "");
    }

    @Test
    public void test_repeat_negative_number_times_returns_empty_string() {
        assertEvaluate("repeat('test', -1)", "");
    }

    @Test
    public void test_repeat_empty_string_returns_empty_string() {
        assertEvaluate("repeat('', 3)", "");
    }

    @Test
    public void test_repeat_positive_number_times() {
        assertEvaluate("repeat('test', 3)", "testtesttest");
    }

    @Test
    public void test_repeat_null_text_argument_returns_null() {
        assertEvaluateNull("repeat(null, 1)");
    }

    @Test
    public void test_repeat_null_repetitions_argument_returns_null() {
        assertEvaluateNull("repeat('', null)");
    }
}
