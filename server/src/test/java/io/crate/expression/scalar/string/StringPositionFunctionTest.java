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

package io.crate.expression.scalar.string;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class StringPositionFunctionTest extends ScalarTestCase {

    @Test
    public void find_position_when_substring_found() {

        assertEvaluate("strpos('crate', 'ate')", 3);
    }

    @Test
    public void find_position_when_substring_not_found() {
        assertEvaluate("strpos('crate', 'db')", 0);
    }

    @Test
    public void test_multiple_occurrences() {
        assertEvaluate("strpos('This is crate', 'is')", 3);
    }

    @Test
    public void test_when_substring_is_empty() {
        assertEvaluate("strpos('ThIs IS crate', '')", 1);
    }

    public void test_when_string_is_empty() {
        assertEvaluate("strpos('', 'crate')", 0);
    }

    public void test_when_string_and_substring_is_empty() {
        assertEvaluate("strpos('', '')", 1);
    }

    public void test_when_substring_is_null() {
        assertEvaluateNull("strpos('This is Crate', null)");
    }

    public void test_when_string_is_null() {
        assertEvaluateNull("strpos(null, 'crate')");
    }

    public void test_when_string_and_substring_is_null() {
        assertEvaluateNull("strpos(null, null)");
    }

    @Test
    public void test_position_returns_same_value_as_strpos() {
        assertEvaluate("strpos('crate','ate') = position('ate' in 'crate')", true);
    }

}
