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

public class EndsWithFunctionTest extends ScalarTestCase {

    @Test
    public void test_ends_with_when_suffix_found() {
        assertEvaluate("ends_with('crate', 'te')", true);
    }

    @Test
    public void test_ends_with_when_suffix_not_found() {
        assertEvaluate("ends_with('crate', 'db')", false);
    }

    @Test
    public void test_ends_with_when_suffix_is_empty() {
        assertEvaluate("ends_with('crate', '')", true);
    }

    @Test
    public void test_ends_with_when_string_is_empty() {
        assertEvaluate("ends_with('', 'crate')", false);
    }

    @Test
    public void test_ends_with_when_string_and_suffix_is_empty() {
        assertEvaluate("ends_with('', '')", true);
    }

    @Test
    public void test_ends_with_when_suffix_is_null() {
        assertEvaluateNull("ends_with('crate', null)");
    }

    @Test
    public void test_ends_with_when_string_is_null() {
        assertEvaluateNull("ends_with(null, 'crate')");
    }

    @Test
    public void test_ends_with_when_string_and_suffix_is_null() {
        assertEvaluateNull("ends_with(null, null)");
    }
}
