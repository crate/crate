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

import org.junit.Test;

public class AsciiFunctionTest extends ScalarTestCase {

    @Test
    public void test_ascii_returns_code_of_first_character() throws Exception {
        assertEvaluate("ascii('a')", 97);
        assertEvaluate("ascii('ab')", 97);
    }

    @Test
    public void test_ascii_returns_0_for_empty_string() throws Exception {
        assertEvaluate("ascii('')", 0);
    }

    @Test
    public void test_ascii_returns_null_for_null_arg() throws Exception {
        assertEvaluate("ascii(null)", null);
    }

    @Test
    public void test_ascii_with_unicode_arg() throws Exception {
        assertEvaluate("ascii('💩')", 128169);
    }
}
