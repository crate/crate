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

package io.crate.expression.scalar.systeminformation;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class FormatTypeFunctionTest extends ScalarTestCase {

    @Test
    public void test_format_type_null_oid_returns_null() throws Exception {
        assertEvaluate("format_type(null, null)", null);
    }

    @Test
    public void test_format_type_for_unknown_oid_returns_questionmarks() throws Exception {
        assertEvaluate("format_type(2, null)", "???");
    }

    @Test
    public void test_format_type_for_known_oid_returns_type_name() throws Exception {
        assertEvaluate("format_type(25, null)", "text");
    }

    @Test
    public void test_format_type_return_pg_array_notation_for_array_types() throws Exception {
        assertEvaluate("format_type(1009, null)", "text[]");
    }
}
