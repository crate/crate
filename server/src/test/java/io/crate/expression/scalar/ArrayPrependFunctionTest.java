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

import static io.crate.testing.Asserts.isLiteral;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class ArrayPrependFunctionTest extends ScalarTestCase {

    @Test
    public void test_prepending_element() {
        assertNormalize("array_prepend(1, [2, 3])", isLiteral(List.of(1, 2, 3)));
    }

    @Test
    public void test_prepending_null() {
        assertNormalize("array_prepend(null, [2, 3])", isLiteral(Arrays.asList(null, 2, 3)));
    }

    @Test
    public void test_prepend_to_null_array() {
        assertNormalize("array_prepend(1, null)", isLiteral(List.of(1)));
    }

    @Test
    public void test_prepend_null_to_null() {
        assertNormalize("array_prepend(null, null)", isLiteral(Collections.singletonList(null)));
    }

    @Test
    public void test_prepend_with_different_types() {
        assertNormalize("array_prepend(1.0, [2, 3])", isLiteral(List.of(1.0, 2.0, 3.0)));
    }
}
