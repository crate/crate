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

import org.junit.Test;

public class ArrayOverlapFunctionTest extends ScalarTestCase {

    @Test
    public void test_nulls() {
        assertNormalize("array_overlap([1, 2], null)", isLiteral(null));
        assertNormalize("array_overlap(null, [1, 2])", isLiteral(null));
    }

    @Test
    public void test_overlap() {
        assertNormalize("array_overlap([1, 2], [2, 3])", isLiteral(true));
        assertNormalize("array_overlap([1, 2], [3, 4])", isLiteral(false));
    }

    @Test
    public void test_overlap_nested_arrays() {
        assertNormalize("array_overlap([[1, 2], [2, 3]], [[2, 3], [4, 5]])", isLiteral(true));
        assertNormalize("array_overlap([[1, 2], [2, 3]], [[2, 4], [4, 5]])", isLiteral(false));
    }

    @Test
    public void test_overlap_operator() {
        assertNormalize("[1, 2] && [2, 3]", isLiteral(true));
    }
}
