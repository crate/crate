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

package io.crate.expression.scalar.object;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;

public class ObjectKeysFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_input() {
        assertEvaluateNull("object_keys(null)");
    }

    @Test
    public void test_array_input() {
        assertThatThrownBy(() -> assertEvaluateNull("object_keys([1])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: object_keys(_array(1)), no overload found for " +
                        "matching argument types: (integer_array). Possible candidates: " +
                        "object_keys(object):array(text)");
    }

    @Test
    public void test_empty_object() {
        assertEvaluate("object_keys({})", List.of());
    }

    @Test
    public void test_flat_object() {
        assertEvaluate("object_keys({a=1, b=2})", List.of("a","b"));
    }

    @Test
    public void test_nested_object() {
        assertEvaluate("object_keys({a=1, b={c=2}})", List.of("a","b"));
    }

}
