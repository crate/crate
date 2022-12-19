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

import java.util.Map;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class ObjectMergeFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_input() {
        assertEvaluateNull("concat(obj, obj)", Literal.NULL, Literal.NULL);
        assertEvaluate("concat(null, {\"a\"=1})", Map.of("a",1));
        assertEvaluate("concat({\"a\"=1}, null)", Map.of("a",1));
    }

    @Test
    public void test_empty_object() {
        assertEvaluate("concat({}, {\"a\"=1})", Map.of("a",1));
        assertEvaluate("concat({\"a\"=1}, {})", Map.of("a",1));
    }

    @Test
    public void test_seconds_overwrites_first_object() {
        assertEvaluate("concat({a=1},{a=2,b=2})", Map.of("a",2,"b",2));
    }

    @Test
    public void test_nested_object() {
        assertEvaluate("concat({b=1},{a=1, b={c=2}})", Map.of("a",1,"b",Map.of("c",2)));
    }

}
