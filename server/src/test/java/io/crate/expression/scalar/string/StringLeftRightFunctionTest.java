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
import io.crate.expression.symbol.Literal;

public class StringLeftRightFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateLeftFunc() throws Exception {
        Literal<String> value = Literal.of("crate.io");
        assertEvaluateNull("left(name, 10)", Literal.of((String) null));
        assertEvaluateNull("left(name, null)", value);
        assertEvaluate("left(name, 0)", "", value);
        assertEvaluate("left(name, 100)", "", Literal.of(""));
        assertEvaluate("left(name, 5)", "crate", value);
        assertEvaluate("left(name, -3)", "crate", value);
        assertEvaluate("left('crate.io', age)", "crate", Literal.of(5));
        assertEvaluate("left('crate.io', age)", "crate", Literal.of(-3));
    }

    @Test
    public void testEvaluateRightFunc() throws Exception {
        Literal<String> value = Literal.of("crate.io");
        assertEvaluateNull("right(name, 10)", Literal.of((String) null));
        assertEvaluateNull("right(name, null)", value);
        assertEvaluate("right(name, 0)", "", value);
        assertEvaluate("right(name, 100)", "", Literal.of(""));
        assertEvaluate("right(name, 2)","io", value);
        assertEvaluate("right(name, -6)","io", value);
        assertEvaluate("right('crate.io', age)","io", Literal.of(2));
        assertEvaluate("right('crate.io', age)","io", Literal.of(-6));
    }
}
