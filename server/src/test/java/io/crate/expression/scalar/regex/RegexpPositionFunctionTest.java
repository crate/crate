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

package io.crate.expression.scalar.regex;

import static io.crate.testing.Asserts.isNotSameInstance;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class RegexpPositionFunctionTest extends ScalarTestCase {

    @Test
    public void test_noMatch() {
        assertEvaluate("regexp_instr(name, 'crate')", 0, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_firstMatch() {
        assertEvaluate("regexp_instr(name, 'b..')", 4, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_secondMatch() {
        assertEvaluate("regexp_instr(name, 'b..', 1, 2)", 7, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_matchFromPosition() {
        assertEvaluate("regexp_instr(name, 'b..', 10, 1)", 12, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_endOption() {
        assertEvaluate("regexp_instr(name, 'b..', 10, 1, 1)", 15, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_flags() {
        assertEvaluate("regexp_instr(name, 'B..', 10, 1, 1, 'i')", 15, Literal.of("foobarbequebaz"));
        assertEvaluate("regexp_instr(name, 'B..', 10, 1, 1, '')", 0, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_subExpr() {
        assertEvaluate("regexp_instr(name, 'O(B..).*(B..)', 1, 1, 0, 'i', 0)", 3, Literal.of("foobarbequebaz"));
        assertEvaluate("regexp_instr(name, 'O(B..).*(B..)', 1, 1, 0, 'i', 1)", 4, Literal.of("foobarbequebaz"));
        assertEvaluate("regexp_instr(name, 'O(B..).*(B..)', 1, 1, 0, 'i', 2)", 12, Literal.of("foobarbequebaz"));
        assertEvaluate("regexp_instr(name, 'O(B..).*(B..)', 1, 1, 1, 'i', 2)", 15, Literal.of("foobarbequebaz"));
    }

    @Test
    public void test_nulls() {
        assertEvaluateNull("regexp_instr(null, 'pattern')");
        assertEvaluateNull("regexp_instr('abc', null)");
        assertEvaluateNull("regexp_instr('abc', 'pattern', null)");
        assertEvaluateNull("regexp_instr('abc', 'pattern', 1, null)");
        assertEvaluateNull("regexp_instr('abc', 'pattern', 1, 1, null)");
        assertEvaluateNull("regexp_instr('abc', 'pattern', 1, 1, 0, null)");
    }

    @Test
    public void testCompile() {
        assertCompile("regexp_instr(name, 'O(B..).*(B..)')", isNotSameInstance());
        assertCompile("regexp_instr(name, 'O(B..).*(B..)')", scalar -> compiledScalar -> {
            assertThat(scalar).isInstanceOf(RegexpPositionFunction.class);
            assertThat(compiledScalar).isInstanceOf(RegexpPositionFunction.class);
            assertThat(((RegexpPositionFunction) scalar).getPattern()).isNull();
            assertThat(((RegexpPositionFunction) compiledScalar).getPattern()).isNotNull();
            assertThat(((RegexpPositionFunction) compiledScalar).getPattern().toString()).isEqualTo("O(B..).*(B..)");
        });
    }
}
