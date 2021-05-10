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

import io.crate.expression.symbol.Literal;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class StringToArrayFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: string_to_array()");
        assertEvaluate("string_to_array()", null);
    }

    @Test
    public void testOneArgument() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: string_to_array('xyz')," +
                                        " no overload found for matching argument types: (text).");
        assertEvaluate("string_to_array('xyz')", null);
    }

    @Test
    public void testEmptyStringInput() {
        assertEvaluate("string_to_array('', '')", List.of());
        assertEvaluate("string_to_array('', 'x')", List.of());
        assertEvaluate("string_to_array('', '', '')", List.of());
    }

    @Test
    public void testNullStringInput() {
        assertEvaluate("string_to_array(null, '')", null);
        assertEvaluate("string_to_array(null, 'x')", null);
        assertEvaluate("string_to_array(null, '', null)", null);
    }

    @Test
    public void testNullSeparator() {
        assertEvaluate("string_to_array('xyz', null)", List.of("x", "y", "z"));
    }

    @Test
    public void testEmptySeparator() {
        assertEvaluate("string_to_array('xyz', '')", List.of("xyz"));
    }

    @Test
    public void testSingleCharacterSeparator() {
        assertEvaluate("string_to_array('x', 'x')", List.of("", ""));
        assertEvaluate("string_to_array('xx', 'x')", List.of("", "", ""));
        assertEvaluate("string_to_array('x', 'y')", List.of("x"));
        assertEvaluate("string_to_array('xyz', 'x')", List.of("", "yz"));
        assertEvaluate("string_to_array('xyz', 'y')", List.of("x", "z"));
        assertEvaluate("string_to_array('xyz', 'z')", List.of("xy", ""));
        assertEvaluate("string_to_array('xyyz', 'y')", List.of("x", "", "z"));
    }

    @Test
    public void testMultipleCharactersSeparator() {
        assertEvaluate("string_to_array('abcdeabcde', 'ab')", List.of("", "cde", "cde"));
        assertEvaluate("string_to_array('cdefgcdefg', 'fg')", List.of("cde", "cde", ""));
        assertEvaluate("string_to_array('abcdefgabc', 'gabc')", List.of("abcdef", ""));
    }

    @Test
    public void testNullStringParameter() {
        assertEvaluate("string_to_array('xyz', '', 'xyz')", Collections.singletonList(null));

        assertEvaluate("string_to_array('xyz', 'xy', 'z')", Arrays.asList("", null));
        assertEvaluate("string_to_array('xyz', 'x', '')", Arrays.asList(null, "yz"));

        assertEvaluate("string_to_array('xyz', null, 'y')", Arrays.asList("x", null, "z"));
        assertEvaluate("string_to_array('xyzy', null, 'y')", Arrays.asList("x", null, "z", null));
    }

    @Test
    public void testNullNullStringParameter() {
        assertEvaluate("string_to_array('xyz', '', null)", List.of("xyz"));
        assertEvaluate("string_to_array(null, '', null)", null);
    }

    @Test
    public void testSingleCharacterSeparatorWithLiterals() {
        assertEvaluate(
            "string_to_array(name, regex_pattern)",
            List.of("", "yz"),
            Literal.of("xyz"), Literal.of("x")
        );
    }

    @Test
    public void testNormalizeSingleCharacterSeparator() {
        assertNormalize("string_to_array('xyz', 'y')", isLiteral(List.of("x", "z")));
    }

    @Test
    public void testNormalizeWithRefs() {
        assertNormalize("string_to_array(name, '')", isFunction("string_to_array"));
    }
}
