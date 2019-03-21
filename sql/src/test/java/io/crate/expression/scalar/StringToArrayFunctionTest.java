/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class StringToArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testZeroArguments() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: string_to_array()");
        assertEvaluate("string_to_array()", null);
    }

    @Test
    public void testOneArgument() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: string_to_array(text)");
        assertEvaluate("string_to_array('xyz')", null);
    }

    @Test
    public void testEmptyStringInput() {
        assertEvaluate("string_to_array('', '')", new String[]{});
        assertEvaluate("string_to_array('', 'x')", new String[]{});
        assertEvaluate("string_to_array('', '', '')", new String[]{});
    }

    @Test
    public void testNullStringInput() {
        assertEvaluate("string_to_array(null, '')", null);
        assertEvaluate("string_to_array(null, 'x')", null);
        assertEvaluate("string_to_array(null, '', null)", null);
    }

    @Test
    public void testNullSeparator() {
        assertEvaluate("string_to_array('xyz', null)", new String[]{"x", "y", "z"});
    }

    @Test
    public void testEmptySeparator() {
        assertEvaluate("string_to_array('xyz', '')", new String[]{"xyz"});
    }

    @Test
    public void testSingleCharacterSeparator() {
        assertEvaluate("string_to_array('x', 'x')", new String[]{"", ""});
        assertEvaluate("string_to_array('xx', 'x')", new String[]{"", "", ""});
        assertEvaluate("string_to_array('x', 'y')", new String[]{"x"});
        assertEvaluate("string_to_array('xyz', 'x')", new String[]{"", "yz"});
        assertEvaluate("string_to_array('xyz', 'y')", new String[]{"x", "z"});
        assertEvaluate("string_to_array('xyz', 'z')", new String[]{"xy", ""});
        assertEvaluate("string_to_array('xyyz', 'y')", new String[]{"x", "", "z"});
    }

    @Test
    public void testMultipleCharactersSeparator() {
        assertEvaluate("string_to_array('abcdeabcde', 'ab')", new String[]{"", "cde", "cde"});
        assertEvaluate("string_to_array('cdefgcdefg', 'fg')", new String[]{"cde", "cde", ""});
        assertEvaluate("string_to_array('abcdefgabc', 'gabc')", new String[]{"abcdef", ""});
    }

    @Test
    public void testNullStringParameter() {
        assertEvaluate("string_to_array('xyz', '', 'xyz')", new String[]{null});

        assertEvaluate("string_to_array('xyz', 'xy', 'z')", new String[]{"", null});
        assertEvaluate("string_to_array('xyz', 'x', '')", new String[]{null, "yz"});

        assertEvaluate("string_to_array('xyz', null, 'y')", new String[]{"x", null, "z"});
        assertEvaluate("string_to_array('xyzy', null, 'y')", new String[]{"x", null, "z", null});
    }

    @Test
    public void testNullNullStringParameter() {
        assertEvaluate("string_to_array('xyz', '', null)", new String[]{"xyz"});
        assertEvaluate("string_to_array(null, '', null)", null);
    }

    @Test
    public void testSingleCharacterSeparatorWithLiterals() {
        assertEvaluate(
            "string_to_array(name, regex_pattern)",
            new String[]{"", "yz"},
            Literal.of("xyz"), Literal.of("x")
        );
    }

    @Test
    public void testNormalizeSingleCharacterSeparator() {
        assertNormalize("string_to_array('xyz', 'y')", isLiteral(new String[]{"x", "z"}));
    }

    @Test
    public void testNormalizeWithRefs() {
        assertNormalize("string_to_array(name, '')", isFunction("string_to_array"));
    }
}
