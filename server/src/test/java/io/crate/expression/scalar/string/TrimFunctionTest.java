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

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isSameInstance;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class TrimFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeTrim() {
        String input = "  Hello World   ";
        String expected = input.trim();
        assertNormalize(String.format("trim(both ' ' from '%s')", input), isLiteral(expected));
        assertNormalize(String.format("trim(' ' from '%s')", input), isLiteral(expected));
        assertNormalize(String.format("trim(from '%s')", input), isLiteral(expected));
        assertNormalize(String.format("trim('%s')", input), isLiteral(expected));
    }

    @Test
    public void test_btrim_trims_both_sides() {
        assertEvaluate("btrim(name)", "Arthur", Literal.of("  Arthur  "));
        assertNormalize("btrim('aaabbbcArthurccbbbaaa', 'abc')", isLiteral("Arthur"));
    }

    @Test
    public void testNormalizeOneSideTrim() {
        String input = "  Hello World   ";
        assertNormalize(String.format("ltrim('%s')", input), isLiteral("Hello World   "));
        assertNormalize(String.format("rtrim('%s')", input), isLiteral("  Hello World"));
    }

    @Test
    public void test_rtrim_trims_right_side_on_evaluate() {
        assertEvaluate("rtrim(name)", "  Arthur", Literal.of("  Arthur  "));
    }

    @Test
    public void testNormalizeOneSideTrimMultiCharsToTrim() {
        String input = "zyxzyzHello Worldzyxzyz";
        assertNormalize(String.format("ltrim('%s', 'xyz')", input), isLiteral("Hello Worldzyxzyz"));
        assertNormalize(String.format("rtrim('%s', 'xyz')", input), isLiteral("zyxzyzHello World"));
    }

    @Test
    public void testNormalizeTrimForEmptyString() {
        assertNormalize("trim('')", isLiteral(""));
        assertNormalize("ltrim('')", isLiteral(""));
        assertNormalize("rtrim('')", isLiteral(""));
        assertNormalize("ltrim('','')", isLiteral(""));
        assertNormalize("rtrim('','')", isLiteral(""));
        assertNormalize("rtrim('','xyz')", isLiteral(""));
        assertNormalize("rtrim('','xyz')", isLiteral(""));
    }

    @Test
    public void testNormalizeTrimForEmptyCharsToTrim() {
        assertNormalize("trim('' from ' hello')", isLiteral(" hello"));
    }

    @Test
    public void testNormalizeTrimMultiCharsToTrim() {
        assertNormalize("trim(leading 'ab' from 'abababcccababab')", isLiteral("cccababab"));
        assertNormalize("trim(trailing 'ab' from 'abababcccababab')", isLiteral("abababccc"));
        assertNormalize("trim(both 'ab' from 'abababcccababab')", isLiteral("ccc"));
        assertNormalize("trim('ab' from 'abababcccababab')", isLiteral("ccc"));
    }

    @Test
    public void testNormalizeTrimTestThatOnlyLeadingOrTrailingCharsAreTrimmed() {
        assertNormalize("trim(leading 'ab' from 'abcabcababab')", isLiteral("cabcababab"));
        assertNormalize("trim(trailing 'ab' from 'abcabcababab')", isLiteral("abcabc"));
        assertNormalize("trim(both 'ab' from 'abcabcababab')", isLiteral("cabc"));
        assertNormalize("trim('ab' from 'abcabcababab')", isLiteral("cabc"));
    }

    @Test
    public void testNormalizeAllCharsAreTrimmed() {
        assertNormalize("trim(leading 'abc' from 'aaabbbcccbbbaaa')", isLiteral(""));
        assertNormalize("trim(trailing 'abc' from 'aaabbbcccbbbaaa')", isLiteral(""));
        assertNormalize("trim(both 'abc' from 'aaabbbcccbbbaaa')", isLiteral(""));
    }

    @Test
    public void testCompileTrimFunctionResultingInOptimisedTrim() {
        assertCompile("trim(both ' ' from name)", isNotSameInstance());
        assertCompile("trim(both 'a' from name)", isNotSameInstance());
        assertCompile("trim('a' from name)", isNotSameInstance());
        assertCompile("trim(initCap('a') from name)", isNotSameInstance());
    }

    @Test
    public void testCompileTrimFunctionResultingInSameFunction() {
        assertCompile("trim(leading ' ' from name)", isSameInstance());
        assertCompile("trim(both 'ab' from name)", isSameInstance());
        assertCompile("trim(both name from name)", isSameInstance());
        assertCompile("trim('ab' from name)", isSameInstance());
        assertCompile("trim(name)", isSameInstance());
        assertCompile("trim(initCap(name) from name)", isSameInstance());
    }

    @Test
    public void testEvaluateOptimisedTrim() {
        assertEvaluate("trim(both ' ' FROM name)", "trim this", Literal.of("  trim this  "));
        assertEvaluate("trim(name)", "trim this", Literal.of("  trim this  "));
    }

    @Test
    public void testEvaluateNonOptimisedTrim() {
        assertEvaluate("trim(leading ' ' FROM name)", "trim this  ", Literal.of("  trim this  "));
        assertEvaluate("trim('ab' from name)", "trim this", Literal.of("ababtrim thisbaba"));
    }

    @Test
    public void testEvaluateTrimWithoutCharsToTrim() {
        assertEvaluate("trim(leading from name)", "trim", Literal.of(" trim"));
        assertEvaluate("trim(both from name)", "trim", Literal.of("  trim  "));
        assertEvaluate("trim(trailing from name)", " trim", Literal.of(" trim  "));
        assertEvaluate("trim(from name)", "trim", Literal.of(" trim "));
    }

    @Test
    public void testEvaluateNullInputOptimisedTrim() {
        assertEvaluateNull("trim(name)", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("ltrim(name)", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("rtrim(name)", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("btrim(name)", Literal.of(DataTypes.STRING, null));

        assertEvaluateNull("trim('foo' FROM name)", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("ltrim(name, 'foo')", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("rtrim(name, 'foo')", Literal.of(DataTypes.STRING, null));
        assertEvaluateNull("btrim(name, 'foo')", Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void test_evaluate_null_second_arg_on_nonnull_input() {
        assertEvaluateNull("trim(NULL FROM name)", Literal.of("foo"));
        assertEvaluateNull("ltrim(name, null)", Literal.of("foo"));
        assertEvaluateNull("rtrim(name, null)", Literal.of("foo"));
        assertEvaluateNull("btrim(name, null)", Literal.of("foo"));
    }

    @Test
    public void testEvaluateNullInputNonOptimisedTrim() {
        assertEvaluateNull("trim('ab' from name)", Literal.of(DataTypes.STRING, null));
    }
}
