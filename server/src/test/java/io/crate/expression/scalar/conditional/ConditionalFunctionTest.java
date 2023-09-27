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

package io.crate.expression.scalar.conditional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class ConditionalFunctionTest extends ScalarTestCase {

    @Test
    public void testCoalesce() throws Exception {
        assertEvaluateNull("coalesce(null)");
        assertEvaluate("coalesce(10, null, 20)", 10);
        assertEvaluate("coalesce(name, 'foo')", "foo", Literal.NULL);
    }

    @Test
    public void testGreatest() throws Exception {
        assertEvaluateNull("greatest(null, null)");
        assertEvaluate("greatest(10)", 10);
        assertEvaluate("greatest(10, 20, null, 30)", 30);
        assertEvaluate("greatest(11.1, 22.2, null)", 22.2);
        assertEvaluate("greatest('foo', name, 'bar')", "foo", Literal.NULL);
    }

    @Test
    public void testLeast() throws Exception {
        assertEvaluateNull("least(null, null)");
        assertEvaluate("least(10)", 10);
        assertEvaluate("least(10, 20, null, 30)", 10);
        assertEvaluate("least(11.1, 22.2, null)", 11.1);
        assertEvaluate("least('foo', name, 'bar')", "bar", Literal.NULL);
    }

    @Test
    public void testNullIf() throws Exception {
        assertEvaluate("nullif(10, 12)", 10);
        assertEvaluateNull("nullif(name, 'foo')", Literal.of("foo"));
        assertEvaluateNull("nullif(null, 'foo')");
    }

    @Test
    public void testNullIfInvalidArgsLength() throws Exception {
        assertThatThrownBy(() -> assertEvaluateNull("nullif(1, 2, 3)"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: nullif(1, 2, 3)," +
                                    " no overload found for matching argument types: (integer, integer, integer).");
    }

    @Test
    public void testOperatorCanBeUsedWithinOperandOfCaseExpression() {
        assertEvaluate("CASE name ~ 'A.*' OR name = 'Trillian' WHEN true THEN 'YES' ELSE 'NO' END",
            "YES",
            Literal.of("Arthur"), Literal.of("Arthur"));
    }

    @Test
    public void testCase() throws Exception {
        assertEvaluate("case name when 'foo' then 'hello foo' when 'bar' then 'hello bar' end",
            "hello foo",
            Literal.of("foo"), Literal.of("foo"));
        assertEvaluate("case name when 'foo' then 'hello foo' when 'bar' then 'hello bar' else 'hello stranger' end",
            "hello stranger",
            Literal.of("hoschi"), Literal.of("hoschi"));
        assertEvaluate("case when name = 'foo' then 'hello foo' when name = 'bar' then 'hello bar' end",
            "hello foo",
            Literal.of("foo"), Literal.of("foo"));
        assertEvaluate("case when name = 'foo' then 'hello foo' when name = 'bar' then 'hello bar' else 'hello stranger' end",
            "hello stranger",
            Literal.of("hoschi"), Literal.of("hoschi"));

        // test that result expression is only evaluated if the condition is true
        assertEvaluate("case when id != 0 then 10/id > 1.5 else false end",
            false,
            Literal.of(0), Literal.of(0));

        // testing nested case statements
        assertEvaluate("case when id != 0 then case when id = 1 then true end else false end",
            true,
            Literal.of(1), Literal.of(1));
    }

    @Test
    public void testCaseIncompatibleTypes() throws Exception {
        assertThatThrownBy(() ->
            assertEvaluate("case name when 'foo' then 'hello foo' when 'bar' then 1 end",
                "hello foo",
                Literal.of("foo"), Literal.of("foo")))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Data types of all result expressions of a CASE statement must be equal, " +
                        "found: [text, integer]");
    }

    @Test
    public void testCaseConditionNotBooleanThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> assertEvaluate("case when 'foo' then x else 1 end", ""))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `boolean`");
    }

    @Test
    public void testIf() throws Exception {
        assertEvaluate("if(id = 0, 'zero', 'other')", "zero", Literal.of(0), Literal.of(0));
        assertEvaluate("if(id = 0, 'zero', if(id = 1, 'one', 'other'))", "one", Literal.of(1), Literal.of(1));
    }

    @Test
    public void testCaseWithDifferentOperandTypes() throws Exception {
        // x = long
        // a = integer
        String expression = "case x + 1 when a::long then 111 end";
        assertEvaluate(expression, 111,
            Literal.of(110L),    // x
            Literal.of(111));    // a
    }

    @Test
    public void testCaseWithNullArgument() throws Exception {
        String expression = "CASE 68 WHEN 38 THEN NULL ELSE 1 END";
        assertEvaluate(expression, 1);
    }

    @Test
    public void testCaseWithNullArgumentReturnsNull() throws Exception {
        String expression = "CASE 38 WHEN 38 THEN NULL ELSE 1 END";
        assertEvaluateNull(expression);
    }

    @Test
    public void testCaseWithNullArgumentElseReturnsNull() throws Exception {
        String expression = "CASE 45 WHEN 38 THEN 1 ELSE NULL END";
        assertEvaluateNull(expression);
    }

    @Test
    public void testCaseWithNullArgumentIfReturnsLong() throws Exception {
        String expression = "CASE 38 WHEN 38 THEN 1 ELSE NULL END";
        assertEvaluate(expression, 1);
    }

    @Test
    public void testCaseWithDifferentResultTypesReturnsLong() throws Exception {
        String expression1 = "CASE 47 WHEN 38 THEN 1 ELSE 12::bigint END";
        assertEvaluate(expression1, 12L);
        String expression2 = "CASE 34 WHEN 38 THEN 1::bigint ELSE 12 END";
        assertEvaluate(expression2, 12L);
    }

    @Test
    public void testCaseWithStringAndLongResultTypesReturnsLong() throws Exception {
        assertEvaluate("CASE 38 WHEN 38 THEN '38' ELSE 40 END",38);
    }

    @Test
    public void testCaseWithStringDefaultTypeReturnsLong() throws Exception {
        assertEvaluate("CASE 45 WHEN 38 THEN 38 WHEN 34 THEN 34 WHEN 80 THEN 80 ELSE '40' END",40);
        assertEvaluate("CASE 34 WHEN 38 THEN 38 WHEN 34 THEN 34 WHEN 80 THEN 80 ELSE '40' END",34);
    }

    /*
     * https://github.com/crate/crate/issues/12674
     */
    @Test
    public void test_large_search_case_results_not_in_stackoverflow() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE \n");
        int n = 10000;
        for (int i = 0; i < n; i++) {
            sb.append(String.format("WHEN %s THEN %s \n", i, i));
        }
        sb.append(String.format("ELSE '%s' END", n));
        assertEvaluate(sb.toString(), 1);
    }

    /*
     * https://github.com/crate/crate/issues/12674
     */
    @Test
    public void test_large_simple_case_results_not_in_stackoverflow() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE 10 - 1 \n");
        int n = 10000;
        for (int i = 0; i < n; i++) {
            sb.append(String.format("WHEN %s THEN %s \n", i, i));
        }
        sb.append(String.format("ELSE '%s' END", n));
        assertEvaluate(sb.toString(), 9);
    }

    @Test
    public void test_null_in_case() throws Exception {
        assertEvaluate("CASE WHEN NULL THEN false WHEN a > 5 THEN true END", true, Literal.of(6));
    }

    @Test
    public void test_in_case_only_true_conditions_should_be_evaluated() {
        assertEvaluate("case when a <= 5 then 0 when a > 5 then 1 / (a - 10) end", 0, Literal.of(4), Literal.of(4), Literal.of(10));
    }

    @Test
    public void test_in_case_default_clause_should_not_be_evaluated() {
        assertEvaluate("case when a <= 5 then 0 else 1 / (a - 10) end", 0, Literal.of(4), Literal.of(10));
    }

}
