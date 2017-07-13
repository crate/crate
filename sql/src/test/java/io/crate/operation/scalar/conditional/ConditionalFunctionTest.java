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

package io.crate.operation.scalar.conditional;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;

public class ConditionalFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testArgsLength() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("dummy function requires at least one argument");
        ConditionalFunction.createInfo("dummy", Collections.<DataType>emptyList());
    }

    @Test
    public void testInvalidDataType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("all arguments for dummy function must have the same data type");
        ConditionalFunction.createInfo("dummy",
            ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.INTEGER));
    }

    @Test
    public void testCoalesce() throws Exception {
        assertEvaluate("coalesce(null)", null);
        assertEvaluate("coalesce(10, null, 20)", 10L);
        assertEvaluate("coalesce(name, 'foo')", "foo", Literal.NULL);
    }

    @Test
    public void testGreatest() throws Exception {
        assertEvaluate("greatest(null, null)", null);
        assertEvaluate("greatest(10)", 10L);
        assertEvaluate("greatest(10, 20, null, 30)", 30L);
        assertEvaluate("greatest(11.1, 22.2, null)", 22.2);
        assertEvaluate("greatest('foo', name, 'bar')", "foo", Literal.NULL);
    }

    @Test
    public void testLeast() throws Exception {
        assertEvaluate("least(null, null)", null);
        assertEvaluate("least(10)", 10L);
        assertEvaluate("least(10, 20, null, 30)", 10L);
        assertEvaluate("least(11.1, 22.2, null)", 11.1);
        assertEvaluate("least('foo', name, 'bar')", "bar", Literal.NULL);
    }

    @Test
    public void testNullIf() throws Exception {
        assertEvaluate("nullif(10, 12)", 10L);
        assertEvaluate("nullif(name, 'foo')", null, Literal.of("foo"));
        assertEvaluate("nullif(null, 'foo')", null);
    }

    @Test
    public void testNullIfInvalidArgsLength() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: nullif(long, long, long)");
        assertEvaluate("nullif(1, 2, 3)", null);
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
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Data types of all result expressions of a CASE statement must be equal, " +
                                        "found: [string, long]");
        assertEvaluate("case name when 'foo' then 'hello foo' when 'bar' then 1 end",
            "hello foo",
            Literal.of("foo"), Literal.of("foo"));
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
        String expression = "case x + 1 when a then 111 end";
        assertEvaluate(expression, 111L,
            Literal.of(110L),    // x
            Literal.of(111));    // a
    }

    @Test
    public void testCaseWithNullArgument() throws Exception {
        String expression = "CASE 68 WHEN 38 THEN NULL ELSE 1 END";
        assertEvaluate(expression, 1L);
    }

    @Test
    public void testCaseWithNullArgumentReturnsNull() throws Exception {
        String expression = "CASE 38 WHEN 38 THEN NULL ELSE 1 END";
        assertEvaluate(expression, null);
    }

    @Test
    public void testCaseWithNullArgumentElseReturnsNull() throws Exception {
        String expression = "CASE 45 WHEN 38 THEN 1 ELSE NULL END";
        assertEvaluate(expression, null);
    }

    @Test
    public void testCaseWithNullArgumentIfReturnsLong() throws Exception {
        String expression = "CASE 38 WHEN 38 THEN 1 ELSE NULL END";
        assertEvaluate(expression, 1L);
    }
}
