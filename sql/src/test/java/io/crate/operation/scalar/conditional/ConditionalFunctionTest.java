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
        assertEvaluate("greatest(null)", null);
        assertEvaluate("greatest(10, 20, 30)", 30L);
        assertEvaluate("greatest(name, 'bar', 'foo')", "foo", Literal.NULL);
    }

    @Test
    public void testLeast() throws Exception {
        assertEvaluate("least(null)", null);
        assertEvaluate("least(10, 20, 30)", 10L);
        assertEvaluate("least(name, 'foo', 'bar')", "bar", Literal.NULL);
    }

    @Test
    public void testNullIf() throws Exception {
        assertEvaluate("nullif(10, 12)", 10L);
        assertEvaluate("nullif(name, 'foo')", null, Literal.newLiteral("foo"));
        assertEvaluate("nullif(null, 'foo')", null);
    }

    @Test
    public void testNullIfInvalidArgsLength() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("invalid size of arguments, 2 expected");
        assertEvaluate("nullif(1, 2, 3)", null);
    }
}
