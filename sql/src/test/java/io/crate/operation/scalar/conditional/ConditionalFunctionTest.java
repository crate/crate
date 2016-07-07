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

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

public class ConditionalFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testCoalesce() throws Exception {
        assertEvaluate("coalesce(null)", null);
        assertEvaluate("coalesce(10, null, 20)", 10L);
        assertEvaluate("coalesce(name, 'foo')", "foo", Literal.NULL);
    }

    @Test
    public void testCoalesceInvalidDataType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("all arguments for coalesce function must have the same data type");
        assertEvaluate("coalesce(name, 11.2, 12)", null, Literal.NULL);
    }

    @Test
    public void testNullIf() throws Exception {
        assertEvaluate("nullif(10, 12)", 10L);
        assertEvaluate("nullif(name, 'foo')", null, Literal.newLiteral("foo"));
        assertEvaluate("nullif(null, 'foo')", null);
    }

    @Test
    public void testNullIfInvalidDataType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("all arguments for nullif function must have the same data type");
        assertEvaluate("nullif(name, age)", null, Literal.newLiteral("Trillian"), Literal.newLiteral(32));
    }

    @Test
    public void testNullIfInvalidArgsLength() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("invalid size of arguments, 2 expected");
        assertEvaluate("nullif(1, 2, 3)", null);
    }
}
