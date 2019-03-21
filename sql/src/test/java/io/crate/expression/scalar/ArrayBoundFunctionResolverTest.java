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

import org.junit.Test;

public class ArrayBoundFunctionResolverTest extends AbstractScalarFunctionsTest {

    @Test
    public void testZeroArguments() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: array_upper()");
        assertEvaluate("array_upper()", null);
    }

    @Test
    public void testOneArgument() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: array_lower(bigint_array)");
        assertEvaluate("array_lower([1])", null);
    }

    @Test
    public void testThreeArguments() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The number of arguments is incorrect");
        assertEvaluate("array_lower([1], 2, [3])", null);
    }

    @Test
    public void testSecondArgumentNotANumber() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast [2] to type integer");
        assertEvaluate("array_lower([1], [2])", null);
    }

    @Test
    public void testFirstArgumentIsNull() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The first argument of the array_upper function cannot be undefined");
        assertEvaluate("array_upper(null, 1)", null);
    }
}
