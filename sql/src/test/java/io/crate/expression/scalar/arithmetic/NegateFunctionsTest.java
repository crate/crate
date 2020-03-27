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

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static org.hamcrest.Matchers.nullValue;


public class NegateFunctionsTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNegateReference() throws Exception {
        assertNormalize("- age", isFunction("_negate"));
    }

    @Test
    public void testNegateOnStringResultsInError() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: _negate(text)");
        assertEvaluate("- name", nullValue(), Literal.of("foo"));
    }

    @Test
    public void testNegateInteger() throws Exception {
        assertEvaluate("- age", -4, Literal.of(4));
    }

    @Test
    public void testNegateNull() throws Exception {
        assertEvaluate("- null", null);
    }

    @Test
    public void testNegateLong() throws Exception {
        assertEvaluate("- cast(age as long)", -4L, Literal.of(4L));
    }

    @Test
    public void testNegateDouble() throws Exception {
        assertEvaluate("- cast(age as double)", -4.2d, Literal.of(4.2d));
    }

    @Test
    public void testNegateUndefinedType() throws Exception {
        assertEvaluate("- - (case 3 when 1 then 1 else Null end) + 1 ", null);
    }
}
