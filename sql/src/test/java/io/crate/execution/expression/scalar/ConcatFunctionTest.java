/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.execution.expression.scalar;

import io.crate.analyze.symbol.Literal;
import io.crate.exceptions.ConversionException;
import io.crate.types.ArrayType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class ConcatFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testTooFewArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: concat(string)");
        assertNormalize("concat('foo')", null);
    }

    @Test
    public void testArgumentThatHasNoStringRepr() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast [1] to type string");
        assertNormalize("concat('foo', [1])", null);
    }


    @Test
    public void testNormalizeWithNulls() throws Exception {
        assertNormalize("concat(null, null)", isLiteral(""));
        assertNormalize("concat(null, 'foo')", isLiteral("foo"));
        assertNormalize("concat('foo', null)", isLiteral("foo"));

        assertNormalize("concat(5, null)", isLiteral("5"));
    }

    @Test
    public void testTwoStrings() throws Exception {
        assertNormalize("concat('foo', 'bar')", isLiteral("foobar"));
    }

    @Test
    public void testManyStrings() throws Exception {
        assertNormalize("concat('foo', null, '_', null, 'testing', null, 'is_boring')",
            isLiteral("foo_testingis_boring"));
    }

    @Test
    public void testStringAndNumber() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type long");
        assertNormalize("concat('foo', 3)", isLiteral("foo3"));
    }

    @Test
    public void testTwoArrays() throws Exception {
        assertNormalize("concat([1, 2], [2, 3])", isLiteral(new Object[]{1L, 2L, 2L, 3L}));
    }

    @Test
    public void testArrayWithAUndefinedInnerType() throws Exception {
        assertNormalize("concat([], [1, 2])", isLiteral(new Object[]{1L, 2L}));
    }

    @Test
    public void testTwoArraysOfIncompatibleInnerTypes() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast [[1, 2]] to type long_array");
        assertNormalize("concat([1, 2], [[1, 2]])", null);
    }

    @Test
    public void testTwoArraysOfUndefinedTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("When concatenating arrays, one of the two arguments can be of undefined inner type, but not both");
        assertNormalize("concat([], [])", null);
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("concat([1], [2::integer, 3::integer])", new Long[]{1L, 2L, 3L});
    }
}
