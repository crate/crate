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

package io.crate.execution.expression.scalar;

import io.crate.analyze.symbol.Literal;
import io.crate.exceptions.ConversionException;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class ArrayUniqueFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        assertNormalize("array_unique([10, 20], [10, 30])", isLiteral(new Long[] {10L, 20L, 30L}));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        assertNormalize("array_unique(long_array, [10, 30])", isFunction("array_unique"));
    }

    @Test
    public void testNullArguments() throws Exception {
        assertNormalize("array_unique([1], null)", isLiteral(new Long[] {1L}));
    }

    @Test
    public void testRefWithNullValue() throws Exception {
        assertEvaluate("array_unique([1], long_array)",
            new Long[] {1L},
            Literal.of(new ArrayType(DataTypes.LONG), null));
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: array_unique()");
        assertEvaluate("array_unique()", null);
    }

    @Test
    public void testOneArgument() throws Exception {
        assertEvaluate("array_unique(['foo', 'bar', 'baz', 'baz'])",
            new BytesRef[]{
                new BytesRef("foo"),
                new BytesRef("bar"),
                new BytesRef("baz")
            }
        );
    }

    @Test
    public void testDifferentButConvertableInnerTypes() throws Exception {
        assertEvaluate("array_unique([10, 20], [10.1, 20.0])", new Long[] {10L, 20L });
    }

    @Test
    public void testConvertNonNumericStringToNumber() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast ['foo', 'bar'] to type long_array");
        assertEvaluate("array_unique([10, 20], ['foo', 'bar'])", null);
    }

    @Test
    public void testDifferentUnconvertableInnerTypes() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast [true] to type geo_point_array");
        assertEvaluate("array_unique([geopoint], [true])", null);
    }

    @Test
    public void testNullElements() throws Exception {
        assertEvaluate("array_unique([1, null, 3], [null, 2, 3])", new Long[] { 1L, null, 3L, 2L });
    }

    @Test
    public void testEmptyArrayAndLongArray() throws Exception {
        assertEvaluate("array_unique([], [111, 222, 333])", new Long[] {111L, 222L, 333L});
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the array_unique function can " +
                                        "be of undefined inner type, but not both");
        assertEvaluate("array_unique([], [])", null);
    }

    @Test
    public void testEmptyArray() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "When used with only one argument, the inner type of the array argument cannot be undefined");
        assertEvaluate("array_unique([])", null);
    }
}
