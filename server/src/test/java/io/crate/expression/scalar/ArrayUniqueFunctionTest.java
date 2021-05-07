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

package io.crate.expression.scalar;

import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class ArrayUniqueFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        assertNormalize("array_unique([10, 20], [10, 30])", isLiteral(Arrays.asList(10, 20, 30)));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        assertNormalize("array_unique(long_array, [10, 30])", isFunction("array_unique"));
    }

    @Test
    public void testNullArguments() throws Exception {
        assertNormalize("array_unique([1], null)", isLiteral(List.of(1)));
    }

    @Test
    public void testRefWithNullValue() throws Exception {
        assertEvaluate(
            "array_unique([1], long_array)",
            Collections.singletonList(1L),
            Literal.of(new ArrayType<>(DataTypes.LONG), null));
    }

    @Test
    public void testArrayUniqueOnNestedArrayReturnsUniqueInnerArrays() {
        assertEvaluate("array_unique([[0, 0], [1, 1]], [[0, 0], [1, 1]])",
            List.of(List.of(0, 0), List.of(1, 1)));
    }

    @Test
    public void testArrayUniqueWithObjectArraysThatContainArraysReturnsUniqueArrays() {
        assertEvaluate(
            "array_unique([{x=[1, 1]}], [{x=[1, 1]}])",
             Matchers.contains(
               Matchers.hasEntry(is("x"), Matchers.contains(is(1), is(1)))
           )
        );
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_unique().");
        assertEvaluate("array_unique()", null);
    }

    @Test
    public void testOneArgument() throws Exception {
        assertEvaluate(
            "array_unique(['foo', 'bar', 'baz', 'baz'])",
            Arrays.asList(
                "foo",
                "bar",
                "baz"
            )
        );
    }

    @Test
    public void testDifferentButConvertableInnerTypes() throws Exception {
        assertEvaluate("array_unique([10, 20], [10.1, 20.0])", Arrays.asList(10.0, 20.0, 10.1));
    }

    @Test
    public void testConvertNonNumericStringToNumber() {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'foo'` of type `text` to type `integer`");
        assertEvaluate("array_unique([10, 20], ['foo', 'bar'])", null);
    }

    @Test
    public void testDifferentUnconvertableInnerTypes() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_unique(_array(doc.users.geopoint), _array(true))," +
                                        " no overload found for matching argument types: (geo_point_array, boolean_array).");
        assertEvaluate("array_unique([geopoint], [true])", null);
    }

    @Test
    public void testNullElements() throws Exception {
        assertEvaluate("array_unique([1, null, 3], [null, 2, 3])", Arrays.asList(1, null, 3, 2));
    }

    @Test
    public void testEmptyArrayAndInterArray() throws Exception {
        assertEvaluate("array_unique([], [111, 222, 333])", Arrays.asList(111, 222, 333));
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the `array_unique` function can " +
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
