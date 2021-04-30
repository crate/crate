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

import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ArrayDifferenceFunctionTest extends ScalarTestCase {

    private static final ArrayType<Integer> INTEGER_ARRAY_TYPE = new ArrayType<>(DataTypes.INTEGER);

    @Test
    public void testCompileWithValues() throws Exception {
        assertCompile("array_difference(int_array, [3, 4, 5])", (s) -> not(IsSame.sameInstance(s)));
    }

    @Test
    public void testCompileWithRefs() throws Exception {
        assertCompile("array_difference(int_array, int_array)", IsSame::sameInstance);
    }

    @Test
    public void testArrayDifferenceRemovesTheNestedArraysInTheFirstArrayThatAreContainedWithinTheSecondArray() {
        assertEvaluate(
            "array_difference([[1, 2], [1, 3]], [[1, 2]])",
            Matchers.contains(Matchers.contains(1, 3)));
    }

    @Test
    public void testArrayDifferenceRemovesTheObjectsInTheFirstArrayThatAreContainedInTheSecond() {
        assertEvaluate(
            "array_difference([{x=[1, 2]}, {x=[1, 3]}], [{x=[1, 3]}])",
            Matchers.contains(Matchers.hasEntry(is("x"), Matchers.contains(1, 2))));
    }

    @Test
    public void testNormalize() throws Exception {
        assertNormalize("array_difference([10, 20], [10, 30])", isLiteral(List.of(20)));
        assertNormalize("array_difference([], [10, 30])", isLiteral(List.of()));
    }

    @Test
    public void testNormalizeNullArguments() throws Exception {
        assertNormalize("array_difference([1], null)", isLiteral(List.of(1)));
    }

    @Test
    public void testEvaluateNullArguments() throws Exception {
        assertEvaluate("array_difference([1], long_array)", List.of(1L), Literal.of(DataTypes.BIGINT_ARRAY, null));
        assertEvaluate("array_difference(long_array, [1])", null, Literal.of(DataTypes.BIGINT_ARRAY, null));
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_difference()." +
                                        " Possible candidates: array_difference(array(E), array(E)):array(E)");
        assertNormalize("array_difference()", null);
    }

    @Test
    public void testOneArgument() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_difference(_array(1))," +
                                        " no overload found for matching argument types: (integer_array).");
        assertNormalize("array_difference([1])", null);
    }

    @Test
    public void testDifferentButConvertableInnerTypes() throws Exception {
        assertEvaluate("array_difference([1::integer], [1::long])", List.of());
    }

    @Test
    public void testNullElements() throws Exception {
        assertEvaluate("array_difference(int_array, int_array)",
            List.of(1),
            Literal.of(Arrays.asList(1, null, 3), INTEGER_ARRAY_TYPE),
            Literal.of(Arrays.asList(null, 2, 3), INTEGER_ARRAY_TYPE)
        );
        assertEvaluate("array_difference(int_array, int_array)",
            Arrays.asList(1, null, 2, null),
            Literal.of(Arrays.asList(1, null, 3, 2, null), INTEGER_ARRAY_TYPE),
            Literal.of(Collections.singletonList(3), INTEGER_ARRAY_TYPE));
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the `array_difference` function can be of undefined inner type, but not both");
        assertNormalize("array_difference([], [])", null);
    }
}
