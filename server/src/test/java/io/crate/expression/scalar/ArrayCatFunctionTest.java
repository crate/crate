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
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class ArrayCatFunctionTest extends ScalarTestCase {

    private static final ArrayType<Integer> INTEGER_ARRAY_TYPE = new ArrayType<>(DataTypes.INTEGER);

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        assertNormalize("array_cat([10, 20], [10, 30])", isLiteral(Arrays.asList(10, 20, 10, 30)));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        assertNormalize("array_cat(long_array, [10, 30])", isFunction(ArrayCatFunction.NAME));
    }

    @Test
    public void testNullArguments() throws Exception {
        assertNormalize("array_cat([1, 2, 3], null)", isLiteral(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void testZeroArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_cat()." +
                                        " Possible candidates: array_cat(array(E), array(E)):array(E)");
        assertEvaluate("array_cat()", null);
    }

    @Test
    public void testOneArgument() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_cat(_array(1))," +
                                        " no overload found for matching argument types: (integer_array).");
        assertEvaluate("array_cat([1])", null);
    }

    @Test
    public void testThreeArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: array_cat(_array(1), _array(2), _array(3))," +
                                        " no overload found for matching argument types: (integer_array, integer_array, integer_array).");
        assertEvaluate("array_cat([1], [2], [3])", null);
    }

    @Test
    public void testDifferentConvertableInnerTypes() throws Exception {
        assertEvaluate("array_cat([1::integer], [1::long])", Arrays.asList(1L, 1L));
    }

    @Test
    public void testNullElements() throws Exception {
        assertEvaluate("array_cat(int_array, int_array)",
            Arrays.asList(1, null, 3, null, 2, 3),
            Literal.of(Arrays.asList(1, null, 3), INTEGER_ARRAY_TYPE),
            Literal.of(Arrays.asList(null, 2, 3), INTEGER_ARRAY_TYPE));
    }

    @Test
    public void testNullValue() throws Exception {
        assertEvaluate("array_cat(int_array, int_array)",
            Arrays.asList(1, 2),
            Literal.of(INTEGER_ARRAY_TYPE, null),
            Literal.of(Arrays.asList(1, 2), INTEGER_ARRAY_TYPE));
    }

    @Test
    public void testTwoIntegerArguments() throws Exception {
        assertEvaluate("array_cat(int_array, int_array)",
            Arrays.asList(1, 2, 2, 3),
            Literal.of(Arrays.asList(1, 2), INTEGER_ARRAY_TYPE),
            Literal.of(Arrays.asList(2, 3), INTEGER_ARRAY_TYPE));
    }

    @Test
    public void testEmptyArrayAndIntegerArray() throws Exception {
        assertEvaluate("array_cat([], int_array)",
            Arrays.asList(1, 2),
            Literal.of(Arrays.asList(1, 2), INTEGER_ARRAY_TYPE));
    }

    @Test
    public void testEmptyArrays() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("One of the arguments of the `array_cat` function can be of undefined inner type, but not both");
        assertNormalize("array_cat([], [])", null);
    }
}
