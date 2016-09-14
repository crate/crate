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

package io.crate.operation.scalar;


import io.crate.analyze.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.crate.testing.TestingHelpers.isLiteral;



public class ArraySortFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("array_sort([1, 2, 5, 4, 6], 'asc')", isLiteral(new Long[]{1L, 2L, 4L, 5L, 6L}));
        assertNormalize("array_sort([1, 2, 5, 4, 6])", isLiteral(new Long[]{1L, 2L, 4L, 5L, 6L}));
        assertNormalize("array_sort([])", isLiteral(new Object[]{}));
    }

    @Test
    public void testEvaluate() {
        assertEvaluate("array_sort(long_array)", new Object[]{1L, 10L, 20L, 30L},
            Literal.newLiteral(new Object[]{10L, 1L, 20L, 30L}, new ArrayType(DataTypes.LONG))
        );
        assertEvaluate("array_sort(tags, 'desc')", new String[]{"a", "b"},
            Literal.newLiteral(new String[]{"b", "a"}, new ArrayType(DataTypes.STRING))
        );

        assertEvaluate("array_sort(int_array, 'foo')", new Integer[]{1, 90},
            Literal.newLiteral(new Integer[]{90, 1}, new ArrayType(DataTypes.INTEGER))
        );

    }



    @Test
    public void testEvaluateEmptyArray() throws Exception {
        assertEvaluate("array_sort(long_array, 'desc')", new Long[]{},
            Literal.newLiteral(new Long[]{}, new ArrayType(DataTypes.LONG))
        );
    }
}
