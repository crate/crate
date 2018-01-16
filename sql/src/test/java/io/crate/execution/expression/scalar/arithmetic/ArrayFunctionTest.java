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

package io.crate.execution.expression.scalar.arithmetic;

import io.crate.analyze.symbol.Literal;
import io.crate.exceptions.ConversionException;
import io.crate.execution.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class ArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testTypeValidation() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast 'foo' to type long");
        assertEvaluate("ARRAY[1, 'foo']", null);
    }

    @Test
    public void testEvaluateArrayWithExpr() {
        assertEvaluate("ARRAY[1 + 2]", new Long[]{3L});
        assertEvaluate("[1 + 1]", new Long[]{2L});
    }

    @Test
    public void testEvaluateNestedArrays() {
        assertEvaluate("ARRAY[[]]", new Object[]{new Object[]{}});
        assertEvaluate("[ARRAY[]]", new Object[]{new Object[]{}});
        assertEvaluate("[[1 + 1], ARRAY[1 + 2]]", new Object[]{new Long[]{2L}, new Long[]{3L}});
    }

    public void testEvaluateArrayOnColumnIdents() {
        assertEvaluate("ARRAY[ARRAY[age], [age]]", new Integer[][]{new Integer[]{2},new Integer[]{1}},
            Literal.of(DataTypes.INTEGER, 2),
            Literal.of(DataTypes.INTEGER, 1));
    }

    @Test
    public void testNormalizeArrays() {
        assertNormalize("ARRAY[1 + 2]", isLiteral(new Long[]{3L}));
        assertNormalize("[ARRAY[1 + 2]]", isLiteral(new Object[]{new Long[]{3L}}));
        assertNormalize("[[null is null], ARRAY[4 is not null], [false]]",
            isLiteral(new Object[]{new Boolean[]{true}, new Boolean[]{true}, new Boolean[]{false}}));
    }
}
