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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.crate.testing.TestingHelpers.isLiteral;

public class PercentileFunctionTest extends AbstractScalarFunctionsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final DataType DOUBLE_ARRAY = new ArrayType(DataTypes.DOUBLE);

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("percentile_cont(0.5, [null, null, 1, null, 2, 3, 4])", isLiteral(2.5));
        assertNormalize("percentile_cont(0.5, [1.0, 2.2, 3.12, 4.3, 5.0, 6.9, 7.0, 8.2, 9.1, 10.5])", isLiteral(5.95));
        assertNormalize("percentile_cont(0.25, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])", isLiteral(2.75));
        assertNormalize("percentile_cont([0.25, 0.5], [1.2, 2.6, 3.6, 5.4, 6.1])", isLiteral(new Double[]{1.9, 3.6}));
        assertNormalize("percentile_cont([0.25, 0.95], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])", isLiteral(new Double[]{2.75, 10.0}));
    }

    @Test
    public void testNormalizeWithEmptyInputArray() throws Exception {
        assertNormalize("percentile_cont([0.25, 0.95], [])", isLiteral(null));
    }

    @Test
    public void testEvaluate() {
        assertEvaluate("percentile_cont(double_type, double_array)", 1.9,
            Literal.newLiteral(0.25),
            Literal.newLiteral(new Double[]{1.2, 2.6, 3.6, 5.4, 6.1}, DOUBLE_ARRAY)
        );
        assertEvaluate("percentile_cont(double_array, double_array)", new Double[]{1.915, 3.65},
            Literal.newLiteral(new Double[]{0.25, 0.5}, DOUBLE_ARRAY),
            Literal.newLiteral(new Double[]{1.21, 2.62, 3.65, 5.443, 6.15}, DOUBLE_ARRAY)
        );
        assertEvaluate("percentile_cont(double_array, double_array)", new Double[]{2.75, 5.5},
            Literal.newLiteral(new Double[]{0.25, 0.5}, DOUBLE_ARRAY),
            Literal.newLiteral(new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L}, new ArrayType(DataTypes.LONG))
        );
    }
    
    @Test
    public void testEvaluateWithNullValuesInInputArray() {
        assertEvaluate("percentile_cont(double_array, double_array)", new Object[]{2.4, 8.1},
            Literal.newLiteral(new Double[]{0.25, 0.75}, DOUBLE_ARRAY),
            Literal.newLiteral(new Double[]{1.9, 2.4, null, 4.5, 5.7, 6.8, null, 8.1, 9.2, null}, DOUBLE_ARRAY)
        );
    }

    @Test
    public void testEvaluateWithNullArguments() {
        assertEvaluate("percentile_cont(double_array, double_array)", null,
            Literal.NULL,
            Literal.newLiteral(new Double[]{2.0}, DOUBLE_ARRAY)
        );
        assertEvaluate("percentile_cont(double_array, double_array)", null,
            Literal.newLiteral(new Double[]{2.0}, DOUBLE_ARRAY),
            Literal.NULL
        );
    }

    @Test
    public void testEvaluateWithEmptyInputArray() {
        assertEvaluate("percentile_cont(0.25, double_array)", null, Literal.newLiteral(new Double[]{}, DOUBLE_ARRAY));
    }

    @Test
    public void testEvaluateWithNotAllowedTypeOfInputArray() {
        thrown.expect(UnsupportedOperationException.class);
        assertEvaluate("percentile_cont(0.25, tags)", null,
            Literal.newLiteral(new String[]{"a", "b", "c"}, new ArrayType(DataTypes.STRING)));
    }
}
