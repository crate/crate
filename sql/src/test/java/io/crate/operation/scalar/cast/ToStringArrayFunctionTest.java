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

package io.crate.operation.scalar.cast;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isLiteral;

public class ToStringArrayFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalize() throws Exception {
        assertNormalize("to_string_array([1, 2, 3])", isLiteral(new ArrayType(DataTypes.STRING).value(new Object[]{"1", "2", "3"})));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("to_string_array(long_array)", new ArrayType(DataTypes.STRING).value(new Object[]{"1", "2", "3"}),
            Literal.of(new Long[]{1L, 2L, 3L}, new ArrayType(DataTypes.LONG)));
    }

    @Test
    public void testInvalidArgumentType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("type 'string' not supported for conversion to 'string_array'");
        sqlExpressions.normalize(sqlExpressions.asSymbol("to_string_array('bla')"));
    }

    @Test
    public void testInvalidArgumentInnerType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("type 'object_array' not supported for conversion to 'string_array'");
        sqlExpressions.normalize(sqlExpressions.asSymbol("to_string_array([{a = 1}])"));
    }

}
