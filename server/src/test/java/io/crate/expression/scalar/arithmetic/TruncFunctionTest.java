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

package io.crate.expression.scalar.arithmetic;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;

import java.util.List;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class TruncFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeOnDouble() throws Exception {
        assertNormalize("trunc(29.1947)", isLiteral(29L));
        assertNormalize("trunc(-29.1947)", isLiteral(-29L));
        assertNormalize("trunc(-29.1947, 0)", isLiteral(-29.0));
        assertNormalize("trunc(29.1947, 0)", isLiteral(29.0));
        assertNormalize("trunc(29.1947, 1)", isLiteral(29.1d));
        assertNormalize("trunc(cast(null as double))", isLiteral(null, DataTypes.LONG));
    }

    @Test
    public void testNormalizeOnFloat() throws Exception {
        assertNormalize("trunc(cast(29.1947 as float))", isLiteral(29));
        assertNormalize("trunc(cast(29.1947 as float), 0)", isLiteral(29.0d));
        assertNormalize("trunc(cast(29.1947 as float), 1)", isLiteral(29.1d));
        assertNormalize("trunc(cast(null as float))", isLiteral(null));
    }

    @Test
    public void testNormalizeOnIntAndLong() throws Exception {
        assertNormalize("trunc(cast(20 as integer))", isLiteral(20));
        assertNormalize("trunc(cast(20 as integer), 0)", isLiteral(20.0d));
        assertNormalize("trunc(cast(20 as integer), 1)", isLiteral(20.0d));
        assertNormalize("trunc(cast(null as integer))", isLiteral(null));

        assertNormalize("trunc(cast(20 as bigint))", isLiteral(20L));
        assertNormalize("trunc(cast(20 as bigint), 0)", isLiteral(20.0d));
        assertNormalize("trunc(cast(20 as bigint), 1)", isLiteral(20.0d));
        assertNormalize("trunc(cast(null as bigint))", isLiteral(null));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("trunc(age)", isFunction(TruncFunction.NAME, List.of(DataTypes.INTEGER)));
        assertNormalize("trunc(x)", isFunction(TruncFunction.NAME, List.of(DataTypes.LONG)));
        assertNormalize("trunc(short_val)", isFunction(TruncFunction.NAME, List.of(DataTypes.SHORT)));
        assertNormalize("trunc(float_val)", isFunction(TruncFunction.NAME, List.of(DataTypes.FLOAT)));
        assertNormalize("trunc(double_val)", isFunction(TruncFunction.NAME, List.of(DataTypes.DOUBLE)));
    }

    @Test
    public void testEvaluateDouble() throws Exception {
        Literal<Double> value = Literal.of(DataTypes.DOUBLE, 29.1947d);
        assertEvaluate("trunc(double_val)", 29L, value);
        assertEvaluate("trunc(double_val, -1)", 20.0, value);
        assertEvaluate("trunc(double_val, 0)", 29.0, value);
        assertEvaluate("trunc(double_val, 1)", 29.1d, value);
        assertEvaluate("trunc(double_val, 2)", 29.19d, value);
        assertEvaluate("trunc(double_val, 3)", 29.194d, value);
        assertEvaluate("trunc(double_val, 4)", 29.1947d, value);
        assertEvaluate("trunc(double_val, 5)", 29.19470d, value);
    }

    @Test
    public void testEvaluateLong() throws Exception {
        Literal<Long> value = Literal.of(DataTypes.LONG, 246L);
        assertEvaluate("trunc(x)", 246L, value);
        assertEvaluate("trunc(x, -2)", 200.0, value);
        assertEvaluate("trunc(x, -1)", 240.0, value);
        assertEvaluate("trunc(x, 0)", 246.0, value);
        assertEvaluate("trunc(x, 1)", 246.0, value);
    }
}
