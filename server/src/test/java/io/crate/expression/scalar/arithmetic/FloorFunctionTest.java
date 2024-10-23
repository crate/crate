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

import java.math.BigDecimal;
import java.util.List;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class FloorFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateOnNumeric() {
        assertNormalize("floor(cast(123.456789 as numeric(9, 6)))", isLiteral(new BigDecimal(123)));
        assertNormalize("floor(cast(-123.456789 as numeric(9, 6)))", isLiteral(new BigDecimal(-124)));
        assertNormalize("floor(cast(null as numeric))", isLiteral(null, DataTypes.NUMERIC));
    }

    @Test
    public void test_numeric_return_type() {
        assertNormalize("floor(cast(null as numeric(10, 5)))", isLiteral(null, NumericType.of(List.of(10, 5))));
    }

    @Test
    public void testEvaluateOnDouble() throws Exception {
        assertNormalize("floor(29.9)", isLiteral(29L));
        assertNormalize("floor(-29.9)", isLiteral(-30L));
        assertNormalize("floor(cast(null as double))", isLiteral(null, DataTypes.LONG));
    }

    @Test
    public void testEvaluateOnFloat() throws Exception {
        assertNormalize("floor(cast(29.9 as float))", isLiteral(29));
        assertNormalize("floor(cast(-29.9 as float))", isLiteral(-30));
        assertNormalize("floor(cast(null as float))", isLiteral(null, DataTypes.INTEGER));
    }

    @Test
    public void testEvaluateOnIntAndLong() throws Exception {
        assertNormalize("floor(20)", isLiteral(20));
        assertNormalize("floor(cast(null as integer))", isLiteral(null, DataTypes.INTEGER));
        assertNormalize("floor(42.9)", isLiteral(42L));

        assertNormalize("floor(20::bigint)", isLiteral(20L));
        assertNormalize("floor(cast(null as long))", isLiteral(null, DataTypes.LONG));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("floor(age)", isFunction(FloorFunction.NAME));
    }
}
