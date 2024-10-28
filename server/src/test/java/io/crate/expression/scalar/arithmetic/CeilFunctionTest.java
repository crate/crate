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

public class CeilFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateOnNumeric() {
        assertNormalize("ceil(cast(123.456789 as numeric(9, 6)))", isLiteral(new BigDecimal(124)));
        assertNormalize("ceil(cast(-123.456789 as numeric(9, 6)))", isLiteral(new BigDecimal(-123)));
        assertNormalize("ceil(cast(null as numeric))", isLiteral(null, DataTypes.NUMERIC));
    }

    @Test
    public void test_numeric_return_type() {
        assertNormalize("ceil(cast(null as numeric(10, 5)))", isLiteral(null, NumericType.of(List.of(10, 5))));
    }

    @Test
    public void testEvaluateOnDouble() throws Exception {
        assertNormalize("ceil(29.9)", isLiteral(30L));
        assertNormalize("ceil(-29.9)", isLiteral(-29L));
        assertNormalize("ceil(null)", isLiteral(null));
    }

    @Test
    public void testEvaluateOnFloat() throws Exception {
        assertNormalize("ceil(cast(29.9 as float))", isLiteral(30));
        assertNormalize("ceil(cast(-29.9 as float))", isLiteral(-29));
        assertNormalize("ceil(cast(null as float))", isLiteral(null, DataTypes.INTEGER));
    }

    @Test
    public void test_ceiling_works_as_alias_for_ceil() {
        assertEvaluate("ceiling(-95.3)", -95L);
    }

    @Test
    public void testEvaluateOnIntAndLong() throws Exception {
        assertNormalize("ceil(20)", isLiteral(20));
        assertNormalize("ceil(cast(null as integer))", isLiteral(null, DataTypes.INTEGER));

        assertNormalize("ceil(20::bigint)", isLiteral(20L));
        assertNormalize("ceil(cast(null as long))", isLiteral(null, DataTypes.LONG));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("ceil(age)", isFunction(CeilFunction.CEIL));
    }
}
