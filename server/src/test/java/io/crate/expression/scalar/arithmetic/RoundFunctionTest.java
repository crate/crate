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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;


public class RoundFunctionTest extends ScalarTestCase {

    @Test
    public void testRound() throws Exception {
        assertEvaluate("round(42.2)", 42L);
        assertEvaluate("round(42)", 42);
        assertEvaluate("round(42::bigint)", 42L);
        assertEvaluate("round(cast(42.5 as float))", 43);
        assertEvaluate("round(cast(-42.5 as float))", -42);
        assertEvaluate("round(cast(12.545 as numeric(5, 2)))", new BigDecimal(13));
        assertEvaluate("round(cast(-12.545 as numeric(5, 2)))", new BigDecimal(-13));
        assertEvaluateNull("round(null)");

        assertNormalize("round(id)", isFunction("round"));
    }

    @Test
    public void test_numeric_return_type() {
        assertNormalize("round(cast(null as numeric(10, 5)))", isLiteral(null, NumericType.of(List.of(10, 5))));
    }

    @Test
    public void test_round_with_precision() throws Exception {
        assertEvaluate("round(123.123,0)", BigDecimal.valueOf(123));
        assertEvaluate("round(123.123,1)", BigDecimal.valueOf(1231,1));
        assertEvaluate("round(123.123,4)", BigDecimal.valueOf(1231230,4));
        assertEvaluate("round(123.123,-1)", BigDecimal.valueOf(120));
        assertEvaluate("round(123.123,-4)", BigDecimal.valueOf(0));

        assertEvaluate("round(987.987,0)", BigDecimal.valueOf(988));
        assertEvaluate("round(987.987,1)", BigDecimal.valueOf(9880,1));
        assertEvaluate("round(987.987,-1)", BigDecimal.valueOf(990));

        assertEvaluate("round(1000.0, 17)", new BigDecimal("1000.00000000000000000"));
        assertEvaluate("round(260.775, 2)", BigDecimal.valueOf(26078,2));

        assertEvaluate("round(-123.123,0)", BigDecimal.valueOf(-123));
        assertEvaluate("round(-123.123,1)", BigDecimal.valueOf(-1231,1));
        assertEvaluate("round(-123.123,4)", BigDecimal.valueOf(-1231230,4));
        assertEvaluate("round(-123.123,-1)", BigDecimal.valueOf(-120));
        assertEvaluate("round(-123.123,-4)", BigDecimal.valueOf(0));

        assertEvaluate("round(2147483647, -1)", new BigDecimal("2147483650"));
        assertEvaluate("round(9223372036854775807, -1)", new BigDecimal("9223372036854775810"));
        assertEvaluate("round('92233720368547758070.123'::NUMERIC, 1)", new BigDecimal("92233720368547758070.1"));
        assertEvaluate("round('12.345'::NUMERIC, 2)", new BigDecimal("12.35"));
        assertEvaluate("round('-12.345'::NUMERIC, 2)", new BigDecimal("-12.35"));

        assertEvaluateNull("round(1,null)");
        assertEvaluateNull("round(null,null)");
        assertEvaluateNull("round(null,1)");
    }

    @Test
    public void test_round_with_negative_precision_at_the_magnitude_of_the_value() {
        // The value is rounded up to the next power of ten, it does not become 0
        assertEvaluate("round(999.9, -3)", new BigDecimal("1000"));
        assertEvaluate("round(999.9, -4)", BigDecimal.ZERO);
        assertEvaluate("round(5000, -4)", new BigDecimal("10000"));
        assertEvaluate("round(500, -4)", BigDecimal.ZERO);
        assertEvaluate("round(0.5, -1)", BigDecimal.ZERO);
    }

    // https://github.com/crate/crate/issues/19918
    @Test
    public void test_round_with_large_negative_precision_returns_zero() {
        assertEvaluate("round(-1479165877, -1000000)", BigDecimal.ZERO);
        assertEvaluate("round('-1479165877'::NUMERIC, -556375977)", BigDecimal.ZERO);
        assertEvaluate("round('92233720368547758070.123'::NUMERIC, -2000000000)", BigDecimal.ZERO);
        assertEvaluate("round(0.0, -2000000000)", BigDecimal.ZERO);
    }

    // Similar to: https://github.com/crate/crate/issues/19918
    @Test
    public void test_round_with_too_large_precision_raises_an_error() {
        assertEvaluate("round(1.5, " + RoundFunction.MAX_PRECISION + ")",
            BigDecimal.valueOf(15, 1).setScale(RoundFunction.MAX_PRECISION, RoundingMode.HALF_UP));
        assertEvaluate("round(1.5, " + (RoundFunction.MAX_PRECISION + 100) + ")",
            BigDecimal.valueOf(15, 1).setScale(RoundFunction.MAX_PRECISION, RoundingMode.HALF_UP));
    }

    @Test
    public void test_numeric_return_type_with_precision_param() {
        assertNormalize("round(cast(null as numeric(10, 5)), 1)", isLiteral(null, NumericType.of(List.of(10, 5))));
        assertNormalize("round(cast(null as double), 1)", isLiteral(null, DataTypes.NUMERIC));
    }

    @Test
    public void testInvalidType() throws Exception {
        assertThatThrownBy(() -> assertEvaluateNull("round('foo')"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `byte`");
    }
}
