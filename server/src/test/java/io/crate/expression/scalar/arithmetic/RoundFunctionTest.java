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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;


public class RoundFunctionTest extends ScalarTestCase {

    @Test
    public void testRound() throws Exception {
        assertEvaluate("round(42.2)", 42L);
        assertEvaluate("round(42)", 42);
        assertEvaluate("round(42::bigint)", 42L);
        assertEvaluate("round(cast(42.2 as float))", 42);
        assertEvaluateNull("round(null)");

        assertNormalize("round(id)", isFunction("round"));
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

        assertEvaluateNull("round(1,null)");
        assertEvaluateNull("round(null,null)");
        assertEvaluateNull("round(null,1)");
    }

    @Test
    public void testInvalidType() throws Exception {
        assertThatThrownBy(() -> assertEvaluateNull("round('foo')"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `byte`");
    }
}
