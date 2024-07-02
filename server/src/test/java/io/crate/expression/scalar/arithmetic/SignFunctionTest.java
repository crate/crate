/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar.arithmetic;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;

public class SignFunctionTest extends ScalarTestCase {

    @Test
    public void testSign() throws Exception {
        assertEvaluate("sign(-2)", -1.0);
        assertEvaluate("sign(-2.0)", -1.0);
        assertEvaluate("sign(-2::bigint)", -1.0);
        assertEvaluate("sign(-2.0::float)", -1.0);
        assertEvaluate("sign(-2.0::numeric)", BigDecimal.valueOf(-1.0));

        assertEvaluate("sign(11)", 1.0);
        assertEvaluate("sign(11.0)", 1.0);
        assertEvaluate("sign(11::bigint)", 1.0);
        assertEvaluate("sign(11.0::float)", 1.0);
        assertEvaluate("sign(11.0::numeric)", BigDecimal.valueOf(1.0));

        assertEvaluate("sign(0)", 0.0);
        assertEvaluateNull("sign(null)");
    }

    @Test
    public void testWrongType() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("sign('foo')", null))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `double precision`");
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("sign(id)", isFunction("sign"));
    }

    @Test
    public void testNormalizeNull() throws Exception {
        assertNormalize("sign(null)", isLiteral(null));
    }
}
