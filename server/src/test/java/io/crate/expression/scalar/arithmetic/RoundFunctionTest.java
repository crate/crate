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
    public void testInvalidType() throws Exception {
        assertThatThrownBy(() -> {
            assertEvaluateNull("round('foo')");

        })
                .isExactlyInstanceOf(ConversionException.class)
                .hasMessage("Cannot cast `'foo'` of type `text` to type `byte`");
    }
}
