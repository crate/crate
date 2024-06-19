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

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;


public class AbsFunctionTest extends ScalarTestCase {

    @Test
    public void testAbs() throws Exception {
        assertEvaluate("abs(-2)", 2);
        assertEvaluate("abs(-2.0)", 2.0);
        assertEvaluate("abs(cast(-2 as bigint))", 2L);
        assertEvaluate("abs(cast(-2.0 as float))", 2.0f);
        assertEvaluateNull("abs(null)");
    }

    @Test
    public void testWrongType() throws Exception {
        assertThatThrownBy(() -> {
            assertEvaluateNull("abs('foo')");

        })
                .isExactlyInstanceOf(ConversionException.class)
                .hasMessage("Cannot cast `'foo'` of type `text` to type `byte`");
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("abs(id)", isFunction("abs"));
    }

    @Test
    public void testNormalizeNull() throws Exception {
        assertNormalize("abs(null)", isLiteral(null));
    }
}
