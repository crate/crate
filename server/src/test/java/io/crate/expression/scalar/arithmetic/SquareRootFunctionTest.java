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


public class SquareRootFunctionTest extends ScalarTestCase {

    @Test
    public void testSqrt() {
        assertEvaluate("sqrt(25)", 5.0d);
        assertEvaluate("sqrt(25.0)", 5.0d);
        assertEvaluateNull("sqrt(null)");
        assertEvaluate("sqrt(cast(25 as integer))", 5.0);
        assertEvaluate("sqrt(cast(25.0 as float))", 5.0);
    }

    @Test
    public void testSmallerThanZero() {
        assertThatThrownBy(() -> assertEvaluateNull("sqrt(-25.0)"))
            .hasMessage("cannot take square root of a negative number");
    }

    @Test
    public void testInvalidType() {
        assertThatThrownBy(() -> assertEvaluateNull("sqrt('foo')"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `byte`");
    }

    @Test
    public void testNormalizeWithRef() {
        assertNormalize("sqrt(id)", isFunction("sqrt"));
    }
}
