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

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class ArrayFunctionTest extends ScalarTestCase {

    @Test
    public void testTypeValidation() {
        assertThatThrownBy(() -> assertEvaluateNull("ARRAY[1, 'foo']"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'foo'` of type `text` to type `integer`");
    }

    @Test
    public void testEvaluateArrayWithExpr() {
        assertEvaluate("ARRAY[1 + 2]", List.of(3));
        assertEvaluate("[1 + 1]", List.of(2));
    }

    @Test
    public void testEvaluateNestedArrays() {
        assertEvaluate("ARRAY[[]]", List.of(List.of()));
        assertEvaluate("[ARRAY[]]", List.of(List.of()));
        assertEvaluate("[[1 + 1], ARRAY[1 + 2]]", List.of(List.of(2), List.of(3)));
    }

    public void testEvaluateArrayOnColumnIdents() {
        assertEvaluate(
            "ARRAY[ARRAY[age], [age]]",
            List.of(List.of(2), List.of(1)),
            Literal.of(DataTypes.INTEGER, 2),
            Literal.of(DataTypes.INTEGER, 1));
    }

    @Test
    public void testNormalizeArrays() {
        assertNormalize("ARRAY[1 + 2]", isLiteral(List.of(3)));
        assertNormalize("[ARRAY[1 + 2]]", isLiteral(List.of(List.of(3))));
        assertNormalize("[[null is null], ARRAY[4 is not null], [false]]",
            isLiteral(List.of(List.of(true), List.of(true), List.of(false))));
    }
}
