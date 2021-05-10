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
package io.crate.expression.operator;

import io.crate.expression.symbol.Literal;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class InOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbolSetLiteralIntegerIncluded() {
        assertNormalize("1 in (1, 2, 4, 8)", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralIntegerNotIncluded() {
        assertNormalize("128 in (1, 2, 4, 8)", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralDifferentDataTypeValue() {
        assertNormalize("2.3 in (1, 2, 4, 8)", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralReference() {
        assertNormalize("age in (1, 2)", isFunction(AnyOperator.OPERATOR_PREFIX + "="));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringIncluded() {
        assertNormalize("'charlie' in ('alpha', 'bravo', 'charlie', 'delta')", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringNotIncluded() {
        assertNormalize("'not included' in ('alpha', 'bravo', 'charlie', 'delta')", isLiteral(false));
    }

    @Test
    public void testEvaluateInOperator() {
        assertEvaluate("null in ('alpha', 'bravo')", null);
        assertEvaluate("name in ('alpha', 'bravo')", null, Literal.of(DataTypes.STRING, null));
        assertEvaluate("null in (name)", null, Literal.of(DataTypes.STRING, null));
        assertEvaluate("'alpha' in ('alpha', null)", true);
        assertEvaluate("'alpha' in (null, 'alpha')", true);
        assertEvaluate("'alpha' in ('beta', null)", null);
        assertEvaluate("'alpha' in (null)", null);
        assertEvaluate("null in (null)", null);
    }
}
